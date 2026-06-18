require('dotenv').config();
const express = require('express');
const cors = require('cors');
const puppeteer = require('puppeteer');
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand, PutCommand } = require("@aws-sdk/lib-dynamodb");

const app = express();
app.use(cors());
app.use(express.json());

// ============================================================================
// 1. DYNAMODB KONFIGURACIJA
// ============================================================================
const client = new DynamoDBClient({ region: "eu-central-1" });
const docClient = DynamoDBDocumentClient.from(client);
const TABLE_NAME = "Usput";

// ============================================================================
// 2. SUSTAV ZA RED ČEKANJA (QUEUE)
// ============================================================================
class TaskQueue {
    constructor(concurrency) {
        this.concurrency = concurrency;
        this.running = 0;
        this.queue = [];
    }
    add(task) {
        return new Promise((resolve, reject) => {
            this.queue.push(async () => {
                try { resolve(await task()); }
                catch (err) { reject(err); }
                finally { this.running--; this.next(); }
            });
            this.next();
        });
    }
    next() {
        if (this.running >= this.concurrency || this.queue.length === 0) return;
        this.running++;
        const task = this.queue.shift();
        if (task) task();
    }
}

// Ograničavamo na 2 taba istovremeno zbog 2GB RAM-a na t3.small
const browserQueue = new TaskQueue(2);

const PLANNER_URLS = {
    'PLATSA': 'https://www.ikea.com/addon-app/storageone/platsa/web/latest/hr/hr/#/planner?vpc=',
    'PAX': 'https://www.ikea.com/addon-app/storageone/pax/web/latest/hr/hr/#/planner?vpc=',
    'BESTA': 'https://www.ikea.com/addon-app/storageone/besta/web/latest/hr/hr/#/planner?vpc=',
    'BESTÅ': 'https://www.ikea.com/addon-app/storageone/besta/web/latest/hr/hr/#/planner?vpc=',
    'METOD': 'https://kitchen.planner.ikea.com/hr/hr/?project='
};

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', message: 'Usput Puppeteer API radi savršeno!' });
});

// ============================================================================
// 3. GLAVNA API FUNKCIJA
// ============================================================================
app.post('/api/list-volume', async (req, res) => {
    try {
        const listUrl = req.body.url;

        if (!listUrl || !listUrl.includes('ikea.com')) {
            return res.status(400).json({ success: false, error: 'Molimo unesite ispravnu IKEA poveznicu.' });
        }

        const itemsMap = new Map();
        let requiresVan = false;
        const foundBigItems = new Set();

        console.log('\n--- NOVI ZAHTJEV ZAPRIMLJEN ---', listUrl);
        const urlObj = new URL(listUrl);

        // 1. Izvlačenje osnovnih artikala s liste (iz URL path-a)
        if (listUrl.includes('/receive-share/')) {
            const pathSegments = urlObj.pathname.split('/');
            const shareIndex = pathSegments.indexOf('receive-share');
            if (shareIndex !== -1 && pathSegments.length > shareIndex + 1) {
                const itemsSegment = pathSegments[shareIndex + 1];
                if (itemsSegment && itemsSegment.includes(':')) {
                    itemsSegment.split(',').forEach((item) => {
                        const [code, qty] = item.split(':');
                        itemsMap.set(code, { code, name: `Učitavam...`, quantity: parseInt(qty, 10) || 1, dimensions: null, isDesignPart: false });
                    });
                }
            }
        }

        // 2. Globalno izvlačenje dizajnera iz parametara
        const generatedDesigns = [];
        const designsParam = urlObj.searchParams.get('designs');
        if (designsParam) {
            designsParam.split(',').forEach(designStr => {
                const parts = designStr.split(':');
                const designId = parts[0];
                const familyUpper = parts[1] ? parts[1].toUpperCase() : 'KOMBINACIJA';
                const currentDesignQty = parseInt(parts[2], 10) || 1;

                if (PLANNER_URLS[familyUpper]) {
                    generatedDesigns.push({
                        link: PLANNER_URLS[familyUpper] + designId,
                        qty: currentDesignQty
                    });
                }
            });
        }

        await browserQueue.add(async () => {
            let browser = await puppeteer.launch({
                headless: 'new',
                args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            });

            try {
                const page = await browser.newPage();
                await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');

                // A) SKENIRANJE GLAVNE STRANICE
                await page.goto(listUrl, { waitUntil: 'networkidle2', timeout: 30000 });
                const scrapedListItems = await page.evaluate(() => {
                    const results = [];
                    document.querySelectorAll('.list-ingka-share-summary__item, [data-testid="list-item"], li[class*="item"]').forEach(el => {
                        const text = el.textContent || '';
                        const codeMatch = text.match(/\b(\d{3}\.\d{3}\.\d{2})\b/);
                        if (codeMatch) {
                            const code = codeMatch[1].replace(/\./g, '');
                            let qty = 1;
                            const qtyInput = el.querySelector('input[type="number"]');
                            if (qtyInput && qtyInput.value) qty = parseInt(qtyInput.value, 10) || 1;
                            else {
                                const qtyMatch = text.match(/(\d+)\s*(kom|x)/i);
                                if (qtyMatch) qty = parseInt(qtyMatch[1], 10);
                            }
                            results.push({ code, quantity: qty });
                        }
                    });
                    return results;
                });

                for (const item of scrapedListItems) {
                    if (itemsMap.has(item.code)) {
                        itemsMap.get(item.code).quantity = Math.max(itemsMap.get(item.code).quantity, item.quantity);
                    } else {
                        itemsMap.set(item.code, { code: item.code, name: 'Učitavam...', quantity: item.quantity, dimensions: null, isDesignPart: false });
                    }
                }

                // B) SKENIRANJE DIZAJNERA (Sada ultra pouzdano)
                if (generatedDesigns.length > 0) {
                    for (const design of generatedDesigns) {
                        console.log(`[Dizajner] Učitavam dizajn: ${design.link}`);

                        page.removeAllListeners('response');
                        page.on('response', async res => {
                            const req = res.request();

                            if (req.resourceType() === 'fetch' || req.resourceType() === 'xhr') {
                                // Preskakanje preflight CORS zahtjeva
                                if (res.status() === 204 || res.status() === 202) return;

                                try {
                                    const json = await res.json();

                                    const extractCodes = (obj) => {
                                        if (!obj || typeof obj !== 'object') return;

                                        // POPRAVAK: Svi mogući IKEA ključevi za proizvode (uključujući articleCode)
                                        const code = obj.articleNumber || obj.itemNo || obj.articleNo || obj.itemNumber || obj.partNumber || obj.id || obj.articleCode || obj.itemCode || obj.productId;
                                        const qty = obj.quantity !== undefined ? obj.quantity : (obj.qty !== undefined ? obj.qty : obj.count);

                                        if (code !== undefined && qty !== undefined) {
                                            const cleanCode = String(code).replace(/\D/g, '');
                                            if (cleanCode.length >= 6 && cleanCode.length <= 10) {
                                                const finalQty = (parseInt(qty, 10) || 1) * design.qty;
                                                if (itemsMap.has(cleanCode)) {
                                                    itemsMap.get(cleanCode).quantity = Math.max(itemsMap.get(cleanCode).quantity, finalQty);
                                                } else {
                                                    itemsMap.set(cleanCode, { code: cleanCode, name: 'Učitavam...', quantity: finalQty, dimensions: null, isDesignPart: true });
                                                }
                                            }
                                        }

                                        for (const key in obj) {
                                            if (Object.prototype.hasOwnProperty.call(obj, key)) extractCodes(obj[key]);
                                        }
                                    };
                                    extractCodes(json);
                                } catch(e) {
                                    // Prazni ili ne-JSON odgovori se ignoriraju
                                }
                            }
                        });

                        // Cekamo samo da se učita kostur stranice (DOMContentLoaded)
                        await page.goto(design.link, { waitUntil: 'domcontentloaded', timeout: 15000 }).catch(e => console.log("Timeout planera, nastavljam..."));

                        // OBAVEZNO: Dajemo 4 sekunde vremena API-ju da pošalje i primi JSON u pozadini
                        console.log(`[Dizajner] Čekam 4 sekunde da se povuku svi proizvodi iz planera...`);
                        await new Promise(resolve => setTimeout(resolve, 4000));
                    }
                }

                // C) PROVJERA Baze Podataka (DynamoDB)
                const allCodes = Array.from(itemsMap.values()).map(i => i.code);
                const missingCodesToFetch = [];
                const fetchedDetails = {};

                for (const code of allCodes) {
                    try {
                        const dbResponse = await docClient.send(new GetCommand({
                            TableName: TABLE_NAME,
                            Key: { PK: `ARTICLE#${code}`, SK: 'INFO' }
                        }));

                        if (dbResponse.Item && dbResponse.Item.name) {
                            fetchedDetails[code] = dbResponse.Item;
                        } else {
                            missingCodesToFetch.push(code);
                        }
                    } catch (dbError) {
                        missingCodesToFetch.push(code);
                    }
                }

                // D) ČUPANJE DIMENZIJA ZA NOVE ARTIKLE PREKO NODE FETCH-a
                if (missingCodesToFetch.length > 0) {
                    console.log(`[Server] Skidam dimenzije za ${missingCodesToFetch.length} novih artikala...`);

                    for (const code of missingCodesToFetch) {
                        try {
                            const response = await fetch(`https://www.ikea.com/hr/hr/p/-${code}/`, {
                                headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
                            });

                            if (response.ok) {
                                const htmlText = await response.text();

                                const titleMatch = htmlText.match(/<title>([^<]+)<\/title>/i);
                                let pageTitle = titleMatch ? titleMatch[1] : '';
                                pageTitle = pageTitle.replace(/- IKEA/i, '').trim().split(',')[0].trim();

                                let width = 0, height = 0, length = 0, weight = 0;
                                const wMatch = htmlText.match(/Širina:\s*(\d+)\s*cm/i);
                                const hMatch = htmlText.match(/Visina:\s*(\d+)\s*cm/i);
                                const lMatch = htmlText.match(/Duljina:\s*(\d+)\s*cm/i);
                                const wtMatch = htmlText.match(/Težina:\s*([\d,.]+)\s*kg/i);

                                if (lMatch) length = parseInt(lMatch[1], 10);
                                if (wMatch) width = parseInt(wMatch[1], 10);
                                if (hMatch) height = parseInt(hMatch[1], 10);
                                if (wtMatch) weight = parseFloat(wtMatch[1].replace(',', '.'));

                                if (!length && width) length = width;

                                const dims = (width > 0 || length > 0 || weight > 0) ? { width, height, length, weight } : null;

                                if (pageTitle && pageTitle.toUpperCase() !== 'IKEA') {
                                    const details = { name: pageTitle, dimensions: dims };
                                    fetchedDetails[code] = details;

                                    await docClient.send(new PutCommand({
                                        TableName: TABLE_NAME,
                                        Item: {
                                            PK: `ARTICLE#${code}`, SK: 'INFO',
                                            code: code, name: pageTitle, dimensions: dims, updatedAt: new Date().toISOString()
                                        }
                                    })).catch(e => console.log("DB Put Error:", e.message));
                                    continue;
                                }
                            }
                        } catch (e) {}

                        fetchedDetails[code] = { name: `IKEA Artikl (${code})`, dimensions: null };
                    }
                }

                // Spajanje svih imena i dimenzija
                for (const [code, details] of Object.entries(fetchedDetails)) {
                    const item = itemsMap.get(code);
                    if (item) {
                        item.name = item.isDesignPart ? `↳ [Dio dizajna] ${details.name}` : details.name;
                        item.dimensions = details.dimensions;
                    }
                }

            } finally {
                if (browser) await browser.close();
            }
        });

        // 4. FINALNI IZRAČUN (Volumen i Kilaža)
        const finalParsedItems = Array.from(itemsMap.values());
        let totalItemsCount = 0;
        let totalVolume = 0;
        let totalWeight = 0;
        let hasMissingDimensions = false;

        for (const item of finalParsedItems) {
            totalItemsCount += item.quantity;

            if (item.dimensions) {
                const { length, width, height, weight } = item.dimensions;

                if (length > 170 || weight > 30) {
                    requiresVan = true;
                    const cleanName = item.name.replace('↳ [Dio dizajna] ', '');
                    foundBigItems.add(`${cleanName} (${length}cm / ${weight}kg)`);
                }

                if (length && width && height) {
                    totalVolume += ((length * width * height) / 1000000) * item.quantity;
                } else {
                    hasMissingDimensions = true;
                }

                if (weight) {
                    totalWeight += (weight * item.quantity);
                } else {
                    hasMissingDimensions = true;
                }
            } else {
                hasMissingDimensions = true;
            }
        }

        if (totalItemsCount > 15 || listUrl.toUpperCase().includes('PLATSA') || listUrl.toUpperCase().includes('PAX')) {
            requiresVan = true;
        }

        res.json({
            success: true,
            data: {
                articlesFound: totalItemsCount,
                parsedItems: finalParsedItems,
                foundBigItems: Array.from(foundBigItems),
                requiresVan: requiresVan,
                designLink: generatedDesigns.length > 0 ? generatedDesigns[0].link : null,
                totalVolume: Number(totalVolume.toFixed(3)),
                totalWeight: Number(totalWeight.toFixed(2)),
                hasMissingDimensions: hasMissingDimensions
            }
        });

    } catch (error) {
        console.error('Kritična greška na backendu:', error.message);
        res.status(500).json({ success: false, error: 'Greška prilikom obrade IKEA liste.' });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🚀 Usput Scraper API je pokrenut i sluša na portu ${PORT}`);
});