require('dotenv').config();
const express = require('express');
const cors = require('cors');
const puppeteer = require('puppeteer');
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand, PutCommand } = require("@aws-sdk/lib-dynamodb");

const app = express();
app.use(cors());
app.use(express.json());

const client = new DynamoDBClient({ region: "eu-central-1" });
const docClient = DynamoDBDocumentClient.from(client);
const TABLE_NAME = "Usput";

class TaskQueue {
    constructor(concurrency) {
        this.concurrency = concurrency;
        this.running = 0;
        this.queue = [];
    }
    add(task) {
        return new Promise((resolve, reject) => {
            this.queue.push(async () => {
                try { resolve(await task()); } catch (err) { reject(err); } finally { this.running--; this.next(); }
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

const browserQueue = new TaskQueue(2);

const PLANNER_URLS = {
    'PLATSA': 'https://www.ikea.com/addon-app/storageone/platsa/web/latest/hr/hr/#/planner?vpc=',
    'PAX': 'https://www.ikea.com/addon-app/storageone/pax/web/latest/hr/hr/#/planner?vpc=',
    'BESTA': 'https://www.ikea.com/addon-app/storageone/besta/web/latest/hr/hr/#/planner?vpc=',
    'BESTÅ': 'https://www.ikea.com/addon-app/storageone/besta/web/latest/hr/hr/#/planner?vpc=',
    'METOD': 'https://kitchen.planner.ikea.com/hr/hr/?project='
};

app.post('/api/list-volume', async (req, res) => {
    try {
        const listUrl = req.body.url;
        if (!listUrl || !listUrl.includes('ikea.com')) {
            return res.status(400).json({ success: false, error: 'Molimo unesite ispravnu IKEA poveznicu.' });
        }

        const itemsMap = new Map();
        let requiresVan = false;
        const foundBigItems = new Set();

        console.log('\n--- NOVI ZAHTJEV ---', listUrl);
        const urlObj = new URL(listUrl);

        // 1. Čupanje iz URL-a (receive-share)
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

        const generatedDesigns = [];
        const designsParam = urlObj.searchParams.get('designs');
        if (designsParam) {
            designsParam.split(',').forEach(designStr => {
                const parts = designStr.split(':');
                const designId = parts[0];
                const familyUpper = parts[1] ? parts[1].toUpperCase() : 'KOMBINACIJA';
                const currentDesignQty = parseInt(parts[2], 10) || 1;

                if (PLANNER_URLS[familyUpper]) {
                    generatedDesigns.push({ link: PLANNER_URLS[familyUpper] + designId, qty: currentDesignQty });
                }
            });
        }

        await browserQueue.add(async () => {
            let browser = await puppeteer.launch({
                headless: 'new',
                args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
            });

            try {
                const page = await browser.newPage();
                await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');

                // --- GLAVNA LISTA ---
                await page.goto(listUrl, { waitUntil: 'networkidle2', timeout: 30000 });
                await new Promise(r => setTimeout(r, 2000)); // Čekamo React da nacrta UI

                // Pokušaj kliknuti "Vidi detalje" ili slične gumbe na glavnoj stranici
                await page.evaluate(() => {
                    document.querySelectorAll('button, a').forEach(btn => {
                        const txt = (btn.innerText || '').toLowerCase();
                        if (txt.includes('vidi') || txt.includes('prikaži') || txt.includes('kombinacij')) {
                            try { btn.click(); } catch(e) {}
                        }
                    });
                });
                await new Promise(r => setTimeout(r, 1500));

                const scrapedListItems = await page.evaluate(() => {
                    const results = [];
                    // Klasični elementi
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

                    // AGRESIVNI FALLBACK: Pročešljaj apsolutno cijeli HTML na ekranu!
                    const allText = document.documentElement.innerHTML || '';
                    const matches = [...allText.matchAll(/\b(\d{3}\.\d{3}\.\d{2})\b/g)];
                    matches.forEach(m => {
                        const code = m[1].replace(/\./g, '');
                        // Ako šifra nije pronađena preko klasa, dodaj je nasilno s količinom 1
                        if (!results.some(r => r.code === code)) {
                            results.push({ code, quantity: 1, isDesignPart: true });
                        }
                    });

                    return results;
                });

                for (const item of scrapedListItems) {
                    if (itemsMap.has(item.code)) {
                        itemsMap.get(item.code).quantity = Math.max(itemsMap.get(item.code).quantity, item.quantity);
                    } else {
                        itemsMap.set(item.code, { code: item.code, name: 'Učitavam...', quantity: item.quantity, dimensions: null, isDesignPart: item.isDesignPart });
                    }
                }

                // --- POSJETA DIZAJNERU (PLATSA, PAX) ---
                if (generatedDesigns.length > 0) {
                    for (const design of generatedDesigns) {
                        console.log(`[Dizajner] Učitavam: ${design.link}`);

                        await page.goto(design.link, { waitUntil: 'networkidle2', timeout: 20000 }).catch(() => {});
                        await new Promise(r => setTimeout(r, 3000)); // Čekaj da se učita

                        // Ponovno agresivno čupanje s ekrana samog dizajnera!
                        const plannerCodes = await page.evaluate(() => {
                            const codes = [];
                            const text = document.documentElement.innerHTML || '';
                            const matches = [...text.matchAll(/\b(\d{3}\.\d{3}\.\d{2})\b/g)];
                            matches.forEach(m => codes.push(m[1].replace(/\./g, '')));
                            return [...new Set(codes)];
                        });

                        plannerCodes.forEach(code => {
                            if (!itemsMap.has(code)) {
                                itemsMap.set(code, { code: code, name: 'Dio dizajna', quantity: design.qty, dimensions: null, isDesignPart: true });
                            }
                        });
                    }
                }

                // --- BAZA PODATAKA (DynamoDB) ---
                const allCodes = Array.from(itemsMap.values()).map(i => i.code);
                const missingCodesToFetch = [];
                const fetchedDetails = {};

                for (const code of allCodes) {
                    try {
                        const dbResponse = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { PK: `ARTICLE#${code}`, SK: 'INFO' } }));
                        if (dbResponse.Item && dbResponse.Item.name) {
                            fetchedDetails[code] = dbResponse.Item;
                        } else {
                            missingCodesToFetch.push(code);
                        }
                    } catch (e) { missingCodesToFetch.push(code); }
                }

                // --- BRZO NODE FETCH-ANJE S WEBA ---
                if (missingCodesToFetch.length > 0) {
                    console.log(`[Server] Skidam dimenzije za ${missingCodesToFetch.length} novih artikala...`);
                    for (const code of missingCodesToFetch) {
                        try {
                            const response = await fetch(`https://www.ikea.com/hr/hr/p/-${code}/`, {
                                headers: { 'User-Agent': 'Mozilla/5.0' }
                            });

                            if (response.ok) {
                                const htmlText = await response.text();
                                const titleMatch = htmlText.match(/<title>([^<]+)<\/title>/i);
                                let pageTitle = titleMatch ? titleMatch[1].replace(/- IKEA/i, '').trim().split(',')[0].trim() : '';

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
                                    fetchedDetails[code] = { name: pageTitle, dimensions: dims };
                                    await docClient.send(new PutCommand({
                                        TableName: TABLE_NAME,
                                        Item: { PK: `ARTICLE#${code}`, SK: 'INFO', code: code, name: pageTitle, dimensions: dims, updatedAt: new Date().toISOString() }
                                    })).catch(() => {});
                                    continue;
                                }
                            }
                        } catch (e) {}

                        // AKO DIMENZIJE NISU PRONAĐENE, ARTIKL OSTAJE U LISTI S OVIM IMENOM:
                        fetchedDetails[code] = { name: `IKEA Artikl (${code})`, dimensions: null };
                    }
                }

                // --- SPAJANJE PODATAKA ---
                for (const [code, details] of Object.entries(fetchedDetails)) {
                    const item = itemsMap.get(code);
                    if (item) {
                        item.name = item.isDesignPart ? `↳ [Dio] ${details.name}` : details.name;
                        item.dimensions = details.dimensions;
                    }
                }

            } finally {
                if (browser) await browser.close();
            }
        });

        // --- IZRAČUN CIJENE I VOLUMENA ---
        const finalParsedItems = Array.from(itemsMap.values());
        let totalItemsCount = 0, totalVolume = 0, totalWeight = 0, hasMissingDimensions = false;

        for (const item of finalParsedItems) {
            totalItemsCount += item.quantity;
            if (item.dimensions) {
                const { length, width, height, weight } = item.dimensions;
                if (length > 170 || weight > 30) {
                    requiresVan = true;
                    foundBigItems.add(`${item.name} (${length}cm / ${weight}kg)`);
                }
                if (length && width && height) totalVolume += ((length * width * height) / 1000000) * item.quantity;
                else hasMissingDimensions = true;

                if (weight) totalWeight += weight * item.quantity;
                else hasMissingDimensions = true;
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
                totalVolume: Number(totalVolume.toFixed(3)),
                totalWeight: Number(totalWeight.toFixed(2)),
                hasMissingDimensions: hasMissingDimensions
            }
        });

    } catch (error) {
        console.error('Greška:', error.message);
        res.status(500).json({ success: false, error: 'Greška prilikom obrade IKEA liste.' });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => { console.log(`🚀 API pokrenut na portu ${PORT}`); });