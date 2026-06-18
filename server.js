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

// Povećano na 3 taba jer server sada ima 4GB RAM-a
const browserQueue = new TaskQueue(3);

const PLANNER_URLS = {
    'PLATSA': 'https://www.ikea.com/addon-app/storageone/platsa/web/latest/hr/hr/#/planner',
    'PAX': 'https://www.ikea.com/addon-app/storageone/pax/web/latest/hr/hr/#/planner',
    'BESTA': 'https://www.ikea.com/addon-app/storageone/besta/web/latest/hr/hr/#/planner',
    'BESTÅ': 'https://www.ikea.com/addon-app/storageone/besta/web/latest/hr/hr/#/planner',
    'METOD': 'https://kitchen.planner.ikea.com/hr/hr/'
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
                    generatedDesigns.push({
                        family: familyUpper,
                        code: designId,
                        link: PLANNER_URLS[familyUpper] + '?vpc=' + designId,
                        qty: currentDesignQty
                    });
                }
            });
        }

        await browserQueue.add(async () => {
            let browser = await puppeteer.launch({
                headless: 'new',
                args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--window-size=1280,800']
            });

            try {
                const page = await browser.newPage();
                await page.setViewport({ width: 1280, height: 800 });
                await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

                console.log(`[Glavna stranica] Učitavam ${listUrl}...`);
                await page.goto(listUrl, { waitUntil: 'networkidle2', timeout: 30000 }).catch(() => {});
                await new Promise(r => setTimeout(r, 3000));

                const scrapedListItems = await page.evaluate(() => {
                    const results = [];
                    const allText = document.body.innerText || '';
                    const matches = [...allText.matchAll(/\b(\d{3}\.\d{3}\.\d{2})\b/g)];
                    matches.forEach(m => {
                        const code = m[1].replace(/\./g, '');
                        if (!results.some(r => r.code === code)) {
                            results.push({ code, quantity: 1 });
                        }
                    });
                    return results;
                });

                for (const item of scrapedListItems) {
                    if (!itemsMap.has(item.code)) {
                        itemsMap.set(item.code, { code: item.code, name: 'Učitavam...', quantity: item.quantity, dimensions: null, isDesignPart: false });
                    }
                }

                if (generatedDesigns.length > 0) {
                    for (const design of generatedDesigns) {
                        try {
                            console.log(`[Dizajner] Oponašam korisnika za: ${design.family} -> kod: ${design.code}`);

                            // PASIVNO PRISLUŠKIVANJE MREŽE - Slušamo JSON u letu!
                            let networkCaughtItems = [];

                            const responseHandler = async (res) => {
                                const req = res.request();
                                if (req.resourceType() === 'fetch' || req.resourceType() === 'xhr') {
                                    try {
                                        const json = await res.json();

                                        // Rekurzivna funkcija koja traži bilo koji ključ s artiklima
                                        const extractCodes = (obj) => {
                                            if (!obj || typeof obj !== 'object') return;

                                            const code = obj.articleNumber || obj.itemNo || obj.articleNo || obj.itemNumber || obj.partNumber || obj.id || obj.articleCode || obj.productId;
                                            const qty = obj.quantity !== undefined ? obj.quantity : (obj.qty !== undefined ? obj.qty : obj.count);

                                            if (code !== undefined && qty !== undefined) {
                                                const cleanCode = String(code).replace(/\D/g, '');
                                                // IKEA kodovi su uvijek 8 znamenki kad se maknu točke
                                                if (cleanCode.length >= 6 && cleanCode.length <= 10) {
                                                    networkCaughtItems.push({ code: cleanCode, quantity: parseInt(qty, 10) || 1 });
                                                }
                                            }

                                            for (const key in obj) {
                                                if (Object.prototype.hasOwnProperty.call(obj, key)) extractCodes(obj[key]);
                                            }
                                        };
                                        extractCodes(json);
                                    } catch (e) {
                                        // Prazni odgovori se ignoriraju
                                    }
                                }
                            };

                            // Uključujemo slušalicu
                            page.on('response', responseHandler);

                            // Odlazimo na dizajn link i čekamo
                            await page.goto(design.link, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
                            await new Promise(r => setTimeout(r, 8000));

                            // Klik na "Prihvati kolačiće" ako smeta na ekranu
                            await page.evaluate(() => {
                                const cookieBtn = document.querySelector('#onetrust-accept-btn-handler');
                                if (cookieBtn) cookieBtn.click();
                            });

                            // Gasimo slušalicu da ne opterećujemo memoriju
                            page.off('response', responseHandler);

                            console.log(`[Dizajner] Preko mreže "u letu" ulovljeno ${networkCaughtItems.length} artikala.`);

                            // Dodajemo ulovljene dizajn artikle na našu glavnu listu
                            networkCaughtItems.forEach(item => {
                                const finalQty = item.quantity * design.qty;
                                if (itemsMap.has(item.code)) {
                                    // Povećavamo samo ako je dizajn donio veću količinu ili zbrajamo
                                    itemsMap.get(item.code).quantity += finalQty;
                                } else {
                                    itemsMap.set(item.code, {
                                        code: item.code,
                                        name: 'Dio dizajna',
                                        quantity: finalQty,
                                        dimensions: null,
                                        isDesignPart: true
                                    });
                                }
                            });

                        } catch (plannerErr) {
                            console.error(`[Dizajner] Greška pri obradi dizajna, ignoriram: ${plannerErr.message}`);
                        }
                    }
                }

                // --- BAZA PODATAKA (DynamoDB / Fetch) ---
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

                if (missingCodesToFetch.length > 0) {
                    console.log(`[Server] Skidam podatke za ${missingCodesToFetch.length} novih komada...`);
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

                        fetchedDetails[code] = { name: `IKEA Artikl (${code})`, dimensions: null };
                    }
                }

                // --- SPAJANJE PODATAKA ---
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
        console.error('Kritična greška:', error.message);
        res.status(500).json({ success: false, error: 'Greška prilikom obrade IKEA liste.' });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => { console.log(`🚀 API pokrenut na portu ${PORT}`); });