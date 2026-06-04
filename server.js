require('dotenv').config();
const express = require('express');
const cors = require('cors');
const puppeteer = require('puppeteer');
const multer = require('multer');
const fs = require('fs');
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand, PutCommand } = require("@aws-sdk/lib-dynamodb");

const app = express();
app.use(cors());
app.use(express.json());

// ============================================================================
// 1. DYNAMODB KONFIGURACIJA
// ============================================================================
// Na EC2 serveru, ako IAM rola ima pristup, ovo radi automatski.
// Za lokalno testiranje trebat ćeš AWS_ACCESS_KEY_ID u .env datoteci.
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
                try {
                    resolve(await task());
                } catch (err) {
                    reject(err);
                } finally {
                    this.running--;
                    this.next();
                }
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
// 3. GLAVNA API FUNKCIJA (Tvoja skripta prebačena u Express)
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
        let generatedDesignLink = null;
        let designQty = 1;

        console.log('\n--- NOVI ZAHTJEV ZAPRIMLJEN ---', listUrl);

        if (listUrl.includes('/receive-share/')) {
            const urlObj = new URL(listUrl);
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

            const designsParam = urlObj.searchParams.get('designs');
            if (designsParam) {
                const parts = designsParam.split(',')[0].split(':');
                const designId = parts[0];
                const familyUpper = parts[1] ? parts[1].toUpperCase() : 'KOMBINACIJA';
                designQty = parseInt(parts[2], 10) || 1;
                if (PLANNER_URLS[familyUpper]) generatedDesignLink = PLANNER_URLS[familyUpper] + designId;
            }
        }

        await browserQueue.add(async () => {
            let browser = await puppeteer.launch({
                headless: 'new', // Novo pravilo za Puppeteer
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage', // Važno za EC2
                    '--disable-gpu'
                ]
            });

            try {
                const page = await browser.newPage();
                await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');

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

                    if (results.length === 0) {
                        document.querySelectorAll('[data-testid^="product-name-"]').forEach(el => {
                            const testId = el.getAttribute('data-testid') || '';
                            const codeMatch = testId.match(/product-name-(.+)/);
                            if (codeMatch && codeMatch[1]) {
                                const code = codeMatch[1].replace(/\D/g, '');
                                let qty = 1;
                                const container = el.closest('div, li');
                                if (container) {
                                    const input = container.querySelector('input[type="number"]');
                                    if (input && input.value) qty = parseInt(input.value, 10) || 1;
                                }
                                if (code.length >= 6) results.push({ code, quantity: qty });
                            }
                        });
                    }
                    return results;
                });

                for (const item of scrapedListItems) {
                    if (itemsMap.has(item.code)) {
                        itemsMap.get(item.code).quantity = Math.max(itemsMap.get(item.code).quantity, item.quantity);
                    } else {
                        itemsMap.set(item.code, { code: item.code, name: 'Učitavam...', quantity: item.quantity, dimensions: null, isDesignPart: false });
                    }
                }

                if (generatedDesignLink) {
                    await page.setRequestInterception(true);
                    page.removeAllListeners('request');
                    page.removeAllListeners('response');

                    page.on('request', req => req.continue());
                    page.on('response', async res => {
                        const req = res.request();
                        if ((req.resourceType() === 'fetch' || req.resourceType() === 'xhr') && res.url().includes('ikea.com')) {
                            try {
                                const json = await res.json();
                                const extractCodes = (obj) => {
                                    if (!obj || typeof obj !== 'object') return;
                                    const code = obj.articleNumber || obj.itemNo || obj.id || obj.partNumber;
                                    const qty = obj.quantity || obj.qty || obj.count;

                                    if (code && qty !== undefined && typeof code === 'string') {
                                        const cleanCode = code.replace(/\D/g, '');
                                        if (cleanCode.length >= 6 && cleanCode.length <= 10) {
                                            const finalQty = (parseInt(qty, 10) || 1) * designQty;
                                            if (itemsMap.has(cleanCode)) {
                                                itemsMap.get(cleanCode).quantity += finalQty;
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
                            } catch(e) {}
                        }
                    });

                    await page.goto(generatedDesignLink, { waitUntil: 'networkidle0', timeout: 30000 });
                }

                await page.setRequestInterception(false);
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
                            console.log(`Pronađeno u bazi: ${code}`);
                        } else {
                            missingCodesToFetch.push(code);
                        }
                    } catch (dbError) {
                        console.error(`Greška pri dohvaćanju šifre ${code} iz baze (možda fali AWS rola):`, dbError.message);
                        missingCodesToFetch.push(code); // Ako baza zakaže, svejedno grebajmo web
                    }
                }

                if (missingCodesToFetch.length > 0) {
                    console.log(`Grebem s weba ${missingCodesToFetch.length} novih artikala...`);
                    const newWebDetails = await page.evaluate(async (codes) => {
                        const results = {};
                        const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

                        for (const code of codes) {
                            try {
                                const response = await fetch(`https://www.ikea.com/hr/hr/p/-${code}/`);
                                if (response.ok) {
                                    const htmlText = await response.text();
                                    const doc = new DOMParser().parseFromString(htmlText, 'text/html');

                                    let pageTitle = doc.querySelector('title')?.textContent || '';
                                    pageTitle = pageTitle.replace(/- IKEA/i, '').trim();
                                    pageTitle = pageTitle.split(',')[0].trim();

                                    let width = 0, height = 0, length = 0, weight = 0;
                                    const packagingSection = doc.querySelector('#pip-packaging-tab-panel, .pip-product-details__packaging, [data-testid="pip-packaging-tab"]');
                                    const textToSearch = packagingSection ? (packagingSection.textContent || '') : (doc.body.textContent || '');

                                    const wMatches = [...textToSearch.matchAll(/Širina:\s*(\d+)\s*cm/gi)];
                                    const hMatches = [...textToSearch.matchAll(/Visina:\s*(\d+)\s*cm/gi)];
                                    const lMatches = [...textToSearch.matchAll(/Duljina:\s*(\d+)\s*cm/gi)];
                                    const wtMatches = [...textToSearch.matchAll(/Težina:\s*([\d,.]+)\s*kg/gi)];

                                    if (lMatches.length > 0) length = Math.max(...lMatches.map(m => parseInt(m[1], 10)));
                                    else if (wMatches.length > 0) length = Math.max(...wMatches.map(m => parseInt(m[1], 10)));

                                    if (wMatches.length > 0) width = Math.max(...wMatches.map(m => parseInt(m[1], 10)));
                                    if (hMatches.length > 0) height = hMatches.map(m => parseInt(m[1], 10)).reduce((a, b) => a + b, 0);
                                    if (wtMatches.length > 0) weight = wtMatches.map(m => parseFloat(m[1].replace(',', '.'))).reduce((a, b) => a + b, 0);

                                    const dims = (width > 0 || length > 0 || weight > 0) ? { width, height, length, weight } : null;

                                    if (pageTitle && pageTitle.toUpperCase() !== 'IKEA') {
                                        results[code] = { name: pageTitle, dimensions: dims };
                                        await sleep(Math.floor(Math.random() * 400) + 300);
                                        continue;
                                    }
                                }
                            } catch (e) {}

                            results[code] = { name: `IKEA Artikl (${code})`, dimensions: null };
                            await sleep(300);
                        }
                        return results;
                    }, missingCodesToFetch);

                    for (const [code, details] of Object.entries(newWebDetails)) {
                        fetchedDetails[code] = details;

                        if (details.name && !details.name.includes('IKEA Artikl')) {
                            try {
                                await docClient.send(new PutCommand({
                                    TableName: TABLE_NAME,
                                    Item: {
                                        PK: `ARTICLE#${code}`,
                                        SK: 'INFO',
                                        code: code,
                                        name: details.name,
                                        dimensions: details.dimensions,
                                        updatedAt: new Date().toISOString()
                                    }
                                }));
                                console.log(`Spremljeno u bazu: ${code}`);
                            } catch (putError) {
                                console.error(`Greška pri spremanju u bazu za šifru ${code}:`, putError.message);
                            }
                        }
                    }
                }

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
                    const itemVolume = (length * width * height) / 1000000;
                    totalVolume += (itemVolume * item.quantity);
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
                designLink: generatedDesignLink,
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