require('dotenv').config();
const express = require('express');
const cors = require('cors');
const puppeteer = require('puppeteer');
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand, PutCommand } = require("@aws-sdk/lib-dynamodb");

const app = express();
app.use(cors());
app.use(express.json());

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

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
                        itemsMap.set(code, { code, name: `Učitavam...`, quantity: parseInt(qty, 10) || 1, dimensions: null, price: 0, image: '', isDesignPart: false });
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
                    console.log(`[Informacija] Dizajn detektiran! Obitelj: ${familyUpper}, Šifra dizajna: ${designId}`);
                    generatedDesigns.push({
                        family: familyUpper,
                        code: designId,
                        link: PLANNER_URLS[familyUpper],
                        qty: currentDesignQty
                    });
                }
            });
        }

        await browserQueue.add(async () => {
            let browser = await puppeteer.launch({
                headless: "new",
                args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--window-size=1280,800']
            });

            try {
                const page = await browser.newPage();
                await page.setViewport({ width: 1280, height: 800 });
                await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');

                console.log(`[Glavna stranica] Skeniram obične artikle...`);
                await page.goto(listUrl, { waitUntil: 'networkidle2', timeout: 30000 }).catch(() => {});
                await delay(3000);

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
                        itemsMap.set(item.code, { code: item.code, name: 'Učitavam...', quantity: item.quantity, dimensions: null, price: 0, image: '', isDesignPart: false });
                    }
                }

                if (generatedDesigns.length > 0) {
                    for (const design of generatedDesigns) {
                        let networkCaughtItems = [];

                        const responseHandler = async (res) => {
                            const req = res.request();
                            if (req.resourceType() === 'fetch' || req.resourceType() === 'xhr') {
                                try {
                                    const json = await res.json();
                                    const extractCodes = (obj) => {
                                        if (!obj || typeof obj !== 'object') return;
                                        const code = obj.articleNumber || obj.itemNo || obj.articleNo || obj.itemNumber || obj.partNumber || obj.id || obj.articleCode || obj.productId;
                                        const qty = obj.quantity !== undefined ? obj.quantity : (obj.qty !== undefined ? obj.qty : obj.count);

                                        if (code !== undefined && qty !== undefined) {
                                            const cleanCode = String(code).replace(/\D/g, '');
                                            if (cleanCode.length >= 6 && cleanCode.length <= 10) {
                                                networkCaughtItems.push({ code: cleanCode, quantity: parseInt(qty, 10) || 1 });
                                            }
                                        }
                                        for (const key in obj) {
                                            if (Object.prototype.hasOwnProperty.call(obj, key)) extractCodes(obj[key]);
                                        }
                                    };
                                    extractCodes(json);
                                } catch (e) {}
                            }
                        };

                        page.on('response', responseHandler);

                        try {
                            console.log(`[Dizajner - Korak 1] Otvaram IKEA planer...`);
                            await page.goto(design.link, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
                            await delay(6000);

                            console.log(`[Dizajner] Rješavam kolačiće...`);
                            await page.evaluate(() => {
                                const cookieBtn = document.querySelector('#onetrust-accept-btn-handler');
                                if (cookieBtn) cookieBtn.click();
                            });
                            await delay(1500);

                            console.log(`[Dizajner - Korak 2] Otvaram Izbornik (Menu)...`);
                            await page.evaluate(() => {
                                const menuHost = document.querySelector('[data-element-name="kompis-menu"]');
                                if (menuHost && menuHost.shadowRoot) {
                                    const shadowBtn = menuHost.shadowRoot.querySelector('[data-element-name="skapa-icon-button"]');
                                    if (shadowBtn) {
                                        const innerBtn = shadowBtn.shadowRoot?.querySelector('button');
                                        if (innerBtn) innerBtn.click();
                                        else shadowBtn.click();
                                    }
                                }
                            });
                            await delay(2000);

                            console.log(`[Dizajner - Korak 3] Klikam "Otvori šifru dizajna"...`);
                            await page.evaluate(() => {
                                const menuCard = document.querySelector('[data-element-name="kompis-menu-card"]');
                                if (menuCard && menuCard.shadowRoot) {
                                    const targetBtn = menuCard.shadowRoot.getElementById('openDesignCodeButtonClick');
                                    if (targetBtn) targetBtn.click();
                                }
                            });
                            await delay(2500);

                            console.log(`[Dizajner - Korak 4] Unosim kod dizajna: ${design.code}...`);
                            try {
                                const inputFound = await page.evaluate(() => {
                                    const card = document.querySelector('[data-element-name="kompis-open-design-card"]');
                                    if (!card || !card.shadowRoot) return false;
                                    const inputContainer = card.shadowRoot.querySelector('[data-element-name="kompis-open-design-code-input"]');
                                    if (!inputContainer || !inputContainer.shadowRoot) return false;
                                    const input = inputContainer.shadowRoot.getElementById('open-design-code-input-input');
                                    if (!input) return false;
                                    input.focus(); input.click();
                                    return true;
                                });

                                if (inputFound) {
                                    await page.keyboard.type(design.code, { delay: 150 });
                                    await delay(1000);
                                    await page.keyboard.press('Enter');
                                    await delay(1500);
                                    await page.evaluate(() => {
                                        const card = document.querySelector('[data-element-name="kompis-open-design-card"]');
                                        if (card && card.shadowRoot) {
                                            const openBtn = card.shadowRoot.querySelector('[data-element-name="skapa-button"][a11y-label="Otvori"], [data-element-name="skapa-button"] [aria-label="Otvori"]');
                                            if (openBtn) {
                                                openBtn.click();
                                                if (openBtn.shadowRoot) {
                                                    const innerBtn = openBtn.shadowRoot.querySelector('button');
                                                    if (innerBtn) innerBtn.click();
                                                }
                                            }
                                        }
                                    });
                                }
                            } catch (e) {}

                            console.log(`[Dizajner - Korak 5] Čekam učitavanje 3D prikaza...`);
                            await delay(6000);

                            console.log(`[Dizajner - Korak 6] Klikam na Sažetak...`);
                            await page.evaluate(() => {
                                const header = document.querySelector('[data-element-name="kompis-planning-header"]');
                                if (header && header.shadowRoot) {
                                    const summaryComponent = header.shadowRoot.querySelector('[data-element-name="kompis-mini-configuration-summary"]');
                                    if (summaryComponent && summaryComponent.shadowRoot) {
                                        const summaryBtn = summaryComponent.shadowRoot.querySelector('[data-testid="configurationSummaryPrimaryButton"]');
                                        if (summaryBtn) {
                                            summaryBtn.click();
                                            if (summaryBtn.shadowRoot) {
                                                const innerBtn = summaryBtn.shadowRoot.querySelector('button');
                                                if (innerBtn) innerBtn.click();
                                            }
                                        }
                                    }
                                }
                            });
                            await delay(4000);

                            console.log(`[Dizajner - Korak 7] Preskačem upitnik za ručke...`);
                            for (let i = 0; i < 2; i++) {
                                if (page.url().includes('summary')) break;
                                await page.evaluate(() => {
                                    const btns = Array.from(document.querySelectorAll('button, [data-element-name="skapa-button"]'));
                                    const skipBtn = btns.find(b => b.textContent && (b.textContent.toLowerCase() === 'ne' || b.textContent.toLowerCase().includes('bez') || b.textContent.toLowerCase().includes('preskoči')));
                                    if (skipBtn) skipBtn.click();
                                });
                                await delay(2000);
                            }

                            console.log(`[Dizajner - Korak 8] Skupljam mrežne podatke...`);
                            await delay(3000);
                            page.off('response', responseHandler);

                            console.log(`[Dizajner - Gotovo] Ulovljeno ${networkCaughtItems.length} stavki.`);
                            networkCaughtItems.forEach(item => {
                                const finalQty = item.quantity * design.qty;
                                if (itemsMap.has(item.code)) {
                                    itemsMap.get(item.code).quantity += finalQty;
                                } else {
                                    itemsMap.set(item.code, { code: item.code, name: 'Dio dizajna', quantity: finalQty, dimensions: null, price: 0, image: '', isDesignPart: true });
                                }
                            });

                        } catch (plannerErr) {
                            page.off('response', responseHandler);
                            console.error(`[Dizajner] Greška: ${plannerErr.message}`);
                        }
                    }
                }

                const ArrayCodes = Array.from(itemsMap.values()).map(i => i.code);
                const missingCodesToFetch = [];
                const fetchedDetails = {};

                for (const code of ArrayCodes) {
                    try {
                        const dbResponse = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { PK: `ARTICLE#${code}`, SK: 'INFO' } }));
                        if (dbResponse.Item && dbResponse.Item.name && dbResponse.Item.dimensions) {
                            fetchedDetails[code] = dbResponse.Item;
                        } else {
                            missingCodesToFetch.push(code);
                        }
                    } catch (e) { missingCodesToFetch.push(code); }
                }

                if (missingCodesToFetch.length > 0) {
                    console.log(`[Server] Direktno povlačim podatke, cijene i slike za ${missingCodesToFetch.length} novih komada...`);
                    for (const code of missingCodesToFetch) {
                        try {
                            const response = await fetch(`https://www.ikea.com/hr/hr/p/-${code}/`, {
                                headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
                            });

                            if (response.ok) {
                                const htmlText = await response.text();

                                let itemPrice = 0;
                                let pageTitle = '';
                                let width = 0, height = 0, length = 0, weight = 0;
                                let imageUrl = '';

                                const hydrateMatches = htmlText.match(/<script[^>]*type="text\/hydrate"[^>]*>([\s\S]*?)<\/script>/g);

                                if (hydrateMatches) {
                                    for (const scriptBlock of hydrateMatches) {
                                        try {
                                            const cleanJson = scriptBlock.replace(/<script[^>]*>|<\/script>/g, '').trim();
                                            const payload = JSON.parse(cleanJson);

                                            if (payload && payload.product) {
                                                const p = payload.product;
                                                if (p.price && itemPrice === 0) itemPrice = p.price;
                                                if (p.name && !pageTitle) pageTitle = `${p.name} ${p.description || p.typeName || ''}`.trim();
                                                if (p.mediaList && p.mediaList.length > 0 && !imageUrl) {
                                                    imageUrl = p.mediaList[0].content?.url || '';
                                                }

                                                if (p.packageMeasurements && p.packageMeasurements.length > 0 && width === 0) {
                                                    p.packageMeasurements.forEach(pkg => {
                                                        width = Math.max(width, pkg.width?.value || 0);
                                                        height = Math.max(height, pkg.height?.value || 0);
                                                        length = Math.max(length, pkg.length?.value || 0);
                                                        weight += (pkg.weight?.value || 0);
                                                    });
                                                }

                                                if (p.packaging && p.packaging.packages && width === 0) {
                                                    p.packaging.packages.forEach(pkg => {
                                                        let qty = pkg.quantity?.value || 1;
                                                        pkg.measurementGroups?.forEach(mg => {
                                                            mg.measurements?.forEach(m => {
                                                                if (m.type === 'width') width = Math.max(width, m.value);
                                                                if (m.type === 'height') height = Math.max(height, m.value);
                                                                if (m.type === 'length') length = Math.max(length, m.value);
                                                                if (m.type === 'weight') weight += (m.value * qty);
                                                            });
                                                        });
                                                    });
                                                }
                                            }

                                            if (payload && payload.pageProps && payload.pageProps.product) {
                                                const p = payload.pageProps.product;
                                                if (p.price && itemPrice === 0) itemPrice = p.price;
                                                if (p.name && !pageTitle) pageTitle = `${p.name} ${p.description || p.typeName || ''}`.trim();

                                                if (p.packaging && p.packaging.packages && width === 0) {
                                                    p.packaging.packages.forEach(pkg => {
                                                        let qty = pkg.quantity?.value || 1;
                                                        pkg.measurementGroups?.forEach(mg => {
                                                            mg.measurements?.forEach(m => {
                                                                if (m.type === 'width') width = Math.max(width, m.value);
                                                                if (m.type === 'height') height = Math.max(height, m.value);
                                                                if (m.type === 'length') length = Math.max(length, m.value);
                                                                if (m.type === 'weight') weight += (m.value * qty);
                                                            });
                                                        });
                                                    });
                                                }
                                            }
                                        } catch(err) {}
                                    }
                                }

                                if (itemPrice === 0) {
                                    const prMatch = htmlText.match(/"product_prices":\s*\["([\d.]+)"\]/);
                                    if (prMatch) itemPrice = parseFloat(prMatch[1]);
                                }
                                if (!pageTitle) {
                                    const titleMatch = htmlText.match(/<title>([^<]+)<\/title>/i);
                                    pageTitle = titleMatch ? titleMatch[1].replace(/- IKEA/i, '').trim() : `IKEA Artikl (${code})`;
                                }
                                if (!imageUrl) {
                                    const imgMatch = htmlText.match(/<meta[^>]*property="og:image"[^>]*content="([^"]+)"/i) || htmlText.match(/<meta[^>]*content="([^"]+)"[^>]*property="og:image"/i);
                                    if (imgMatch) imageUrl = imgMatch[1];
                                }
                                if (width === 0 && length === 0 && weight === 0) {
                                    const wMatch = htmlText.match(/Širina:\s*(\d+)\s*cm/i);
                                    const hMatch = htmlText.match(/Visina:\s*(\d+)\s*cm/i);
                                    const lMatch = htmlText.match(/Duljina:\s*(\d+)\s*cm/i);
                                    const wtMatch = htmlText.match(/Težina:\s*([\d,.]+)\s*kg/i);

                                    if (lMatch) length = parseInt(lMatch[1], 10);
                                    if (wMatch) width = parseInt(wMatch[1], 10);
                                    if (hMatch) height = parseInt(hMatch[1], 10);
                                    if (wtMatch) weight = parseFloat(wtMatch[1].replace(',', '.'));
                                    if (!length && width) length = width;
                                }

                                const dims = (width > 0 || length > 0 || weight > 0) ? { width, height, length, weight } : null;
                                fetchedDetails[code] = { name: pageTitle, dimensions: dims, price: itemPrice, image: imageUrl };

                                await docClient.send(new PutCommand({
                                    TableName: TABLE_NAME,
                                    Item: {
                                        PK: `ARTICLE#${code}`,
                                        SK: 'INFO',
                                        code: code,
                                        name: pageTitle,
                                        dimensions: dims,
                                        price: itemPrice,
                                        image: imageUrl,
                                        updatedAt: new Date().toISOString()
                                    }
                                })).catch(() => {});
                                continue;
                            }
                        } catch (e) {}
                        fetchedDetails[code] = { name: `IKEA Artikl (${code})`, dimensions: null, price: 0, image: '' };
                    }
                }

                for (const [code, details] of Object.entries(fetchedDetails)) {
                    const item = itemsMap.get(code);
                    if (item) {
                        item.name = details.name;
                        item.dimensions = details.dimensions;
                        item.price = details.price || 0;
                        item.image = details.image || '';
                    }
                }

            } finally {
                if (browser) await browser.close();
            }
        });

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

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => { console.log(`🚀 API pokrenut na portu ${PORT}`); });