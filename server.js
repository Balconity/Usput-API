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

app.post('/api/list-volume', async (req, res) => {
    try {
        const listUrl = req.body.url;
        if (!listUrl || !listUrl.includes('ikea.com')) {
            return res.status(400).json({ success: false, error: 'Molimo unesite ispravnu IKEA poveznicu.' });
        }

        const itemsMap = new Map();

        const urlObj = new URL(listUrl);

        // 1. Čitanje direktno iz podijeljenog linka košarice (ako je u URL-u)
        if (listUrl.includes('/receive-share/')) {
            const pathSegments = urlObj.pathname.split('/');
            const shareIndex = pathSegments.indexOf('receive-share');
            if (shareIndex !== -1 && pathSegments.length > shareIndex + 1) {
                const itemsSegment = pathSegments[shareIndex + 1];
                if (itemsSegment && itemsSegment.includes(':')) {
                    itemsSegment.split(',').forEach((item) => {
                        const [code, qty] = item.split(':');
                        itemsMap.set(code, { code, quantity: parseInt(qty, 10) || 1 });
                    });
                }
            }
        }

        // 2. Skeniranje HTML-a ako artikli nisu bili u samom URL-u
        await browserQueue.add(async () => {
            let browser = await puppeteer.launch({
                headless: "new",
                args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--window-size=1280,800']
            });

            try {
                const page = await browser.newPage();
                await page.setViewport({ width: 1280, height: 800 });
                await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36');

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
                        itemsMap.set(item.code, { code: item.code, quantity: item.quantity });
                    }
                }

                const ArrayCodes = Array.from(itemsMap.values()).map(i => i.code);
                const fetchedDetails = {};
                const missingCodesToFetch = [];

                // 3. Provjera u bazi
                for (const code of ArrayCodes) {
                    try {
                        const dbResponse = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { PK: `ARTICLE#${code}`, SK: 'INFO' } }));
                        if (dbResponse.Item && dbResponse.Item.packages && dbResponse.Item.packages.length > 0) {
                            fetchedDetails[code] = dbResponse.Item;
                        } else {
                            missingCodesToFetch.push(code);
                        }
                    } catch (e) { missingCodesToFetch.push(code); }
                }

                // 4. Dohvaćanje s weba i RAZBIJANJE NA PAKETE
                if (missingCodesToFetch.length > 0) {
                    for (const code of missingCodesToFetch) {
                        try {
                            const response = await fetch(`https://www.ikea.com/hr/hr/p/-${code}/`, {
                                headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
                            });

                            if (response.ok) {
                                const htmlText = await response.text();

                                let itemPrice = 0;
                                let pageTitle = '';
                                let imageUrl = '';
                                let extractedPackages = [];

                                const hydrateMatches = htmlText.match(/<script[^>]*type="text\/hydrate"[^>]*>([\s\S]*?)<\/script>/g);

                                if (hydrateMatches) {
                                    for (const scriptBlock of hydrateMatches) {
                                        try {
                                            const cleanJson = scriptBlock.replace(/<script[^>]*>|<\/script>/g, '').trim();
                                            const payload = JSON.parse(cleanJson);
                                            const productData = payload?.product || payload?.pageProps?.product;

                                            if (productData) {
                                                if (productData.price && itemPrice === 0) itemPrice = productData.price;
                                                if (productData.name && !pageTitle) pageTitle = `${productData.name} ${productData.description || productData.typeName || ''}`.trim();
                                                if (productData.mediaList && productData.mediaList.length > 0 && !imageUrl) {
                                                    imageUrl = productData.mediaList[0].content?.url || '';
                                                }

                                                // --- KLJUČNA LOGIKA: ČITANJE PAKETA IZ JSON-a ---
                                                if (productData.packaging && productData.packaging.packages && extractedPackages.length === 0) {
                                                    productData.packaging.packages.forEach(pkg => {
                                                        const qtyInOneProduct = pkg.quantity?.value || 1;
                                                        const pkgName = pkg.name || pageTitle;
                                                        const pkgTypeName = pkg.typeName || '';
                                                        const pkgCode = pkg.articleNumber?.value || pkg.articleNumber || pkg.itemNo || code;

                                                        let w = 0, h = 0, l = 0, wt = 0;

                                                        pkg.measurementGroups?.forEach(mg => {
                                                            mg.measurements?.forEach(m => {
                                                                if (m.type === 'width') w = m.value;
                                                                if (m.type === 'height') h = m.value;
                                                                if (m.type === 'length') l = m.value;
                                                                if (m.type === 'weight') wt = m.value;
                                                            });
                                                        });

                                                        const vol = Number(((w * h * l) / 1000000).toFixed(4));

                                                        extractedPackages.push({
                                                            code: String(pkgCode).replace(/\D/g, ''),
                                                            name: `${pkgName} ${pkgTypeName}`.trim(),
                                                            quantityPerProduct: qtyInOneProduct,
                                                            dimensions: { width: w, height: h, length: l, weight: wt, volume: vol }
                                                        });
                                                    });
                                                }
                                                // Fallback za proizvode s jednostavnim nizom 'packageMeasurements'
                                                else if (productData.packageMeasurements && productData.packageMeasurements.length > 0 && extractedPackages.length === 0) {
                                                    productData.packageMeasurements.forEach((pkg, idx) => {
                                                        const w = pkg.width?.value || 0;
                                                        const h = pkg.height?.value || 0;
                                                        const l = pkg.length?.value || 0;
                                                        const wt = pkg.weight?.value || 0;
                                                        const vol = Number(((w * h * l) / 1000000).toFixed(4));

                                                        extractedPackages.push({
                                                            code: `${code}-PKG${idx+1}`,
                                                            name: `${pageTitle} (Paket ${idx+1})`,
                                                            quantityPerProduct: 1,
                                                            dimensions: { width: w, height: h, length: l, weight: wt, volume: vol }
                                                        });
                                                    });
                                                }
                                            }
                                        } catch(err) {}
                                    }
                                }

                                // Spremi u bazu za idući put
                                fetchedDetails[code] = {
                                    name: pageTitle || `IKEA Artikl (${code})`,
                                    price: itemPrice || 0,
                                    image: imageUrl || '',
                                    packages: extractedPackages
                                };

                                await docClient.send(new PutCommand({
                                    TableName: TABLE_NAME,
                                    Item: {
                                        PK: `ARTICLE#${code}`,
                                        SK: 'INFO',
                                        code: code,
                                        name: fetchedDetails[code].name,
                                        price: fetchedDetails[code].price,
                                        image: fetchedDetails[code].image,
                                        packages: fetchedDetails[code].packages,
                                        updatedAt: new Date().toISOString()
                                    }
                                })).catch(() => {});
                            }
                        } catch (e) {}
                    }
                }

                // Spajamo podatke iz baze/mreže s naručenom količinom
                for (const [code, details] of Object.entries(fetchedDetails)) {
                    const item = itemsMap.get(code);
                    if (item) {
                        item.name = details.name;
                        item.price = details.price;
                        item.image = details.image;
                        item.packages = details.packages || [];
                    }
                }

            } finally {
                if (browser) await browser.close();
            }
        });

        // 5. OBRADA SVIH KUTIJA I IZRAČUN VOLUMENA/TEŽINE
        const finalFlattenedItems = [];
        let totalVolume = 0;
        let totalWeight = 0;
        let totalBoxes = 0;
        let hasMissingDimensions = false;


        for (const mainItem of Array.from(itemsMap.values())) {

            const cartQuantity = mainItem.quantity; // Koliko je komada glavnog proizvoda u košarici
            let isFirstBox = true; // Zbog praćenja cijene da je ne dupliciramo

            if (mainItem.packages && mainItem.packages.length > 0) {
                mainItem.packages.forEach(box => {
                    // Množimo količinu kutija po proizvodu s naručenom količinom proizvoda
                    const finalBoxQuantity = box.quantityPerProduct * cartQuantity;
                    totalBoxes += finalBoxQuantity;

                    if (box.dimensions) {
                        totalVolume += (box.dimensions.volume * finalBoxQuantity);
                        totalWeight += (box.dimensions.weight * finalBoxQuantity);
                    } else {
                        hasMissingDimensions = true;
                    }

                    finalFlattenedItems.push({
                        code: box.code,
                        name: box.name,
                        image: mainItem.image,
                        price: isFirstBox ? mainItem.price : 0,
                        quantity: finalBoxQuantity,
                        dimensions: box.dimensions
                    });

                    isFirstBox = false;
                });
            } else {
                hasMissingDimensions = true;
            }
        }

        res.json({
            success: true,
            data: {
                articlesFound: totalBoxes,
                parsedItems: finalFlattenedItems,
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