require('dotenv').config();
const express = require('express');
const cors = require('cors');
const puppeteer = require('puppeteer');
const multer = require('multer');
const fs = require('fs');

const app = express();

// Middleware
app.use(cors()); // Dozvoljava upite s tvog Nuxt frontenda
app.use(express.json()); // Dozvoljava čitanje JSON podataka u body-u

// Setup za primanje datoteka (PDF-ova)
const uploadDir = 'uploads/';
if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir);
}
const upload = multer({ dest: uploadDir });

// 1. HEALTH CHECK RUTA (za provjeru radi li server)
app.get('/health', (req, res) => {
    res.json({ status: 'ok', message: 'Usput Puppeteer API radi savršeno!' });
});

// 2. RUTA ZA IKEA LINK (Scraping)
app.post('/api/list-volume', async (req, res) => {
    const { url } = req.body;

    if (!url) {
        return res.status(400).json({ success: false, error: 'Nedostaje URL parametar.' });
    }

    let browser;
    try {
        console.log(`Zaprimljen zahtjev za analizu: ${url}`);

        // KRITIČNO ZA EC2: Parametri bez kojih Puppeteer puca na Linuxu
        browser = await puppeteer.launch({
            headless: 'new',
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu'
            ]
        });

        const page = await browser.newPage();

        // TODO: Ovdje će ići tvoja stvarna logika za navigaciju i vađenje podataka s IKEA-e
        // await page.goto(url, { waitUntil: 'networkidle2' });
        // const scrapedData = await page.evaluate(() => { ... });

        await browser.close();

        // Trenutno vraćamo mockirane podatke prema onome što očekuje tvoj Nuxt frontend
        res.json({
            success: true,
            data: {
                totalVolume: 1.85,
                totalWeight: 65.2,
                requiresVan: true,
                articlesFound: 2,
                parsedItems: [
                    {
                        code: '102.145.66',
                        name: 'PAX Ormar',
                        quantity: 1,
                        dimensions: { width: 50, height: 236, length: 60, weight: 45.2 }
                    },
                    {
                        code: '304.017.84',
                        name: 'MALM Komoda',
                        quantity: 1,
                        dimensions: { width: 48, height: 78, length: 80, weight: 20 }
                    }
                ]
            }
        });

    } catch (error) {
        if (browser) await browser.close();
        console.error('Greška u scraping procesu:', error);
        res.status(500).json({ success: false, error: 'Sustav nije uspio analizirati poveznicu. Provjerite je li link ispravan.' });
    }
});

// 3. RUTA ZA PDF RAČUN
app.post('/api/volume', upload.single('orderPdf'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ success: false, error: 'PDF datoteka nije pronađena.' });
        }

        console.log('Zaprimljen PDF:', req.file.originalname);

        // TODO: Logika za čitanje PDF-a (pdf-parse)

        // Brisanje PDF-a sa servera nakon obrade kako ne bismo zatrpali EC2 memoriju
        fs.unlinkSync(req.file.path);

        res.json({
            success: true,
            data: {
                totalVolume: 0.4,
                totalWeight: 15,
                requiresVan: false,
                articlesFound: 1,
                parsedItems: [
                    { code: '999.999.99', name: 'Testni IKEA Artikl iz PDF-a', quantity: 1, dimensions: null }
                ]
            }
        });
    } catch (error) {
        console.error('Greška pri obradi PDF-a:', error);
        res.status(500).json({ success: false, error: 'Neuspješna analiza dokumenta.' });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🚀 Usput Scraper API je pokrenut i sluša na portu ${PORT}`);
});