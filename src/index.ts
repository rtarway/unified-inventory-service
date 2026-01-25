import express from 'express';
import cors from 'cors';
import { InventoryService } from './services/inventory-service';
import { startExpiryAgent } from './jobs/expiry-agent';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json());
app.use(cors());

const inventoryService = new InventoryService();

// Init connections
inventoryService.init().then(() => {
    console.log('Inventory Service Initialized');

    // Start Expiry Agent (Background)
    startExpiryAgent(60000); // Check every 60s
}).catch(err => {
    console.error('Failed to init services', err);
});

// Health Check
app.get('/health', (req, res) => {
    res.json({ status: 'ok' });
});

// GET /inventory/:sku?locationId=WEB
app.get('/inventory/:sku', async (req, res) => {
    try {
        const sku = req.params.sku;
        const locationId = (req.query.locationId as string) || "WEB";
        const window = parseInt((req.query.window as string) || "30");

        const result = await inventoryService.getUnifiedPosition(sku, locationId, window);
        res.json(result);
    } catch (e: any) {
        res.status(500).json({ error: e.message });
    }
});

// POST /inventory/query (Batch)
app.post('/inventory/query', async (req, res) => {
    try {
        const { skus, locationId, window } = req.body;
        if (!skus || !Array.isArray(skus)) {
            return res.status(400).json({ error: 'Missing skus array' });
        }

        const result = await inventoryService.getUnifiedPositionBatch(skus, locationId || "WEB", window || 30);
        res.json(result);
    } catch (e: any) {
        res.status(500).json({ error: e.message });
    }
});

// POST /reservations
app.post('/reservations', async (req, res) => {
    try {
        const { orderId, sku, qty, locationId, type, ttlMinutes } = req.body;
        if (!orderId || !sku || !qty) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        const result = await inventoryService.createReservation(
            orderId,
            sku,
            qty,
            locationId || "WEB",
            type || 'SOFT',
            ttlMinutes ? parseInt(ttlMinutes) : 15
        );
        res.json(result);
    } catch (e: any) {
        res.status(400).json({ error: e.message });
    }
});

// POST /shipments
app.post('/shipments', async (req, res) => {
    try {
        const { orderId, sku } = req.body;
        if (!orderId || !sku) {
            return res.status(400).json({ error: 'Missing required fields' });
        }
        const result = await inventoryService.shipAllocation(orderId, sku);
        res.json(result);
    } catch (e: any) {
        res.status(400).json({ error: e.message });
    }
});

// POST /allocations
app.post('/allocations', async (req, res) => {
    try {
        const { orderId, sku, qty, locationId } = req.body;
        if (!orderId || !sku || !qty) {
            return res.status(400).json({ error: 'Missing required fields' });
        }
        const result = await inventoryService.createAllocation(orderId, sku, qty, locationId || "WEB");
        res.json(result);
    } catch (e: any) {
        res.status(400).json({ error: e.message });
    }
});

// POST /cancellations
app.post('/cancellations', async (req, res) => {
    try {
        const { orderId, sku } = req.body;
        if (!orderId || !sku) {
            return res.status(400).json({ error: 'Missing required fields' });
        }
        const result = await inventoryService.createCancellation(orderId, sku);
        res.json(result);
    } catch (e: any) {
        res.status(400).json({ error: e.message });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(Number(PORT), '0.0.0.0', () => {
    console.log(`Unified Inventory Service running on port ${PORT}`);
});
