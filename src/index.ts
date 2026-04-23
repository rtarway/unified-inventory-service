import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { InventoryService } from './services/inventory-service';
import { startExpiryAgent } from './jobs/expiry-agent';
import dotenv from 'dotenv';

dotenv.config();

const isProd = process.env.NODE_ENV === 'production';

const MAX_BATCH_SKUS = (() => {
    const n = parseInt(process.env.MAX_BATCH_SKUS || '500', 10);
    if (!Number.isFinite(n) || n < 1) return 500;
    return Math.min(n, 10000);
})();

const corsOrigins = process.env.CORS_ORIGINS?.split(',').map((s) => s.trim()).filter(Boolean);

function clientErrorMessage(err: unknown, statusCode: number): string {
    if (statusCode >= 500 && isProd) {
        return 'Internal server error';
    }
    if (err instanceof Error) return err.message;
    return 'Request failed';
}

function logAndRespond(res: Response, err: unknown, statusCode: number) {
    console.error(err);
    res.status(statusCode).json({ error: clientErrorMessage(err, statusCode) });
}

function requireApiToken(req: Request, res: Response, next: NextFunction) {
    const expected = process.env.API_TOKEN;
    if (!expected) return next();

    const hdr = req.headers.authorization;
    if (!hdr || !hdr.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    const token = hdr.slice(7);
    if (token !== expected) {
        return res.status(401).json({ error: 'Unauthorized' });
    }
    next();
}

const app = express();
app.use(helmet());
app.use(cors(corsOrigins?.length ? { origin: corsOrigins } : { origin: false }));
app.use(express.json({ limit: process.env.JSON_BODY_LIMIT || '256kb' }));

const inventoryService = new InventoryService();

// Init connections
inventoryService.init().then(() => {
    console.log('Inventory Service Initialized');

    // Start Expiry Agent (Background)
    startExpiryAgent(60000); // Check every 60s
}).catch(err => {
    console.error('Failed to init services', err);
});

// Health Check (no auth — for probes)
app.get('/health', (req, res) => {
    res.json({ status: 'ok' });
});

app.use(requireApiToken);

// GET /inventory/:sku?locationId=WEB
app.get('/inventory/:sku', async (req, res) => {
    try {
        const sku = req.params.sku;
        const locationId = (req.query.locationId as string) || 'WEB';
        const window = parseInt((req.query.window as string) || '30', 10);

        const result = await inventoryService.getUnifiedPosition(sku, locationId, window);
        res.json(result);
    } catch (e: unknown) {
        logAndRespond(res, e, 500);
    }
});

// POST /inventory/query (Batch)
app.post('/inventory/query', async (req, res) => {
    try {
        const { skus, locationId, window } = req.body;
        if (!skus || !Array.isArray(skus)) {
            return res.status(400).json({ error: 'Missing skus array' });
        }
        if (skus.length > MAX_BATCH_SKUS) {
            return res.status(400).json({ error: `At most ${MAX_BATCH_SKUS} SKUs per request` });
        }

        const result = await inventoryService.getUnifiedPositionBatch(skus, locationId || 'WEB', window ?? 30);
        res.json(result);
    } catch (e: unknown) {
        logAndRespond(res, e, 500);
    }
});

// POST /reservations
app.post('/reservations', async (req, res) => {
    try {
        const { orderId, sku, qty, locationId, type, ttlMinutes, inventoryType } = req.body;
        if (!orderId || !sku || !qty) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        const result = await inventoryService.createReservation(
            orderId,
            sku,
            qty,
            locationId || 'WEB',
            type || 'SOFT',
            ttlMinutes != null ? parseInt(String(ttlMinutes), 10) : 15,
            inventoryType || 'ON_HAND'
        );
        res.json(result);
    } catch (e: unknown) {
        logAndRespond(res, e, 400);
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
    } catch (e: unknown) {
        logAndRespond(res, e, 400);
    }
});

// POST /allocations
app.post('/allocations', async (req, res) => {
    try {
        const { orderId, sku, qty, locationId } = req.body;
        if (!orderId || !sku || !qty) {
            return res.status(400).json({ error: 'Missing required fields' });
        }
        const result = await inventoryService.createAllocation(orderId, sku, qty, locationId || 'WEB');
        res.json(result);
    } catch (e: unknown) {
        logAndRespond(res, e, 400);
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
    } catch (e: unknown) {
        logAndRespond(res, e, 400);
    }
});

// POST /asn - Create Inbound Shipment
app.post('/asn', async (req, res) => {
    try {
        const asnData = req.body;
        if (!asnData.asnId || !asnData.poId || !asnData.items) {
            return res.status(400).json({ error: 'Missing required fields (asnId, poId, items)' });
        }
        const result = await inventoryService.createInboundShipment(asnData);
        res.json(result);
    } catch (e: unknown) {
        logAndRespond(res, e, 500);
    }
});

// PUT /asn/:id - Update Inbound Shipment
app.put('/asn/:id', async (req, res) => {
    try {
        const asnId = req.params.id;
        const updates = req.body;
        const result = await inventoryService.updateInboundShipment(asnId, updates);
        res.json(result);
    } catch (e: unknown) {
        logAndRespond(res, e, 500);
    }
});

const PORT = process.env.PORT || 3000;
app.listen(Number(PORT), '0.0.0.0', () => {
    console.log(`Unified Inventory Service running on port ${PORT}`);
});
