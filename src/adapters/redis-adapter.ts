import { createClient } from 'redis';

export class RedisAdapter {
    private client;
    private isConnected: boolean = false;

    constructor(url: string = 'redis://localhost:6379') {
        this.client = createClient({ url });

        this.client.on('error', (err) => console.error('Redis Client Error', err));
        this.client.on('connect', () => {
            console.log('Redis connected');
            this.isConnected = true;
        });
    }

    async connect(): Promise<void> {
        if (!this.isConnected) {
            await this.client.connect();
        }
    }

    async disconnect(): Promise<void> {
        if (this.isConnected) {
            await this.client.disconnect();
            this.isConnected = false;
        }
    }

    /**
     * getOnHand
     * Fetches current on-hand quantity from the 'inventory:totals' hash.
     * 
     * @param sku The SKU (e.g. "SKU123")
     * @param storeId The Store ID (e.g. "WEB")
     * @returns number The quantity available
     */
    async getOnHand(sku: string, storeId: string = "WEB"): Promise<number> {
        if (!this.isConnected) {
            // Auto connect or throw? For now auto-connect
            await this.connect();
        }

        // Key Format from Inventory Stream Processor:
        // We analysed RetailDataProcessingPipeline.java: getKey = event.getStoreId() + "-" + event.getProductId()
        // RedisSinkService uses this key in 'inventory:totals', BUT prepends "ID:" in the processor topology (likely).
        // Actually, let's verify if the processor ADDS "ID:". 
        // Based on previous debugging, the keys in Redis were like "ID:WEB-SKU..." or similar?
        // Wait, the Plan says "Stream Processor writes ID:WEB-[SKU]". 
        // So we must match that.

        const key = `ID:${storeId}-${sku}`;

        try {
            // HGET inventory:totals <key>
            const val = await this.client.hGet('inventory:totals', key);

            if (val) {
                // The value is stored as a string (BigDecimal representation)
                return parseFloat(val);
            }
            return 0;
        } catch (error) {
            console.error(`Error fetching stock for ${key}`, error);
            // Default to 0 on error for safety, or rethrow? 
            // Better to default to 0 for availability to avoid crashes, but log error.
            return 0;
        }
    }

    /**
     * incrementOnHand
     * Atomic increment/decrement of on-hand inventory.
     * @param qty Positive to add, Negative to subtract.
     */
    async incrementOnHand(sku: string, locationId: string = "WEB", qty: number): Promise<number> {
        if (!this.isConnected) {
            await this.connect();
        }
        const key = `ID:${locationId}-${sku}`;
        // HINCRBYFLOAT returns the new value as string
        const newVal = await this.client.hIncrByFloat('inventory:totals', key, qty);
        return typeof newVal === 'number' ? newVal : parseFloat(newVal);
    }
}
