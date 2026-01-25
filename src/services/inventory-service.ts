import { RedisAdapter } from '../adapters/redis-adapter';
import { PostgresAdapter } from '../adapters/postgres-adapter';
import { KafkaAdapter } from '../adapters/kafka-adapter';

export class InventoryService {
    private redis: RedisAdapter;
    private postgres: PostgresAdapter;
    private kafka: KafkaAdapter;

    constructor() {
        this.redis = new RedisAdapter(process.env.REDIS_URL);
        this.postgres = new PostgresAdapter(process.env.DATABASE_URL);
        this.kafka = new KafkaAdapter('unified-ui', (process.env.KAFKA_BROKERS || 'localhost:9092').split(','));
    }

    async init() {
        await this.redis.connect();
        await this.postgres.connect();
        await this.kafka.connect();
    }

    /**
     * getUnifiedPosition
     * Aggregates On-Hand (Redis), Future (Postgres), and Reservations (Postgres).
     */
    async getUnifiedPosition(sku: string, locationId: string = "WEB", futureWindowDays: number = 30) {
        const [onHand, futureInventory, reservedQty] = await Promise.all([
            this.redis.getOnHand(sku, locationId),
            this.postgres.getInboundInventory(sku, locationId, futureWindowDays),
            this.postgres.getActiveReservations(sku, locationId)
        ]);

        const futureTotal = futureInventory.reduce((sum: number, item: any) => sum + item.qty_remaining, 0);

        // Simple ATP Calculation
        // ATP = (OnHand + Future) - Reserved
        // Note: This is a simplified view. Real logic might separate ATP_ON_HAND vs ATP_FUTURE.

        const atp = (onHand + futureTotal) - reservedQty;

        return {
            sku,
            locationId,
            onHand: {
                total: onHand,
                source: 'Redis'
            },
            future: {
                total: futureTotal,
                windowDays: futureWindowDays,
                details: futureInventory // List of ASNs
            },
            reservations: {
                total: reservedQty,
            },
            atp
        };
    }

    async getUnifiedPositionBatch(skus: string[], locationId: string = "WEB", futureWindowDays: number = 30) {
        // Parallelize for all SKUs
        // Check Redis MGET support in Adapter? 
        // For now, simple Promise.all iteration. Optimization: Adapter MGET.

        const promises = skus.map(sku => this.getUnifiedPosition(sku, locationId, futureWindowDays));
        return Promise.all(promises);
    }

    async createReservation(orderId: string, sku: string, qty: number, locationId: string, type: 'SOFT' | 'HARD') {
        // 1. Check Availability (Optional enforcing)
        // For now, we allow over-reservation but return warning? 
        // Or we enforce? Let's assume we enforce ATP > 0 for HARD reservations.

        const pos = await this.getUnifiedPosition(sku, locationId);
        if (type === 'HARD' && pos.atp < qty) {
            throw new Error(`Insufficient ATP for Hard Reservation. Available: ${pos.atp}, Requested: ${qty}`);
        }

        const reservationId = `RES-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
        await this.postgres.createReservation(reservationId, orderId, sku, qty, type, locationId);

        // Publish Event for Control Tower
        await this.kafka.publishEvent('inventory.events', 'RESERVATION_CREATED', {
            reservationId,
            orderId,
            sku,
            qty,
            type,
            locationId,
            status: 'CREATED'
        });

        return { reservationId, status: 'CREATED' };
    }

    async cancelReservation(reservationId: string) {
        // 1. Mark as CANCELLED in DB
        await this.postgres.updateReservationStatus(reservationId, 'CANCELLED');

        // 2. Publish Event
        await this.kafka.publishEvent('inventory.events', 'RESERVATION_CANCELLED', {
            reservationId,
            status: 'CANCELLED'
        });

        return { reservationId, status: 'CANCELLED' };
    }

    async allocateInventory(orderId: string, sku: string, qty: number, locationId: string = "WEB") {
        // 1. Ideally, we link allocation to a reservation. 
        // For simplicity, we assume the reservation exists and is active for this order.
        // In a real system, we'd lookup the reservation ID by Order ID.
        // Here, we'll assume we are converting "Reserve" -> "Allocated" for the Order.

        // Check if reservation exists for order?
        // Skipped for simplicity. We will just Decrement OnHand.

        // 2. Decrement OnHand (Atomic)
        const newOnHand = await this.redis.incrementOnHand(sku, locationId, -qty);

        // 3. Publish Event
        await this.kafka.publishEvent('inventory.events', 'INVENTORY_ALLOCATED', {
            orderId,
            sku,
            qty,
            locationId,
            newOnHand,
            status: 'ALLOCATED'
        });

        return { orderId, status: 'ALLOCATED', newOnHand };
    }

    async cancelAllocation(orderId: string, sku: string, qty: number, locationId: string = "WEB") {
        // 1. Revert OnHand
        const newOnHand = await this.redis.incrementOnHand(sku, locationId, qty);

        // 2. Publish Event
        await this.kafka.publishEvent('inventory.events', 'ALLOCATION_CANCELLED', {
            orderId,
            sku,
            qty,
            locationId,
            newOnHand,
            status: 'ALLOCATION_CANCELLED'
        });

        return { orderId, status: 'ALLOCATION_CANCELLED', newOnHand };
    }
}
