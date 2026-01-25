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
        const [onHandAvailable, futureInventory, futureReservations] = await Promise.all([
            this.redis.getOnHand(sku, locationId),
            this.postgres.getInboundInventory(sku, locationId, futureWindowDays),
            // We assume future reservations are tracked separately if needed, 
            // for now we assume they are included in getting active reservations logic if we extended it.
            // But per new formula: ATP = Redis.OnHandAvailable + (Future.Total)
            // Note: If you have Future Reservations, they subtract from Future Total.
            // Simplified for Phase 1: Future is unreserved.
            Promise.resolve(0)
        ]);

        const futureTotal = futureInventory.reduce((sum: number, item: any) => sum + item.qty_remaining, 0);

        // New ATP Formula: Redis OnHand is ALREADY Net Available (Physical - Reserved)
        const atp = onHandAvailable + (futureTotal - futureReservations);

        return {
            sku,
            locationId,
            onHand: {
                available: onHandAvailable,
                source: 'Redis'
            },
            future: {
                total: futureTotal,
                windowDays: futureWindowDays,
                details: futureInventory
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

    async createReservation(orderId: string, sku: string, qty: number, locationId: string, type: 'SOFT' | 'HARD', ttlMinutes: number = 15) {
        const pos = await this.getUnifiedPosition(sku, locationId);
        if (type === 'HARD' && pos.atp < qty) {
            throw new Error(`Insufficient ATP for Hard Reservation. Available: ${pos.atp}, Requested: ${qty}`);
        }

        const reservationId = `RES-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
        await this.postgres.createReservation(reservationId, orderId, sku, qty, type, locationId, ttlMinutes);

        // Publish Event: INVENTORY_RESERVED
        // Stream Processor will decrement Redis OnHand
        // Publish Event: INVENTORY_RESERVED (Raw for Processor)
        const eventId = `${locationId}-${sku}`;
        await this.kafka.publish('events-input', eventId, {
            id: eventId,
            value: -qty,
            type: 'NORMAL',
            timestamp: new Date().toISOString(),
            metadata: []
        });

        return { reservationId, status: 'CREATED' };
    }

    async shipAllocation(orderId: string, sku: string) {
        const allocation = await this.postgres.getAllocation(orderId, sku);
        if (!allocation) {
            throw new Error(`No active allocation found for Order ${orderId}`);
        }

        await this.postgres.updateAllocationStatus(allocation.allocation_id, 'SHIPPED');

        // Optional: Publish INVENTORY_SHIPPED event for audit trail
        // No impact on OnHand (already deducted)
        const eventId = `${allocation.location_id}-${sku}`;
        await this.kafka.publishEvent('events-input', 'INVENTORY_SHIPPED', {
            allocationId: allocation.allocation_id,
            orderId,
            sku
        });

        return { allocationId: allocation.allocation_id, status: 'SHIPPED' };
    }

    async cancelReservation(reservationId: string) {
        // 1. Mark as CANCELLED in DB
        await this.postgres.updateReservationStatus(reservationId, 'CANCELLED');

        // 2. Publish Event
        await this.kafka.publishEvent('events-input', 'RESERVATION_CANCELLED', {
            reservationId,
            status: 'CANCELLED'
        });

        return { reservationId, status: 'CANCELLED' };
    }

    async createAllocation(orderId: string, sku: string, qty: number, locationId: string) {
        // Logic: Check for EXISTING Reservation
        const reservation = await this.postgres.getReservation(orderId, sku);
        const allocationId = `ALLOC-${Date.now()}`;

        if (reservation && reservation.status === 'ACTIVE') {
            const isSoftNetwork = (reservation.type === 'SOFT' && reservation.location_id !== locationId);
            const isHardMatch = (reservation.location_id === locationId);

            // Mark Reservation Consumed
            await this.postgres.updateReservationStatus(reservation.reservation_id, 'CONSUMED');

            if (isSoftNetwork) {
                // Scenario: Soft Res at Network, Alloc at Store
                // 1. Release Network Hold
                // 1. Release Network Hold (Add back to Network)
                const releaseId = `${reservation.location_id}-${sku}`;
                await this.kafka.publish('events-input', releaseId, {
                    id: releaseId,
                    value: qty,
                    type: 'NORMAL',
                    timestamp: new Date().toISOString(),
                    metadata: []
                });
                // 2. Consume at Store (Alloc Event)
                // 2. Consume at Store (Subtract from Store)
                const allocId = `${locationId}-${sku}`;
                await this.kafka.publish('events-input', allocId, {
                    id: allocId,
                    value: -qty,
                    type: 'NORMAL',
                    timestamp: new Date().toISOString(),
                    metadata: []
                });
            } else {
                // Hard Match or Soft Match at same location
                // Inventory already decremented at Reservation. No Kafka Event needed for Availability decrement.
                // But we might want an event for tracing. For Availability logic: NO EVENT.
                console.log(`Allocation consumed local reservation ${reservation.reservation_id}. No availability change.`);
            }

            await this.postgres.createAllocation(allocationId, orderId, sku, qty, locationId, reservation.reservation_id);
            return { allocationId, status: 'ALLOCATED', strategy: 'CONSUMED_RESERVATION' };

        } else {
            // No Active Reservation (Walk-in or Expired)
            // Create Allocation
            await this.postgres.createAllocation(allocationId, orderId, sku, qty, locationId);

            // Publish Event: INVENTORY_ALLOCATED (Decrements Redis)
            // Publish Event: INVENTORY_ALLOCATED (Decrements Redis)
            const allocId = `${locationId}-${sku}`;
            await this.kafka.publish('events-input', allocId, {
                id: allocId,
                value: -qty,
                type: 'NORMAL',
                timestamp: new Date().toISOString(),
                metadata: []
            });

            return { allocationId, status: 'ALLOCATED', strategy: 'FRESH_ALLOCATION' };
        }
    }

    async expireReservations() {
        // 1. Find Expired
        const expiredList = await this.postgres.getExpiredReservations();
        let processedCount = 0;

        for (const res of expiredList) {
            // 2. Mark EXPIRED
            await this.postgres.updateReservationStatus(res.reservation_id, 'EXPIRED');

            // 3. Publish Event
            // 3. Publish Event
            const releaseId = `${res.location_id}-${res.sku}`;
            await this.kafka.publish('events-input', releaseId, {
                id: releaseId,
                value: res.qty,
                type: 'NORMAL',
                timestamp: new Date().toISOString(),
                metadata: []
            });
            processedCount++;
        }

        if (processedCount > 0) {
            console.log(`Expired ${processedCount} reservations.`);
        }
        return processedCount;
    }

    async createCancellation(orderId: string, sku: string) {
        // 1. Check Allocation
        const allocation = await this.postgres.getAllocation(orderId, sku);
        if (allocation) {
            await this.postgres.updateAllocationStatus(allocation.allocation_id, 'CANCELLED');
            // Release Inventory
            // Release Inventory
            const releaseId = `${allocation.location_id}-${sku}`;
            await this.kafka.publish('events-input', releaseId, {
                id: releaseId,
                value: allocation.qty,
                type: 'NORMAL',
                timestamp: new Date().toISOString(),
                metadata: []
            });
            return { status: 'CANCELLED', source: 'ALLOCATION' };
        }

        // 2. Check Reservation
        const reservation = await this.postgres.getReservation(orderId, sku);
        if (reservation && reservation.status === 'ACTIVE') {
            await this.postgres.updateReservationStatus(reservation.reservation_id, 'CANCELLED');
            // Release Inventory
            // Release Inventory
            const releaseId = `${reservation.location_id}-${sku}`;
            await this.kafka.publish('events-input', releaseId, {
                id: releaseId,
                value: reservation.qty,
                type: 'NORMAL',
                timestamp: new Date().toISOString(),
                metadata: []
            });
            return { status: 'CANCELLED', source: 'RESERVATION' };
        }

        throw new Error(`No active allocation or reservation found for Order ${orderId}`);
    }
}
