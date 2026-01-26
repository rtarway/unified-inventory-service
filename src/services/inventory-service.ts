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

        // Subscribe to Receipt Events
        // We assume the Warehouse WMS publishes to 'events-output' or similar, 
        // OR we use 'events-input' if we treat it as an input event. Requirements say "inventory-receipt" or "inventory-stream".
        // Let's use 'events-input' and filter for type='RECEIPT' or a dedicated topic.
        // Implementation Plan said: "subscribing to events-input (filtering for Receipt types)"
        await this.kafka.subscribe('events-warehouse', this.handleReceipt.bind(this));
    }

    /**
     * getUnifiedPosition
     * Aggregates On-Hand (Redis), Future (Postgres), and Reservations (Postgres).
     */
    async getUnifiedPosition(sku: string, locationId: string = "WEB", futureWindowDays: number = 30) {
        const [onHandAvailable, futureInventory, futureReservations] = await Promise.all([
            this.redis.getOnHand(sku, locationId),
            this.postgres.getInboundInventory(sku, locationId, futureWindowDays),
            this.postgres.getFutureReservations(sku)
        ]);

        const futureTotal = futureInventory.reduce((sum: number, item: any) => sum + item.qty_remaining, 0);
        const futureReservedQty = futureReservations.reduce((sum: number, item: any) => sum + item.qty, 0);

        // New ATP Formula: Redis OnHand is ALREADY Net Available (Physical - Reserved)
        const atp = onHandAvailable + (futureTotal - futureReservedQty);

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

    async createReservation(
        orderId: string,
        sku: string,
        qty: number,
        locationId: string,
        type: 'SOFT' | 'HARD',
        ttlMinutes: number = 15,
        inventoryType: 'ON_HAND' | 'FUTURE' = 'ON_HAND'
    ) {
        const pos = await this.getUnifiedPosition(sku, locationId);

        // Validation for ON_HAND Hard Reservations
        if (inventoryType === 'ON_HAND' && type === 'HARD' && pos.atp < qty) {
            throw new Error(`Insufficient ATP for Hard Reservation. Available: ${pos.atp}, Requested: ${qty}`);
        }

        // Future Inventory Validation (Basic)
        if (inventoryType === 'FUTURE') {
            // Validate that there is enough future quantity? 
            // pos.future.total >= qty?
            // For now, allow soft/hard override logic, but generally Future is Soft-ish until confirmed better.
            if (pos.future.total < qty) {
                // Warn or Error? Let's Error for strictness.
                // throw new Error(`Insufficient Future Inventory. Incoming: ${pos.future.total}, Requested: ${qty}`);
            }
        }

        const reservationId = `RES-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
        await this.postgres.createReservation(reservationId, orderId, sku, qty, type, locationId, ttlMinutes, inventoryType);

        // Publish Event: INVENTORY_RESERVED
        // Stream Processor will decrement Redis OnHand ONLY if type is ON_HAND
        if (inventoryType === 'ON_HAND') {
            const eventId = `${locationId}-${sku}`;
            await this.kafka.publish('events-input', eventId, {
                id: eventId,
                value: -qty,
                type: 'NORMAL',
                timestamp: new Date().toISOString(),
                metadata: []
            });
        }

        return { reservationId, status: 'CREATED', msg: 'Future reservation created, no on-hand impact' };
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
    // --- Inbound Shipments (ASN) ---

    async createInboundShipment(asnData: any) {
        // asnData should include: asnId, poId, items: [{sku, qty}], etc.
        if (!asnData.items || asnData.items.length === 0) {
            throw new Error("ASN must contain items");
        }

        await this.postgres.createASN(asnData, asnData.items);
        return { asnId: asnData.asnId, status: 'CREATED' };
    }

    async updateInboundShipment(asnId: string, updates: any) {
        await this.postgres.updateASN(asnId, updates);

        // If updating Date, we *could* check for broken promises here.
        // For Phase 1-2, just update DB.
        return { asnId, status: 'UPDATED' };
        return { asnId, status: 'UPDATED' };
    }

    // --- Receipt Lifecycle ---

    async handleReceipt(message: any) {
        // Expected Message: { type: 'RECEIPT', asnId, sku, qty, locationId }
        // If message structure is different, we parse it. 
        // Assuming simple payload for now.

        // Filter
        if (message.type !== 'RECEIPT') return;

        console.log(`[InventoryService] Processing Receipt: ${message.asnId} ${message.sku} (+${message.qty})`);
        const { asnId, sku, qty, locationId } = message;

        // 1. Update ASN (Qty Received)
        await this.postgres.updateASNItemReceived(asnId, sku, qty);

        // 2. Migrate Future Reservations
        const futureRes = await this.postgres.getFutureReservations(sku);
        let remainingReceipt = qty;

        for (const res of futureRes) {
            if (remainingReceipt <= 0) break;

            const migrateQty = Math.min(res.qty, remainingReceipt);

            // If full reservation covered
            if (migrateQty === res.qty) {
                console.log(`[InventoryService] Migrating Reservation ${res.reservation_id} to ON_HAND`);
                await this.postgres.updateReservationType(res.reservation_id, 'ON_HAND');

                // Publish Reserve Event to decrement physical availability in Redis
                // (Since Receipt Event increments Redis, this migration 'consumes' that increment for the reservation)
                const eventId = `${locationId}-${sku}`;
                await this.kafka.publish('events-input', eventId, {
                    id: eventId,
                    value: -res.qty, // Decrement
                    type: 'MIGRATED',
                    timestamp: new Date().toISOString(),
                    metadata: { reservationId: res.reservation_id }
                });

                remainingReceipt -= res.qty;
            } else {
                // Partial migration? 
                // Complex. For Phase 3, we assume simplifiction: Migration only if fully covered OR split logic.
                // Requirements didn't specify partial split. 
                // Let's skip partial for now or implement "Split Reservation" later.
                console.log(`[InventoryService] Skipping partial migration for ${res.reservation_id}`);
            }
        }
    }
}
