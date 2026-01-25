
import { InventoryService } from '../../src/services/inventory-service';
import { PostgresAdapter } from '../../src/adapters/postgres-adapter';
import { KafkaAdapter } from '../../src/adapters/kafka-adapter';
import { RedisAdapter } from '../../src/adapters/redis-adapter';

// Mock dependencies
jest.mock('../../src/adapters/postgres-adapter');
jest.mock('../../src/adapters/kafka-adapter');
jest.mock('../../src/adapters/redis-adapter');

describe('EventDrivenInventory', () => {
    let service: InventoryService;
    let mockPostgres: jest.Mocked<PostgresAdapter>;
    let mockKafka: jest.Mocked<KafkaAdapter>;
    let mockRedis: jest.Mocked<RedisAdapter>;

    beforeEach(() => {
        service = new InventoryService();
        // Access mocked instances
        mockPostgres = (service as any).postgres;
        mockKafka = (service as any).kafka;
        mockRedis = (service as any).redis;

        // Reset mocks
        jest.clearAllMocks();
    });

    test('createReservation (HARD) should check ATP and publish INVENTORY_RESERVED (Raw)', async () => {
        // Setup ATP
        mockRedis.getOnHand.mockResolvedValue(100);
        mockPostgres.getInboundInventory.mockResolvedValue([]);
        // mockPostgres.getActiveReservations assumed unused for ATP now or mocked

        // Execute
        await service.createReservation('ORD-1', 'SKU-A', 5, 'STORE-1', 'HARD');

        // Verify
        expect(mockPostgres.createReservation).toHaveBeenCalledWith(
            expect.stringContaining('RES-'),
            'ORD-1', 'SKU-A', 5, 'HARD', 'STORE-1', 15
        );
        // Note: createReservation uses kafka.publish (raw)
        expect(mockKafka.publish).toHaveBeenCalledWith(
            'events-input',
            expect.stringContaining('STORE-1-SKU-A'),
            expect.objectContaining({ type: 'NORMAL', value: -5 })
        );
    });

    test('createAllocation with NO Reservation should publish INVENTORY_ALLOCATED (Raw)', async () => {
        mockPostgres.getReservation.mockResolvedValue(null); // No res

        await service.createAllocation('ORD-2', 'SKU-A', 2, 'STORE-1');

        expect(mockPostgres.createAllocation).toHaveBeenCalled();
        expect(mockKafka.publish).toHaveBeenCalledWith(
            'events-input',
            expect.stringContaining('STORE-1-SKU-A'),
            expect.objectContaining({ type: 'NORMAL', value: -2 })
        );
    });

    test('createAllocation with SOFT RESERVATION at NETWORK (Mismatch) should publish RELEASE + ALLOCATE', async () => {
        mockPostgres.getReservation.mockResolvedValue({
            reservation_id: 'RES-SOFT',
            status: 'ACTIVE',
            type: 'SOFT',
            location_id: 'NETWORK' // Different
        });

        await service.createAllocation('ORD-3', 'SKU-B', 1, 'STORE-5'); // Store

        // 1. Consume Res
        expect(mockPostgres.updateReservationStatus).toHaveBeenCalledWith('RES-SOFT', 'CONSUMED');

        // 2. Events (Raw Publish)
        // Release Network
        expect(mockKafka.publish).toHaveBeenCalledWith(
            'events-input',
            expect.stringContaining('NETWORK-SKU-B'),
            expect.objectContaining({ value: 1 })
        );
        // Alloc Store
        expect(mockKafka.publish).toHaveBeenCalledWith(
            'events-input',
            expect.stringContaining('STORE-5-SKU-B'),
            expect.objectContaining({ value: -1 })
        );
    });

    test('createAllocation with HARD RESERVATION (Match) should NOT publish Events', async () => {
        mockPostgres.getReservation.mockResolvedValue({
            reservation_id: 'RES-HARD',
            status: 'ACTIVE',
            type: 'HARD',
            location_id: 'STORE-1'
        });

        await service.createAllocation('ORD-4', 'SKU-C', 1, 'STORE-1'); // Match

        expect(mockPostgres.updateReservationStatus).toHaveBeenCalledWith('RES-HARD', 'CONSUMED');
        expect(mockKafka.publish).not.toHaveBeenCalled();
    });

    test('expireReservations should find expired and release them', async () => {
        mockPostgres.getExpiredReservations.mockResolvedValue([
            { reservation_id: 'R1', sku: 'S1', qty: 10, location_id: 'L1', expires_at: 'PAST' }
        ]);

        const count = await service.expireReservations();

        expect(count).toBe(1);
        expect(mockPostgres.updateReservationStatus).toHaveBeenCalledWith('R1', 'EXPIRED');
        expect(mockKafka.publish).toHaveBeenCalledWith(
            'events-input',
            expect.stringContaining('L1-S1'),
            expect.objectContaining({ value: 10 })
        );
    });
});
