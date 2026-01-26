
import { InventoryService } from '../../src/services/inventory-service';
import { PostgresAdapter } from '../../src/adapters/postgres-adapter';

// Mock dependencies
const mockRedis = {
    connect: jest.fn(),
    getOnHand: jest.fn(),
    incrementOnHand: jest.fn()
};

const mockPostgres = {
    connect: jest.fn(),
    createASN: jest.fn(),
    updateASN: jest.fn(),
    getInboundInventory: jest.fn(),
    createReservation: jest.fn(),
    getFutureReservations: jest.fn(),
    getActiveReservations: jest.fn()
};

const mockKafka = {
    connect: jest.fn(),
    publish: jest.fn(),
    publishEvent: jest.fn()
};

// Mock Constructor Injection
jest.mock('../../src/adapters/redis-adapter', () => {
    return { RedisAdapter: jest.fn().mockImplementation(() => mockRedis) };
});
jest.mock('../../src/adapters/postgres-adapter', () => {
    return { PostgresAdapter: jest.fn().mockImplementation(() => mockPostgres) };
});
jest.mock('../../src/adapters/kafka-adapter', () => {
    return { KafkaAdapter: jest.fn().mockImplementation(() => mockKafka) };
});

describe('InventoryService - Future Inventory', () => {
    let service: InventoryService;

    beforeEach(() => {
        service = new InventoryService();
        jest.clearAllMocks();
    });

    test('createInboundShipment should call postgres.createASN', async () => {
        const asnData = { asnId: 'ASN-1', poId: 'PO-1', items: [{ sku: 'SKU-FUT', qty: 100 }] };

        await service.createInboundShipment(asnData);

        expect(mockPostgres.createASN).toHaveBeenCalledWith(asnData, asnData.items);
    });

    test('getUnifiedPosition should satisfy ATP formula: OnHand + Future - FutureReserved', async () => {
        // Arrange
        mockRedis.getOnHand.mockResolvedValue(0); // 0 OnHand
        mockPostgres.getInboundInventory.mockResolvedValue([
            { sku: 'SKU-FUT', qty_remaining: 50, estimated_arrival: new Date() }
        ]);
        mockPostgres.getFutureReservations.mockResolvedValue([
            { sku: 'SKU-FUT', qty: 10, inventory_type: 'FUTURE', status: 'ACTIVE' }
        ]);

        // Act
        const result = await service.getUnifiedPosition('SKU-FUT', 'WEB');

        // Assert
        // Future Total = 50
        // Future Reserved = 10
        // ATP = 0 + (50 - 10) = 40
        expect(result.future.total).toBe(50);
        expect(result.atp).toBe(40);
    });

    test('createReservation (FUTURE) should succeed without checking On-Hand ATP', async () => {
        // Arrange
        // Even if OnHand is 0, Future is 50. Requesting 5.
        // The service "Basic Validation" might eventually check Future total, but currently it's permissive or checks totals.
        mockRedis.getOnHand.mockResolvedValue(0);
        mockPostgres.getInboundInventory.mockResolvedValue([
            { sku: 'SKU-FUT', qty_remaining: 50 }
        ]);
        mockPostgres.getFutureReservations.mockResolvedValue([]);

        // Act
        const result = await service.createReservation('ORD-FUT-1', 'SKU-FUT', 5, 'WEB', 'SOFT', 60, 'FUTURE');

        // Assert
        expect(mockPostgres.createReservation).toHaveBeenCalledWith(
            expect.stringContaining('RES-'),
            'ORD-FUT-1', 'SKU-FUT', 5, 'SOFT', 'WEB', 60, 'FUTURE'
        );
        // Should NOT publish decrement event to Kafka (Stream Processor handles OnHand, not Future)
        expect(mockKafka.publish).not.toHaveBeenCalled();
        expect(result.status).toBe('CREATED');
    });

    test('createReservation (ON_HAND) should fail if insufficient ATP', async () => {
        // Arrange
        mockRedis.getOnHand.mockResolvedValue(0);
        mockPostgres.getInboundInventory.mockResolvedValue([]);
        mockPostgres.getFutureReservations.mockResolvedValue([]);
        // ATP = 0

        // Act & Assert
        await expect(service.createReservation('ORD-FAIL', 'SKU-1', 5, 'WEB', 'HARD', 15, 'ON_HAND'))
            .rejects.toThrow('Insufficient ATP');
    });
});
