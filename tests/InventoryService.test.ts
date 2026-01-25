import { InventoryService } from '../src/services/inventory-service';

// Mock Adapters
const mockRedis = {
    connect: jest.fn(),
    getOnHand: jest.fn(),
    incrementOnHand: jest.fn()
};

const mockPostgres = {
    connect: jest.fn(),
    getInboundInventory: jest.fn(),
    getActiveReservations: jest.fn(),
    createReservation: jest.fn(),
    updateReservationStatus: jest.fn()
};

const mockKafka = {
    connect: jest.fn(),
    publishEvent: jest.fn()
};

// Mock Constructor Injection
jest.mock('../src/adapters/redis-adapter', () => {
    return { RedisAdapter: jest.fn().mockImplementation(() => mockRedis) };
});
jest.mock('../src/adapters/postgres-adapter', () => {
    return { PostgresAdapter: jest.fn().mockImplementation(() => mockPostgres) };
});
jest.mock('../src/adapters/kafka-adapter', () => {
    return { KafkaAdapter: jest.fn().mockImplementation(() => mockKafka) };
});

describe('InventoryService', () => {
    let service: InventoryService;

    beforeEach(() => {
        service = new InventoryService();
        jest.clearAllMocks();
    });

    test('getUnifiedPosition sums OnHand and Future correctly', async () => {
        // Arrange
        mockRedis.getOnHand.mockResolvedValue(10); // 10 On Hand
        mockPostgres.getInboundInventory.mockResolvedValue([
            { sku: 'SKU123', qty_remaining: 5, estimated_arrival: new Date() } // 5 Future
        ]);
        mockPostgres.getActiveReservations.mockResolvedValue(2); // 2 Reserved

        // Act
        const result = await service.getUnifiedPosition('SKU123', 'WEB');

        // Assert
        expect(result.onHand.total).toBe(10);
        expect(result.future.total).toBe(5);
        expect(result.reservations.total).toBe(2);

        // ATP = (10 + 5) - 2 = 13
        expect(result.atp).toBe(13);
    });

    test('createReservation publishes event', async () => {
        // Arrange
        mockPostgres.createReservation.mockResolvedValue(undefined);
        mockRedis.getOnHand.mockResolvedValue(100);
        mockPostgres.getInboundInventory.mockResolvedValue([]);
        mockPostgres.getActiveReservations.mockResolvedValue(0);

        // Act
        await service.createReservation('ORD-001', 'SKU123', 5, 'WEB', 'SOFT');

        // Assert
        expect(mockPostgres.createReservation).toHaveBeenCalled();
        expect(mockKafka.publishEvent).toHaveBeenCalledWith(
            'inventory.events',
            'RESERVATION_CREATED',
            expect.objectContaining({ orderId: 'ORD-001', type: 'SOFT' })
        );
    });

    test('createReservation throws error for HARD reservation if insufficient ATP', async () => {
        // Arrange
        mockRedis.getOnHand.mockResolvedValue(5);
        mockPostgres.getInboundInventory.mockResolvedValue([]);
        mockPostgres.getActiveReservations.mockResolvedValue(3);
        // ATP = 5 - 3 = 2. Requested 5. Should fail.

        // Act & Assert
        await expect(service.createReservation('ORD-HARD', 'SKU123', 5, 'WEB', 'HARD'))
            .rejects.toThrow('Insufficient ATP');

        expect(mockPostgres.createReservation).not.toHaveBeenCalled();
    });

    test('cancelReservation updates status and publishes event', async () => {
        // Arrange
        mockPostgres.updateReservationStatus = jest.fn().mockResolvedValue(undefined);

        // Act
        await service.cancelReservation('RES-123');

        // Assert
        expect(mockPostgres.updateReservationStatus).toHaveBeenCalledWith('RES-123', 'CANCELLED');
        expect(mockKafka.publishEvent).toHaveBeenCalledWith(
            'inventory.events',
            'RESERVATION_CANCELLED',
            expect.objectContaining({ reservationId: 'RES-123' })
        );
    });

    test('allocateInventory decrements OnHand and publishes event', async () => {
        // Arrange
        mockRedis.incrementOnHand.mockResolvedValue(95); // Assuming started at 100, allocating 5

        // Act
        await service.allocateInventory('ORD-ALLOC', 'SKU123', 5, 'WEB');

        // Assert
        expect(mockRedis.incrementOnHand).toHaveBeenCalledWith('SKU123', 'WEB', -5);
        expect(mockKafka.publishEvent).toHaveBeenCalledWith(
            'inventory.events',
            'INVENTORY_ALLOCATED',
            expect.objectContaining({ orderId: 'ORD-ALLOC', newOnHand: 95 })
        );
    });

    test('cancelAllocation increments OnHand and publishes event', async () => {
        // Arrange
        mockRedis.incrementOnHand.mockResolvedValue(100); // Back to 100

        // Act
        await service.cancelAllocation('ORD-ALLOC', 'SKU123', 5, 'WEB');

        // Assert
        expect(mockRedis.incrementOnHand).toHaveBeenCalledWith('SKU123', 'WEB', 5);
        expect(mockKafka.publishEvent).toHaveBeenCalledWith(
            'inventory.events',
            'ALLOCATION_CANCELLED',
            expect.objectContaining({ orderId: 'ORD-ALLOC', newOnHand: 100 })
        );
    });
});
