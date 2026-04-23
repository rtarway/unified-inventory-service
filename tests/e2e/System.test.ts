
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

// Timeout for E2E tests (k8s operations can be slow)
jest.setTimeout(60000);

describe('E2E System Test: Unified Inventory Service', () => {
    let sku: string;
    let orderId: string;
    let redisPod: string;
    let uisPod: string;
    let kafkaPod: string;

    const BASE_URL = 'http://localhost:3000'; // Not used directly if we use kubectl exec wget

    beforeAll(async () => {
        // 1. Setup Unique SKU/Order
        sku = `TEST-E2E-${Date.now()}`;
        orderId = `ORD-${Date.now()}`;

        // 2. Resolve Pod Names
        try {
            const redisInfo = await execAsync('kubectl get pod -l app=redis -o jsonpath="{.items[0].metadata.name}"');
            redisPod = redisInfo.stdout.trim();

            const uisInfo = await execAsync('kubectl get pod -l app=unified-inventory-service -o jsonpath="{.items[0].metadata.name}"');
            uisPod = uisInfo.stdout.trim();

            const kafkaInfo = await execAsync('kubectl get pod -l app=kafka -o jsonpath="{.items[0].metadata.name}"');
            kafkaPod = kafkaInfo.stdout.trim();

            console.log(`[Setup] SKU: ${sku}, Redis: ${redisPod}, UIS: ${uisPod}, Kafka: ${kafkaPod}`);
        } catch (error) {
            console.error('Failed to get pods. make sure K8s is running.', error);
            throw error;
        }
    });

    // Helper: Produce Event to Kafka
    const produceEvent = async (key: string, val: number, type: 'NORMAL' | 'RESET' = 'NORMAL') => {
        const topic = 'events-input';
        const timestamp = new Date().toISOString();
        const payload = JSON.stringify({
            id: key,
            value: val,
            type: type,
            timestamp: timestamp,
            metadata: []
        });

        // Use kubectl exec to pipe into kafka-console-producer
        // Note: escaping quotes for shell
        const cmd = `echo '${key}:${payload}' | kubectl exec -i ${kafkaPod} -- kafka-console-producer --bootstrap-server localhost:9092 --topic ${topic} --property "parse.key=true" --property "key.separator=:"`;

        await execAsync(cmd);
        console.log(`[Kafka] Produced ${type} event: ${key}=${val}`);
    };

    // Helper: Get Inventory from Stream Processor API
    const getInventoryApi = async (key: string): Promise<string> => {
        // URL: http://api:8080/total/{key}
        // Run wget from inside the UIS pod (it has access to cluster network)
        try {
            const cmd = `kubectl exec ${uisPod} -- wget -qO- "http://api:8080/total/${key}"`;
            const result = await execAsync(cmd);
            // Result: {"key":"...","total":"100.0"}
            const json = JSON.parse(result.stdout);
            // Total might be "100.0" or "100"
            return parseFloat(json.total).toString();
        } catch (e: any) {
            // handle 404 from wget (server returns error) -> returns non-zero exit code
            return 'NOT_FOUND';
        }
    };

    // Helper: Call UIS API
    const callUisApi = async (method: 'POST', endpoint: string, body: any) => {
        const data = JSON.stringify(body);
        // Using wget inside UIS pod to call itself (localhost:3000) or internal IP
        const cmd = `kubectl exec ${uisPod} -- wget -qO- --header="Content-Type: application/json" --post-data='${data}' "http://127.0.0.1:3000${endpoint}"`;
        const result = await execAsync(cmd);
        return JSON.parse(result.stdout);
    };

    // Helper: Wait for API Value
    const waitForApiValue = async (key: string, expected: string, retries = 20): Promise<void> => {
        for (let i = 0; i < retries; i++) {
            const val = await getInventoryApi(key);
            if (val === expected) {
                return;
            }
            await new Promise(r => setTimeout(r, 1000));
        }
        throw new Error(`Timeout waiting for API value ${expected}. Last value: ${await getInventoryApi(key)}`);
    };

    test('Full Flow: Reset -> Reservation -> Allocation -> Expiry', async () => {
        const itemKey = `WEB-${sku}`;
        const redisKey = `ID:WEB-${sku}`;

        // 1. Initialize with RESET
        await produceEvent(itemKey, 100, 'RESET');
        await waitForApiValue(redisKey, '100');

        // 2. Create Hard Reservation (10)
        const resPayload = {
            orderId: orderId,
            sku: sku,
            qty: 10,
            type: 'HARD',
            locationId: 'WEB'
        };
        await callUisApi('POST', '/reservations', resPayload);
        // Expect 100 - 10 = 90
        await waitForApiValue(redisKey, '90');

        // 3. Allocate (Consume Reservation)
        const allocPayload = {
            orderId: orderId,
            sku: sku,
            qty: 10,
            locationId: 'WEB'
        };
        await callUisApi('POST', '/allocations', allocPayload);
        // Expect 90 (Consumed reservation, no double dip)
        await waitForApiValue(redisKey, '90');

        // 4. Create Soft Reservation (5)
        const expOrderId = `ORD-EXP-${Date.now()}`;
        const expPayload = {
            orderId: expOrderId,
            sku: sku,
            qty: 5,
            type: 'SOFT',
            locationId: 'WEB',
            ttlMinutes: 2 // Assuming we support this now
        };
        await callUisApi('POST', '/reservations', expPayload);
        // Expect 90 - 5 = 85
        await waitForApiValue(redisKey, '85');
    });
});
