import { Kafka, Producer, Consumer } from 'kafkajs';

export class KafkaAdapter {
    private kafka: Kafka;
    private producer: Producer;
    private consumer: Consumer;
    private isConnected = false;

    constructor(clientId: string = 'unified-inventory-service', brokers: string[] = ['localhost:9092']) {
        this.kafka = new Kafka({ clientId, brokers });
        this.producer = this.kafka.producer();
        this.consumer = this.kafka.consumer({ groupId: `${clientId}-group` });
    }

    async connect(): Promise<void> {
        if (!this.isConnected) {
            await this.producer.connect();
            await this.consumer.connect();
            this.isConnected = true;
            console.log('Kafka Producer & Consumer connected');
        }
    }

    async disconnect(): Promise<void> {
        if (this.isConnected) {
            await this.producer.disconnect();
            await this.consumer.disconnect();
            this.isConnected = false;
        }
    }

    async publishEvent(topic: string, eventType: string, payload: any): Promise<void> {
        if (!this.isConnected) await this.connect();
        try {
            console.log(`[KafkaAdapter] Sending event ${eventType} to topic ${topic}...`);
            await this.producer.send({
                topic,
                messages: [{
                    key: payload.sku || payload.id || 'unknown',
                    value: JSON.stringify({ eventType, timestamp: new Date().toISOString(), data: payload })
                }]
            });
            console.log(`Published ${eventType} to ${topic}`);
        } catch (e) {
            console.error(`Failed to publish event to ${topic}`, e);
        }
    }

    async publish(topic: string, key: string, message: any): Promise<void> {
        if (!this.isConnected) await this.connect();
        try {
            console.log(`[KafkaAdapter] Sending RAW event to ${topic} Key=${key}`);
            await this.producer.send({
                topic,
                messages: [{ key, value: JSON.stringify(message) }]
            });
        } catch (e) {
            console.error(`Failed to publish raw event to ${topic}`, e);
        }
    }

    async subscribe(topic: string, handler: (msg: any) => Promise<void>): Promise<void> {
        if (!this.isConnected) await this.connect();
        await this.consumer.subscribe({ topic, fromBeginning: false });
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value?.toString();
                if (value) {
                    try {
                        const payload = JSON.parse(value);
                        console.log(`[KafkaAdapter] Received message on ${topic}`);
                        await handler(payload);
                    } catch (e) {
                        console.error(`[KafkaAdapter] Error processing message on ${topic}`, e);
                    }
                }
            }
        });
        console.log(`[KafkaAdapter] Subscribed to ${topic}`);
    }
}
