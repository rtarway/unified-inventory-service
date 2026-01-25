import { Kafka, Producer } from 'kafkajs';

export class KafkaAdapter {
    private kafka: Kafka;
    private producer: Producer;
    private isConnected: boolean = false;

    constructor(clientId: string = 'unified-inventory-service', brokers: string[] = ['localhost:9092']) {
        this.kafka = new Kafka({
            clientId,
            brokers
        });
        this.producer = this.kafka.producer();
    }

    async connect(): Promise<void> {
        if (!this.isConnected) {
            await this.producer.connect();
            this.isConnected = true;
            console.log('Kafka Producer connected');
        }
    }

    async disconnect(): Promise<void> {
        if (this.isConnected) {
            await this.producer.disconnect();
            this.isConnected = false;
        }
    }

    async publishEvent(topic: string, eventType: string, payload: any): Promise<void> {
        if (!this.isConnected) {
            await this.connect();
        }

        try {
            console.log(`[KafkaAdapter] Sending event ${eventType} to topic ${topic}...`);
            await this.producer.send({
                topic,
                messages: [
                    {
                        key: payload.sku || payload.id || 'unknown',
                        value: JSON.stringify({
                            eventType,
                            timestamp: new Date().toISOString(),
                            data: payload
                        })
                    }
                ]
            });
            console.log(`Published ${eventType} to ${topic}`);
        } catch (error) {
            console.error(`Failed to publish event to ${topic}`, error);
        }
    }

    async publish(topic: string, key: string, message: any): Promise<void> {
        if (!this.isConnected) {
            await this.connect();
        }

        try {
            console.log(`[KafkaAdapter] Sending RAW event to ${topic} Key=${key}`);
            await this.producer.send({
                topic,
                messages: [
                    {
                        key,
                        value: JSON.stringify(message)
                    }
                ]
            });
        } catch (error) {
            console.error(`Failed to publish raw event to ${topic}`, error);
        }
    }
}
