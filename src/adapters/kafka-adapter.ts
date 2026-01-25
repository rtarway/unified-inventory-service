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
}
