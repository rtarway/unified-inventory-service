# Unified Inventory Service

> **Status**: Development | **Version**: 1.0.0

A high-performance, event-driven inventory management microservice designed to provide a real-time "Available to Promise" (ATP) view by aggregating on-hand stock, inbound shipments, and active reservations.

## 📖 Business Problem
In modern distributed supply chains, inventory data is often fragmented across multiple systems (WMS, ERP, POS). This leads to:
1.  **Overselling**: Accepting orders for stock that is already reserved or damaged.
2.  **Underselling**: Rejecting orders because inbound stock (arriving tomorrow) isn't visible.
3.  **Race Conditions**: High-concurrency flash sales causing negative inventory.
4.  **Stale Data**: Polling-based architectures result in lag between physical and digital inventory.

## 💡 Solution
The **Unified Inventory Service** acts as the single source of truth for inventory availability. It uses an **Event Sourcing** pattern combined with **CQRS-lite** concepts:

- **Writes (Commands)**: All inventory changes (ASNs, Adjustments, Reservations) are processed as asynchronous events or direct commands.
- **Reads (Queries)**: A highly optimized "Unified View" is calculated on-the-fly using:
    - **On-Hand**: Redis (populated by Stream Processor).
    - **Future**: Postgres (ASNs/Purchase Orders).
    - **Reservations**: Postgres (Hard/Soft allocations).

### Key Features
- **Real-Time ATP Calculation**: `ATP = (On-Hand + Future) - Active Reservations`
- **Configurable Expiry**: Soft reservations aut-expire (TTL) to release stock if checkout is abandoned.
- **Future Visibility**: Configurable look-ahead window (e.g., 30 days) for inbound stock.
- **Shipping Logic**: Full lifecycle support from Reservation -> Allocation -> Shipment.

## 🏗 Architecture

### High-Level Data Flow

```mermaid
graph TD
    Client[Client / Checkout] -->|1. Reservation| UIS[Unified Inventory Service]
    UIS -->|2. Persist Res| DB[(Postgres)]
    UIS -->|3. Publish Event| Kafka{Kafka}
    
    ERP[ERP / WMS] -->|4. Stock Updates| Kafka
    
    Kafka -->|5. Consume Events| Processor[Inventory Stream Processor]
    Processor -->|6. Update On-Hand| Redis[(Redis Cache)]
    
    UIS -->|7. Query On-Hand| Redis
    UIS -->|8. Query Future/Res| DB
    UIS -->|9. Return ATP| Client
```

### Component Details
- **Unified Inventory Service (`this repo`)**:
    - **Role**: Read/Write API for Reservations and ATP.
    - **Tech**: Node.js, Express, TypeScript.
    - **Data**: Reads "Hot" data from Redis, "Cold/Complex" data from Postgres.
- **Inventory Stream Processor**:
    - **Role**: Backend worker that consumes Kafka events (e.g., `StockReceived`, `OrderShipped`) and maintains the Redis "On-Hand" counters.
    - **Tech**: Java/Spring Boot (Separate Repo).

## 🚀 Setup & Configuration

### Prerequisites
- Docker & Kubernetes (Rancher Desktop / Minikube)
- Node.js v18+
- `kubectl` configured

### 1. Infrastructure Setup
The project relies on Redis, Postgres, and Kafka running in Kubernetes.

```bash
# Start Minikube/Rancher
kubectl apply -f k8s/config.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/kafka.yaml
```

### 2. Environment Variables
Create `.env` for local development (if running outside K8s):

```ini
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/inventory_db
KAFKA_BROKERS=localhost:9092
PORT=3000
```

### 3. Installation
```bash
npm install
npm run build
```

## 🧪 Testing

The project uses **Jest** for testing, organized into Unit and End-to-End (System) suites.

### Unit Tests
Run isolated logic tests (mocked DB/Kafka):
```bash
npm test
```
*Locations: `tests/unit/*.test.ts`*

### End-to-End (System) Tests
Runs a full scenario against the running Kubernetes cluster. verified via the **Stream Processor API** and **Kafka**.
> **Requirement**: Ensure all pods (UIS, Redis, Kafka, Processor) are running in K8s.

```bash
npm run test:e2e
```
*Location: `tests/e2e/System.test.ts`*

### Run All Tests
```bash
npm run test:all
```

## 📡 API Reference

### 1. Get Inventory Position
Calculates ATP based on real-time data.

**GET** `/inventory/:sku?locationId=WEB&window=30`

**Response:**
```json
{
  "sku": "SKU-123",
  "atp": 150,
  "onHand": { "available": 100, "source": "Redis" },
  "future": { "total": 60, "windowDays": 30 },
  "reservations": { "total": 10 }
}
```

### 2. Create Reservation
Reserves stock for a customer order. Supports `TTL` for expiry.

**POST** `/reservations`
```json
{
  "orderId": "ORD-555",
  "sku": "SKU-123",
  "qty": 5,
  "type": "SOFT",
  "ttlMinutes": 15
}
```

### 3. Ship Allocation
Marks an order as shipped, finalizing inventory deduction.

**POST** `/shipments`
```json
{
  "orderId": "ORD-555",
  "sku": "SKU-123"
}
```
