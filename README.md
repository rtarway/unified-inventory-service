# Unified Inventory Service

## Business Problem
In modern supply chains, inventory data is often fragmented across multiple systems:
- **On-Hand Inventory**: Live stock levels in warehouses/stores.
- **Future Inventory**: inbound shipments (ASNs) arriving soon.
- **Reservations**: Stock allocated to customer orders but not yet fulfilled.

This fragmentation leads to:
1.  **Overselling**: Selling stock that is already reserved.
2.  **Underselling**: Failing to sell stock that is arriving tomorrow (Future Inventory).
3.  **Poor Customer Experience**: Inaccurate promise dates.

## Solution
The **Unified Inventory Service** provides a single, aggregated view of inventory "Position". It calculates **Available to Promise (ATP)** by synthesizing data from three sources in real-time:

`ATP = (On-Hand + Future Inventory) - Reservable Quantity`

Key capabilities:
- **Unified Query**: Single API to get the complete inventory picture for a SKU.
- **Future Visibility**: Configurable "look-ahead" window (e.g., 30 days) to include inbound stock.
- **Reservation Management**: Hard and soft reservations to safely allocate stock.

## Architecture

The service follows a layered architecture:

- **API Layer**: Node.js/Express REST endpoints (`/inventory/:sku`, `/reservations`).
- **Service Layer**: Business logic for ATP calculation and aggregation.
- **Adapter Layer**:
    - **Redis Adapter**: Connects to Redis for ultra-fast "On-Hand" reads (populated by stream processors).
    - **Postgres Adapter**: Connects to Postgres for "Future Inventory" (ASNs) and "Reservations".
    - **Kafka Adapter**: Publishes reservation events (`RESERVATION_CREATED`) to the control tower.

### Tech Stack
- **Runtime**: Node.js & TypeScript
- **Database**: PostgreSQL (ASNs, Reservations)
- **Cache**: Redis (On-Hand Counters)
- **Messaging**: Kafka (Events)
- **Infrastructure**: Docker / Kubernetes (Rancher Desktop)

## Setup & Configuration

### Prerequisites
- Node.js (v18+)
- Local infrastructure running (Redis, Postgres, Kafka) via Docker/Rancher.

### Environment Variables
Create a `.env` file in the root directory:

```ini
# Infrastructure running on Rancher Desktop VM IP
REDIS_URL=redis://192.168.64.2:6379
DATABASE_URL=postgresql://admin:password@192.168.64.2:5432/inventory_future
KAFKA_BROKERS=192.168.64.2:29092
PORT=3001
```

> **Note**: If running on Rancher Desktop/Colima, verify the bridge IP using `ifconfig` (look for `bridge100` or similar).

## Build & Run

### 1. Install Dependencies
```bash
npm install
```

### 2. Clean & Build
To remove old artifacts and compile the TypeScript code:
```bash
npm run clean-build
```

### 3. Run in Development Mode
Starts the service with hot-reloading (nodemon):
```bash
npm run dev
```

### 4. Run Production Build
```bash
npm start
```

## Testing

Run the unit and integration tests using Jest:

```bash
npm test
```

## API Usage Examples

### Get Inventory Position
```http
GET /inventory/SKU123?locationId=WEB&window=30
```

**Response**:
```json
{
  "sku": "SKU123",
  "atp": 150,
  "onHand": { "total": 100 },
  "future": { "total": 60, "windowDays": 30 },
  "reservations": { "total": 10 }
}
```

### Create Reservation
```http
POST /reservations
Content-Type: application/json

{
  "orderId": "ORD-555",
  "sku": "SKU123",
  "qty": 5,
  "type": "HARD"
}
```
