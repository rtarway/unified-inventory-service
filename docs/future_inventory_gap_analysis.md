# Future Inventory Gap Analysis

## 1. Existing State
The current `InventoryService` implementation is partially aware of Future Inventory but treats it as a read-only aggregate for ATP calculation.

### 1.1 Capabilities
- **Read**: `getUnifiedPosition` fetches inbound inventory from `asn_items` via `PostgresAdapter`.
- **ATP**: Calculates ATP as `Redis.OnHand + Future.Total - Future.Reservations` (simplified).
- **Reservations**: Created in Postgres (`reservations` table), but treated as generic.

### 1.2 Limitations
- **Read-Only ASNs**: The system reads from `asn_items` but has no API or logic to **create** or **update** ASNs (`PostgresAdapter` lacks write methods for ASNs).
- **Generic Reservations**: The `reservations` table and logic do not distinguish between `ON_HAND` and `FUTURE` data sources.
- **No Event Handling**: No logic exists to listen for "Shipment Receipt" events to trigger the "Future -> OnHand" lifecycle.

## 2. Gaps & Missing Features

| Component | Missing Feature | Description |
|-----------|----------------|-------------|
| **Unified Inventory Service** <br> (Data Layer) | **ASN Write Ops** | `PostgresAdapter` needs methods to `createASN`, `updateASN`, `cancelASN`. |
| **Unified Inventory Service** <br> (Data Layer) | **Reservation Schema** | `reservations` table needs an `inventory_type` column (Enum: `ON_HAND`, `FUTURE`). |
| **Unified Inventory Service** <br> (Service Layer) | **ASN Management API** | New endpoints needed: `POST /asn`, `PUT /asn/:id`, `DELETE /asn/:id`. |
| **Unified Inventory Service** <br> (Service Layer) | **Reservation Logic** | `createReservation` must accept `inventoryType`. <br> - If `FUTURE`: Validate `expiry > asn.expectedArrival`. <br> - If `ON_HAND`: Validate against Redis OnHand. |
| **Unified Inventory Service** <br> (Msg Handling) | **Receipt Processing** | Logic to consume `inventory-receipt` event (from Warehouse). <br> -> Clear ASN <br> -> Migrate Future Reservations to OnHand. |
| **External OMS / Checkout** | **Reservation Call** | *Constraint only*: The calling system must be updated to pass `inventoryType` when calling `POST /reservations`. |

## 3. Database Schema Changes

### 3.1 New Tables / Columns
**Table: `reservations`**
- [ADD] `inventory_type` VARCHAR(20) DEFAULT 'ON_HAND' CHECK (inventory_type IN ('ON_HAND', 'FUTURE'))

**Table: `asns` (Assuming existence, create if missing)**
- Needs standard CRUD structure: `asn_id`, `status` (CREATED, IN_TRANSIT, RECEIVED, CANCELLED), `expected_arrival`, `location_id`.

## 4. API Extensions

### 4.1 New Endpoints
- `POST /asn` - Create inbound shipment.
- `PUT /asn/:id` - Update details (triggering the "Update Logic").
- `POST /asn/:id/receipt` - Manually trigger receipt (or via Kafka).

### 4.2 Modified Endpoints
- `POST /reservations` - Add optional body param `inventoryType: 'ON_HAND' | 'FUTURE'`.
