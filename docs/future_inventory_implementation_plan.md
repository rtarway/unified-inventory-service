# Future Inventory Implementation Plan

## 1. Architecture Overview

### 1.1 High-Level Data Flow

```mermaid
graph TD
    Vendor[Vendor / EDI] -->|ASN Create/Update| ApiService[Unified Inventory Service]
    ApiService -->|Write ASN| DB[(Postgres)]
    
    Promising[Promising Engine] -->|1. Query Options| ApiService
    ApiService -->|Return Availability (Incl. Future)| Promising
    
    Customer[Checkout / OMS] -->|2. Get Options| Promising
    Promising -->|Return Best Dates| Customer
    
    Customer -->|3. Confirm Order & Reserve| ApiService
    ApiService -->|Write Reservation (Type=FUTURE/ON_HAND)| DB
    
    Warehouse[Warehouse System] -->|Receipt Event| Kafka{Kafka: events-input}
    
    Kafka -->|Updates OnHand| StreamProcessor[Inventory Stream Processor]
    StreamProcessor -->|Write OnHand| Redis[(Redis)]
    
    Kafka -->|Consume Receipt| ApiService
    ApiService -->|1. Clear ASN| DB
    ApiService -->|2. Migrate Reservations| DB
```

### 1.2 Responsibilities

#### Unified Inventory Service
- **ASN Management**: System of Record for Inbound Shipments (ASNs).
- **Reservation Execution**: Validates and persists reservations. Handles `inventory_type` logic.
- **Lifecycle Management**: Automates the "Future -> On-Hand" transition on receipt.

#### Beamlytics Promising Engine
- **Decision Engine**: purely calculates *when* an order can be fulfilled based on data from UIS.
- **No Side Effects**: Does not write to DB or creating reservations.

## 2. Implementation Phases

### Phase 1: Database & Schema
1.  **Migration**: Add `inventory_type` column to `reservations` table.
2.  **Schema**: Formalize `asns` and `asn_items` tables with proper foreign keys and indexes.

### Phase 2: Core API (CRUD)
1.  **ASN Endpoints**: Implement `POST /asn` and `PUT /asn/:id`.
2.  **Reservation Update**: Modify `createReservation` to accept and validate `inventoryType`.
    *   Validation: If `FUTURE`, ensure `expiryDate > asn.expectedArrival`.
    *   Validation: If `ON_HAND`, ensure `redis.onHand >= requestedQty`.

### Phase 3: The "Receipt" Lifecycle
1.  **Receipt Listener**: Implement a Kafka Consumer in `InventoryService` (or new worker) subscribing to `events-input` (filtering for Receipt types).
2.  **Migration Logic**:
    *   On Receipt: Query active Future Reservations for the SKU/ASN.
    *   Transaction:
        *   Update Reservation: `inventory_type` = `ON_HAND`.
        *   Delete/Archive ASN record.

### Phase 4: Intelligent Updates (The "Promising" Logic)
1.  **ASN Update Handler**:
    *   When `PUT /asn/:id` changes the arrival date:
    *   Find all affected reservations.
    *   **Logic**:
        *   If `NewDate <= PromiseDate`: No-op (or update metadata).
        *   If `NewDate > PromiseDate`: Cancel Reservation & Trigger Alert.

## 3. Detailed Design

### 3.1 Reservation Migration Logic (Pseudo-code)

```typescript
async function handleReceipt(asnId: string, sku: string, qtyReceived: number) {
    // 1. Get Future Reservations
    const futureRes = await db.getFutureReservations(sku);
    
    // 2. Sort by Priority (e.g., Created Date)
    futureRes.sort((a, b) => a.createdAt - b.createdAt);
    
    let remainingReceipt = qtyReceived;
    
    for (const res of futureRes) {
        if (remainingReceipt <= 0) break;
        
        // Move to OnHand
        // NOTE: We don't change quantity, we just change the "pointer"
        // The reservation is now backed by the physical goods in Redis (logically)
        // Since Redis OnHand is "Available - Reserved", and we just increased Available via Stream Processor,
        // We technically need to ensure the Stream Processor *decrements* for these migrated reservations?
        // OR: The Stream Processor sees "Receipt (+100)" -> Redis OnHand = 100.
        // If we have 20 reserved: Redis should reflect 80 Available.
        // We need to send a "Silent Reserve" event to Stream Processor? 
        // OR: The reservation logic in Redis is separate. 
        
        // ARCHITECTURE DECISION: 
        // If Stream Processor manages Redis OnHand, it needs to know about these migrated reservations.
        // Action: Publish 'INVENTORY_RESERVED' event to Kafka immediately after migration.
        
        await db.updateReservationType(res.id, 'ON_HAND');
        await kafka.publish('inventory-reserved', { ...res, type: 'MIGRATED' });
        
        remainingReceipt -= res.qty;
    }
}
```

## 4. Gap Analysis Summary
*See `docs/future_inventory_gap_analysis.md` for full details.*
- **Critical Missing**: ASN Write API, Inventory Type in DB, Receipt Event Listener.
