# Future Inventory Requirements

## 1. Overview
The Unified Inventory Service must support **Future Inventory**, allowing reservations against inbound shipments (ASNs). This capability supports the `beamlytics-promising-engine` in providing accurate "Available to Promise" dates for backorders or pre-orders.

## 2. Roles & Responsibilities

### 2.1 Unified Inventory Service (System of Record)
- **Manage Data**: Stores and manages ASNs (Future Inventory) and On-Hand Inventory.
- **Expose Visibility**: Provides API endpoints to query potential availability (Unified Position).
- **Execute Commitments**: Provides a `POST /reservations` endpoint that accepts an `inventory_type` ('ON_HAND' or 'FUTURE') to lock inventory.
- **Lifecycle Management**: Automatically migrates Future Reservations to On-Hand upon shipment receipt.

### 2.2 Beamlytics Promising Engine (Decision Support)
- **Role**: **Read-Only Intelligence**.
- **Function**: Queries the Inventory Service to determine *when* an order can be fulfilled (Promise Date).
- **Output**: Returns a `PromiseResponse` containing the expected Ship Date, Delivery Date, and Carriers.
- **Constraint**: The Promising Engine **DOES NOT** create reservations. It identifies the "Best Option".

### 2.3 External Order System (Checkout / OMS)
- **Role**: **Execution**.
- **Function**:
    1.  Calls `Promising Engine` to get options/dates for the customer.
    2.  Customer confirms order.
    3.  **Calls `Unified Inventory Service`** to create the reservation, passing the `inventory_type` and `location` determined by the Promising Engine's response.

## 3. Data Sources & Logic (Updated)
- **ASN Data**: Ingested via API/EDI. Contains SKU, Quantity, ETA.
- **Reservation Logic**:
    - **Future Reservation**: Created when the Promise Date relies on an inbound shipment.
    - **Validation**: `expiryDate` of the reservation must be > `ASN.ETA`.
- **Receipt Lifecycle**:
    - Warehouse receives goods -> Kafka Event `INVENTORY_RECEIPT`.
    - Inventory Service consumes event -> Clears ASN -> Migrates linked Future Reservations to On-Hand.

## 4. Interfaces
- **Inventory Service**:
    - `POST /reservations`: Needs `inventoryType` field.
    - `POST /asn`: specific endpoints to manage ASNs.
- **Promising Engine**:
    - No changes to API surface (Input: Order, Output: Promise).
    - Internal logic must verify Future Inventory availability to calculate the valid Promise Date.
