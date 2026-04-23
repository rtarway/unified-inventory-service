# Promising Engine Analysis: Future Inventory Alignment

## 1. Overview
This document analyzes the `beamlytics-promising-engine` codebase to evaluate its support for Future Inventory and alignment with the requirements defined in `docs/future_inventory_requirements.md`.

## 2. Code Analysis

### 2.1 Inventory Fetching (`HttpInventoryProvider`)
- **Location**: `src/services/http-providers.ts`
- **Logic**: Calls `POST /inventory/query` to the Unified Inventory Service.
- **Mapping**: Correctly maps the `future` object from the API response to the engine's internal `Inventory` interface (`futureQty`, `futureDetails`).
- **Verdict**: ✅ **Aligned**. The engine successfully retrieves future inventory data (ASNs, Quantities, ETAs).

### 2.2 Decision Logic (`SourcingEngine`)
- **Location**: `src/sourcing/engine.ts` (Method: `checkAvailability`)
- **Logic**:
    1.  Calculates `onHandNet` (Qty - SafetyStock - Reserved).
    2.  If `onHandNet >= request.qty`: Uses On-Hand (Available Now).
    3.  If `onHandNet < request.qty`:
        - Checks `futureDetails` (ASNs).
        - Sorts ASNs by ETA.
        - Accumulates quantity until demand is met.
        - Sets availability date to the latest ETA of the consumed ASNs.
- **Verdict**: ✅ **Aligned**. The engine correctly prefers On-Hand inventory and falls back to Future Inventory only when necessary, accurately calculating the availability date.

### 2.3 Reservation Execution (`PromisingAgent`)
- **Location**: `src/agent/promising-agent.ts`
- **Requirement**: "Beamlytics-promising-engine will need to call reservation API... to make reservation against future inventory... input must specify inventory type."
- **Finding**:
    - The `PromisingAgent` calls `engine.calculatePromise(order)`.
    - It returns a `PromiseResponse` (packages, dates, costs).
    - **CRITICAL GAP**: There is **NO** code to call `POST /reservations` or similar endpoints. The agent calculates the *plan* but does not *commit* (reserve) it.
- **Verdict**: ❌ **Not Aligned**. The engine is currently "Read-Only". It lacks the capability to execute reservations for either On-Hand or Future inventory.

## 3. Summary of Gaps

| Feature | Status | Description |
| :--- | :--- | :--- |
| **Read Future Inventory** | ✅ | Implemented in `HttpInventoryProvider`. |
| **Availability Calculation** | ✅ | Implemented in `SourcingEngine`. |
| **Execute Reservation** | ❌ | **Missing.** No API client or logic to call `POST /reservations`. |
| **Inventory Type Flag** | ❌ | **Missing.** Since reservation logic is absent, the ability to specify `type: 'FUTURE'` vs `'ON_HAND'` is also absent. |

## 4. Recommendations
1.  **Enhance `HttpInventoryProvider`**: Add a `reserveInventory(payload)` method that wraps `POST /reservations`.
2.  **Update `PromisingAgent`**:
    - Add a "Commit" step after decision making.
    - If the selected strategy uses Future Inventory (detected via Date > Now), call `reserveInventory` with `type: 'FUTURE'`.
    - If using On-Hand, call with `type: 'ON_HAND'`.
