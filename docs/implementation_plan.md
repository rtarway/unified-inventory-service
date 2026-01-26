# Implementation Plan - Test Organization

The user wants to consolidate E2E tests (Kafka -> Stream Processor API) into a proper test suite and separate them from isolated unit tests. Additionally, `package.json` needs updating to run these via `npm test`.

## User Review Required
> [!NOTE]
> I will move existing tests into `tests/unit/`.
> I will create `tests/e2e/System.test.ts` which effectively replaces `tests/e2e-k8s.sh` but uses Jest and TypeScript. This test will use `child_process` to execute `kubectl` commands to ensure connectivity without manual port-forwarding setups, mimicking the robustness of the shell script.

## Proposed Changes

### 1. Directory Structure
- Create `tests/unit/`
- Create `tests/e2e/`
- Move:
    - `tests/InventoryService.test.ts` -> `tests/unit/InventoryService.test.ts`
    - `tests/EventDrivenInventory.test.ts` -> `tests/unit/EventDrivenInventory.test.ts`
    - `tests/scenarios.test.ts` -> `tests/unit/Scenarios.test.ts`
- Archive/Delete:
    - `tests/e2e-k8s.sh` (after porting logic)

### 2. New E2E Test Suite
#### [NEW] [tests/e2e/System.test.ts](file:///Users/rtarway/mygithubprojects/unified-inventory-service/tests/e2e/System.test.ts)
- Use standard Jest structure (`describe`, `test`).
- Implement helper functions equivalent to the shell script:
    - `produceKafkaEvent(key, value, type)` -> calls `kubectl exec ... kafka-console-producer`
    - `getInventoryApi(key)` -> calls `kubectl exec ... wget`
    - `callServiceApi(method, endpoint, body)` -> calls `kubectl exec ... wget`
- Implement the test flow:
    - Reset Inventory (100)
    - Create Reservation
    - Check API
    - Allocate
    - Check API

### 3. Package.json Scripts
#### [MODIFY] [package.json](file:///Users/rtarway/mygithubprojects/unified-inventory-service/package.json)
- Update `scripts`:
    - `"test"`: `"jest tests/unit"` (Fast, isolated)
    - `"test:e2e"`: `"jest tests/e2e"` (Slow, integration)
    - `"test:all"`: `"jest"`

## Verification
- Run `npm test` -> Should run 3 unit test files (Pass).
- Run `npm run test:e2e` -> Should run 1 system test file (Pass, assuming K8s is running).
