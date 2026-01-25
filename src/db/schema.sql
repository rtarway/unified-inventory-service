-- Future Inventory and Reservation Schema

-- 1. Purchase Orders
CREATE TABLE IF NOT EXISTS purchase_orders (
    po_id VARCHAR(50) PRIMARY KEY,
    vendor_id VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'CREATED', -- CREATED, CONFIRMED, CLOSED
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS po_items (
    id SERIAL PRIMARY KEY,
    po_id VARCHAR(50) REFERENCES purchase_orders(po_id),
    sku VARCHAR(50) NOT NULL,
    qty_ordered INTEGER NOT NULL,
    expected_cost DECIMAL(10, 2)
);

-- 2. Advanced Shipment Notices (ASNs)
CREATE TABLE IF NOT EXISTS asns (
    asn_id VARCHAR(50) PRIMARY KEY,
    po_id VARCHAR(50) REFERENCES purchase_orders(po_id),
    carrier_name VARCHAR(100),
    tracking_number VARCHAR(100),
    status VARCHAR(20) NOT NULL DEFAULT 'CREATED', -- CREATED, IN_TRANSIT, DELIVERED, RECEIVED
    origin_location_id VARCHAR(50),
    destination_location_id VARCHAR(50),
    shipped_at TIMESTAMP WITH TIME ZONE,
    estimated_arrival TIMESTAMP WITH TIME ZONE,
    actual_arrival TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS asn_items (
    id SERIAL PRIMARY KEY,
    asn_id VARCHAR(50) REFERENCES asns(asn_id),
    sku VARCHAR(50) NOT NULL,
    qty_shipped INTEGER NOT NULL,
    qty_received INTEGER DEFAULT 0
);

-- 3. Reservations (Soft and Hard)
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    sku VARCHAR(50) NOT NULL,
    location_id VARCHAR(50), -- Optional: If NULL, reserved at network level
    qty INTEGER NOT NULL,
    type VARCHAR(20) NOT NULL, -- SOFT, HARD
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE, CONSUMED, CANCELLED, EXPIRED
    expires_at TIMESTAMP WITH TIME ZONE, -- For SOFT reservations
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_asn_dest_eta ON asns(destination_location_id, estimated_arrival);
CREATE INDEX IF NOT EXISTS idx_res_sku_status ON reservations(sku, status);
CREATE INDEX IF NOT EXISTS idx_res_order ON reservations(order_id);

-- 4. Allocations
CREATE TABLE IF NOT EXISTS allocations (
    allocation_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    sku VARCHAR(50) NOT NULL,
    qty INTEGER NOT NULL,
    location_id VARCHAR(50),
    reservation_id VARCHAR(50) REFERENCES reservations(reservation_id),
    status VARCHAR(20) NOT NULL DEFAULT 'ALLOCATED', -- ALLOCATED, SHIPPED, CANCELLED
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alloc_order ON allocations(order_id);
