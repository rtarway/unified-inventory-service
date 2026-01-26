import { Pool, PoolClient } from 'pg';

export class PostgresAdapter {
    private pool: Pool;

    constructor(connectionString: string = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/inventory_db') {
        this.pool = new Pool({
            connectionString,
        });

        this.pool.on('error', (err) => {
            console.error('Unexpected error on idle client', err);
            process.exit(-1);
        });
    }

    async connect(): Promise<void> {
        // Pool auto-connects
        const client = await this.pool.connect();
        client.release();
        console.log('Postgres connected');
    }

    async disconnect(): Promise<void> {
        await this.pool.end();
    }

    // --- Future Inventory (ASNs) ---

    async getInboundInventory(sku: string, locationId: string, windowDays: number = 30): Promise<any[]> {
        const query = `
            SELECT 
                ai.sku, 
                a.asn_id, 
                a.status, 
                a.estimated_arrival, 
                ai.qty_shipped - ai.qty_received as qty_remaining
            FROM asn_items ai
            JOIN asns a ON ai.asn_id = a.asn_id
            WHERE 
                ai.sku = $1 
                AND a.destination_location_id = $2
                AND a.status IN ('CREATED', 'IN_TRANSIT')
                AND a.estimated_arrival <= NOW() + interval '${windowDays} days'
            ORDER BY a.estimated_arrival ASC
        `;
        const res = await this.pool.query(query, [sku, locationId]);
        return res.rows;
    }

    async createASN(asn: any, items: any[]): Promise<void> {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');

            const asnQuery = `
                INSERT INTO asns (asn_id, po_id, carrier_name, tracking_number, status, origin_location_id, destination_location_id, estimated_arrival)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            `;
            await client.query(asnQuery, [
                asn.asnId, asn.poId, asn.carrierName, asn.trackingNumber,
                asn.status || 'CREATED', asn.originLocationId, asn.destinationLocationId, asn.estimatedArrival
            ]);

            const itemQuery = `
                INSERT INTO asn_items (asn_id, sku, qty_shipped)
                VALUES ($1, $2, $3)
            `;
            for (const item of items) {
                await client.query(itemQuery, [asn.asnId, item.sku, item.qty]);
            }

            await client.query('COMMIT');
        } catch (e) {
            await client.query('ROLLBACK');
            throw e;
        } finally {
            client.release();
        }
    }

    async updateASN(asnId: string, updates: any): Promise<void> {
        const fields: string[] = [];
        const values: any[] = [];
        let idx = 1;

        if (updates.status) { fields.push(`status = $${idx++}`); values.push(updates.status); }
        if (updates.estimatedArrival) { fields.push(`estimated_arrival = $${idx++}`); values.push(updates.estimatedArrival); }
        if (updates.carrierName) { fields.push(`carrier_name = $${idx++}`); values.push(updates.carrierName); }
        if (updates.trackingNumber) { fields.push(`tracking_number = $${idx++}`); values.push(updates.trackingNumber); }

        if (fields.length === 0) return;

        values.push(asnId);
        const query = `UPDATE asns SET ${fields.join(', ')}, updated_at = NOW() WHERE asn_id = $${idx}`;
        await this.pool.query(query, values);
    }

    // --- Reservations ---

    async createReservation(
        reservationId: string,
        orderId: string,
        sku: string,
        qty: number,
        type: 'SOFT' | 'HARD',
        locationId?: string,
        ttlMinutes: number = 15,
        inventoryType: 'ON_HAND' | 'FUTURE' = 'ON_HAND'
    ): Promise<void> {
        const expiresAt = type === 'SOFT' ? `NOW() + interval '${ttlMinutes} minutes'` : 'NULL';

        const query = `
            INSERT INTO reservations (reservation_id, order_id, sku, location_id, qty, type, status, expires_at, inventory_type)
            VALUES ($1, $2, $3, $4, $5, $6, 'ACTIVE', ${expiresAt}, $7)
        `;

        await this.pool.query(query, [reservationId, orderId, sku, locationId, qty, type, inventoryType]);
    }

    async getReservation(orderId: string, sku: string): Promise<any> {
        const query = `
            SELECT * FROM reservations 
            WHERE order_id = $1 AND sku = $2 
            ORDER BY created_at DESC LIMIT 1
        `;
        const res = await this.pool.query(query, [orderId, sku]);
        return res.rows[0];
    }

    async updateReservationStatus(reservationId: string, status: 'CONSUMED' | 'CANCELLED' | 'EXPIRED'): Promise<void> {
        const query = `UPDATE reservations SET status = $1, updated_at = NOW() WHERE reservation_id = $2`;
        await this.pool.query(query, [status, reservationId]);
    }

    async getActiveReservations(sku: string, locationId?: string): Promise<number> {
        let query = `
            SELECT SUM(qty) as total
            FROM reservations
            WHERE sku = $1 AND status = 'ACTIVE'
            AND (expires_at IS NULL OR expires_at > NOW())
        `;
        const params = [sku];

        if (locationId) {
            query += ` AND location_id = $2`;
            params.push(locationId);
        }

        const res = await this.pool.query(query, params);
        return parseInt(res.rows[0].total || '0');
    }

    async getExpiredReservations(): Promise<any[]> {
        const query = `
            SELECT * FROM reservations
            WHERE status = 'ACTIVE'
            AND expires_at IS NOT NULL
            AND expires_at < NOW()
        `;
        const res = await this.pool.query(query);
        return res.rows;
    }

    async getFutureReservations(sku: string): Promise<any[]> {
        const query = `
            SELECT * FROM reservations
            WHERE sku = $1 
            AND status = 'ACTIVE'
            AND inventory_type = 'FUTURE'
            ORDER BY created_at ASC
        `;
        const res = await this.pool.query(query, [sku]);
        return res.rows;
    }

    async updateReservationType(reservationId: string, newType: 'ON_HAND' | 'FUTURE'): Promise<void> {
        const query = `UPDATE reservations SET inventory_type = $1, updated_at = NOW() WHERE reservation_id = $2`;
        await this.pool.query(query, [newType, reservationId]);
    }

    // --- Allocations ---

    async createAllocation(
        allocationId: string,
        orderId: string,
        sku: string,
        qty: number,
        locationId: string,
        reservationId?: string
    ): Promise<void> {
        const query = `
            INSERT INTO allocations (allocation_id, order_id, sku, qty, location_id, reservation_id, status)
            VALUES ($1, $2, $3, $4, $5, $6, 'ALLOCATED')
        `;
        await this.pool.query(query, [allocationId, orderId, sku, qty, locationId, reservationId]);
    }

    async getAllocation(orderId: string, sku: string): Promise<any> {
        const query = `
            SELECT * FROM allocations 
            WHERE order_id = $1 AND sku = $2 
            AND status = 'ALLOCATED'
            ORDER BY created_at DESC LIMIT 1
        `;
        const res = await this.pool.query(query, [orderId, sku]);
        return res.rows[0];
    }

    async updateAllocationStatus(allocationId: string, status: 'SHIPPED' | 'CANCELLED'): Promise<void> {
        const query = `UPDATE allocations SET status = $1, updated_at = NOW() WHERE allocation_id = $2`;
        await this.pool.query(query, [status, allocationId]);
    }
}
