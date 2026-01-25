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

    // --- Reservations ---

    async createReservation(
        reservationId: string,
        orderId: string,
        sku: string,
        qty: number,
        type: 'SOFT' | 'HARD',
        locationId?: string,
        ttlMinutes: number = 15
    ): Promise<void> {
        const expiresAt = type === 'SOFT' ? `NOW() + interval '${ttlMinutes} minutes'` : 'NULL';

        const query = `
            INSERT INTO reservations (reservation_id, order_id, sku, location_id, qty, type, status, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, 'ACTIVE', ${expiresAt})
        `;

        await this.pool.query(query, [reservationId, orderId, sku, locationId, qty, type]);
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
