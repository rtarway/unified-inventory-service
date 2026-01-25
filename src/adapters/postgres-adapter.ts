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
}
