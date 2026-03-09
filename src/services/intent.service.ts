import { pool } from '../db/connection';

export interface Intent {
  id: number;
  delivery_id: number;
  type: 'reschedule' | 'address_change' | 'confirm';
  data: any;
  applied_at: Date | null;
  created_at: Date;
}

export class IntentService {
  async findById(id: number): Promise<Intent | null> {
    const result = await pool.query('SELECT * FROM delivery_intents WHERE id = $1', [id]);
    return result.rows[0] || null;
  }

  async markApplied(id: number): Promise<void> {
    await pool.query(
      'UPDATE delivery_intents SET applied_at = NOW() WHERE id = $1',
      [id]
    );
  }

  async create(deliveryId: number, type: string, data: any): Promise<Intent> {
    const result = await pool.query(
      'INSERT INTO delivery_intents (delivery_id, type, data) VALUES ($1, $2, $3) RETURNING *',
      [deliveryId, type, data]
    );
    return result.rows[0];
  }
}