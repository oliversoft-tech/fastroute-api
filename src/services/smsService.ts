import twilio from 'twilio';
import { pool } from '../config/database';

const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);

export interface SMSResult {
  delivery_id: number;
  phone: string;
  sent: boolean;
  message_sid?: string;
  error?: string;
}

export async function sendDeliveryNotifications(): Promise<SMSResult[]> {
  const tomorrow = new Date();
  tomorrow.setDate(tomorrow.getDate() + 1);
  const tomorrowStr = tomorrow.toISOString().split('T')[0];

  const { rows } = await pool.query(`
    SELECT d.id, d.tracking_number, c.phone, c.name
    FROM deliveries d
    JOIN customers c ON d.customer_id = c.id
    WHERE d.planned_date = $1
      AND d.route_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM sms_logs WHERE delivery_id = d.id AND sent_at::date = CURRENT_DATE
      )
  `, [tomorrowStr]);

  const results: SMSResult[] = [];

  for (const row of rows) {
    const message = `Ola ${row.name}! Sua encomenda ${row.tracking_number} sera entregue amanha. Responda: 1=Confirmar 2=Reagendar 3=Retirar`;

    try {
      const response = await client.messages.create({
        body: message,
        from: process.env.TWILIO_PHONE_NUMBER,
        to: row.phone
      });

      await pool.query(
        'INSERT INTO sms_logs (delivery_id, phone, message, message_sid, sent_at, status) VALUES ($1, $2, $3, $4, NOW(), $5)',
        [row.id, row.phone, message, response.sid, 'sent']
      );

      results.push({
        delivery_id: row.id,
        phone: row.phone,
        sent: true,
        message_sid: response.sid
      });
    } catch (error: any) {
      await pool.query(
        'INSERT INTO sms_logs (delivery_id, phone, message, sent_at, status, error) VALUES ($1, $2, $3, NOW(), $4, $5)',
        [row.id, row.phone, message, 'failed', error.message]
      );

      results.push({
        delivery_id: row.id,
        phone: row.phone,
        sent: false,
        error: error.message
      });
    }
  }

  return results;
}