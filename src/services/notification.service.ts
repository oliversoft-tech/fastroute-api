import twilio from 'twilio';
import { NotificationLogRepository } from '../repositories/notification-log.repository';
import { Delivery } from '../models/delivery.model';

interface SmsMetadata {
  cost_estimate: number;
  provider: string;
  message_sid?: string;
}

export class NotificationService {
  private client = twilio(process.env.TWILIO_SID!, process.env.TWILIO_TOKEN!);
  private logRepo = new NotificationLogRepository();

  async sendSmsAsync(delivery: Delivery): Promise<void> {
    const phone = this.normalizePhone(delivery.recipient_phone);
    const message = this.buildTemplate(delivery);
    const estimatedCost = 0.0075;

    try {
      const result = await this.client.messages.create({
        to: phone,
        from: process.env.TWILIO_PHONE!,
        body: message
      });

      const metadata: SmsMetadata = {
        cost_estimate: estimatedCost,
        provider: 'twilio',
        message_sid: result.sid
      };

      await this.logRepo.create({
        delivery_id: delivery.id,
        channel: 'sms',
        status: 'sent',
        metadata: JSON.stringify(metadata),
        sent_at: new Date()
      });
    } catch (error: any) {
      await this.logRepo.create({
        delivery_id: delivery.id,
        channel: 'sms',
        status: 'failed',
        metadata: JSON.stringify({ error: error.message, cost_estimate: estimatedCost }),
        sent_at: new Date()
      });
      throw error;
    }
  }

  async processSmsWebhook(rawBody: string, parsed: any): Promise<void> {
    await this.logRepo.createWebhookLog({
      raw_body: rawBody,
      message_sid: parsed.MessageSid,
      status: parsed.MessageStatus,
      received_at: new Date()
    });

    if (parsed.MessageSid) {
      await this.logRepo.updateByMessageSid(parsed.MessageSid, {
        status: parsed.MessageStatus,
        metadata: rawBody
      });
    }
  }

  private normalizePhone(phone: string): string {
    return phone.startsWith('+') ? phone : `+55${phone.replace(/\D/g, '')}`;
  }

  private buildTemplate(delivery: Delivery): string {
    return `Olá! Sua entrega #${delivery.tracking_code} está a caminho. Previsão: ${delivery.estimated_date}. Rastreie em: ${process.env.APP_URL}/track/${delivery.tracking_code}`;
  }
}