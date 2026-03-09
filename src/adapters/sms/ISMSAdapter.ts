export interface ISMSAdapter {
  sendSMS(to: string, message: string): Promise<{ success: boolean; messageId?: string; error?: string }>;
  receiveReply(webhookPayload: any): { from: string; message: string; messageId: string } | null;
  validateWebhook(signature: string, body: string): boolean;
}

// file: src/adapters/sms/TwilioAdapter.ts
import twilio from 'twilio';
import crypto from 'crypto';
import { ISMSAdapter } from './ISMSAdapter';

export class TwilioAdapter implements ISMSAdapter {
  private client: twilio.Twilio;
  private fromNumber: string;
  private authToken: string;
  private allowedIPs: string[];

  constructor(accountSid: string, authToken: string, fromNumber: string, allowedIPs: string[]) {
    this.client = twilio(accountSid, authToken);
    this.fromNumber = fromNumber;
    this.authToken = authToken;
    this.allowedIPs = allowedIPs;
  }

  async sendSMS(to: string, message: string): Promise<{ success: boolean; messageId?: string; error?: string }> {
    try {
      const result = await this.client.messages.create({ body: message, from: this.fromNumber, to });
      return { success: true, messageId: result.sid };
    } catch (err: any) {
      return { success: false, error: err.message };
    }
  }

  receiveReply(webhookPayload: any): { from: string; message: string; messageId: string } | null {
    if (!webhookPayload.From || !webhookPayload.Body || !webhookPayload.MessageSid) return null;
    return { from: webhookPayload.From, message: webhookPayload.Body, messageId: webhookPayload.MessageSid };
  }

  validateWebhook(signature: string, body: string): boolean {
    const expected = crypto.createHmac('sha1', this.authToken).update(body).digest('base64');
    return signature === expected;
  }
}