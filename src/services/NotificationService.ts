import { ISMSAdapter } from '../adapters/sms/ISMSAdapter';
import { EmailService } from './EmailService';
import { PushService } from './PushService';

interface NotificationConfig {
  maxRetries: number;
  fallbackThresholdHours: number;
}

export class NotificationService {
  constructor(
    private smsAdapter: ISMSAdapter,
    private emailService: EmailService,
    private pushService: PushService,
    private config: NotificationConfig = { maxRetries: 3, fallbackThresholdHours: 12 }
  ) {}

  async notifyDriver(driverId: string, phone: string, email: string, message: string, deliveryWindowHours: number): Promise<void> {
    let attempts = 0;
    let smsSuccess = false;

    while (attempts < this.config.maxRetries && !smsSuccess) {
      attempts++;
      const result = await this.smsAdapter.sendSMS(phone, message);
      if (result.success) {
        smsSuccess = true;
        console.log(`SMS sent to ${phone}, messageId: ${result.messageId}`);
      } else {
        console.error(`SMS attempt ${attempts} failed: ${result.error}`);
      }
    }

    if (!smsSuccess && deliveryWindowHours > this.config.fallbackThresholdHours) {
      console.log(`SMS failed after ${this.config.maxRetries} attempts, triggering fallback`);
      await Promise.allSettled([
        this.emailService.send(email, 'Notificação de Entrega', message),
        this.pushService.send(driverId, message)
      ]);
    }
  }
}