import { Request, Response } from 'express';
import { NotificationService } from '../services/notification.service';
import { DeliveryRepository } from '../repositories/delivery.repository';

export class DeliveryNotificationController {
  private notificationService = new NotificationService();
  private deliveryRepo = new DeliveryRepository();

  sendNotification = async (req: Request, res: Response) => {
    const { id } = req.params;
    const delivery = await this.deliveryRepo.findById(parseInt(id));
    
    if (!delivery || !delivery.recipient_phone) {
      return res.status(400).json({ error: 'Invalid delivery or missing phone' });
    }

    this.notificationService.sendSmsAsync(delivery).catch(err => 
      console.error('SMS send failed:', err)
    );

    return res.status(202).json({ message: 'Notification queued', delivery_id: id });
  };

  handleSmsWebhook = async (req: Request, res: Response) => {
    const rawBody = JSON.stringify(req.body);
    await this.notificationService.processSmsWebhook(rawBody, req.body);
    return res.status(200).json({ received: true });
  };
}