import { Request, Response } from 'express';
import { parseSmsResponse, updateDeliveryStatus } from '@fastroute/domain';
import { deliveryRepository } from '../repositories/delivery.repository';

export const smsWebhookController = async (req: Request, res: Response): Promise<void> => {
  try {
    const parsed = parseSmsResponse(req.body);
    
    if (!parsed.deliveryId || !parsed.status) {
      res.status(400).json({ error: 'Invalid webhook payload', details: 'Missing deliveryId or status' });
      return;
    }

    const delivery = await deliveryRepository.findById(parsed.deliveryId);
    
    if (!delivery) {
      res.status(404).json({ error: 'Delivery not found' });
      return;
    }

    const updatedDelivery = updateDeliveryStatus(delivery, parsed.status, parsed.timestamp);
    await deliveryRepository.update(updatedDelivery);
    await deliveryRepository.saveSmsResponse(parsed.deliveryId, req.body);

    res.status(200).json({ success: true, deliveryId: parsed.deliveryId });
  } catch (error) {
    res.status(400).json({ error: 'Failed to process SMS webhook', message: (error as Error).message });
  }
};