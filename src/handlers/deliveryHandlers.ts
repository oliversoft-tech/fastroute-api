import { Request, Response } from 'express';
import { enqueueSMSJob } from '../services/smsQueueService';
import { getDeliveryConfirmations } from '../services/deliveryConfirmationService';

export async function sendConfirmationSMS(req: Request, res: Response) {
  const deliveryId = parseInt(req.params.id);
  const { phoneNumber, message } = req.body;

  if (!phoneNumber || !message) {
    return res.status(400).json({ error: 'phoneNumber and message required' });
  }

  await enqueueSMSJob({ deliveryId, phoneNumber, message, attempt: 0 });
  res.status(202).json({ status: 'queued', deliveryId });
}

export async function getConfirmationHistory(req: Request, res: Response) {
  const deliveryId = parseInt(req.params.id);
  const history = await getDeliveryConfirmations(deliveryId);
  res.json({ deliveryId, timeline: history });
}

// file: src/handlers/webhookHandlers.ts
import { Request, Response } from 'express';
import { processSMSReply } from '../services/smsReplyService';
import logger from '../utils/logger';

export async function receiveSMSReply(req: Request, res: Response) {
  try {
    const { phoneNumber, message, timestamp, externalId } = req.body;
    await processSMSReply({ phoneNumber, message, timestamp, externalId });
    res.status(200).json({ received: true });
  } catch (error) {
    logger.error('SMS reply processing error', { error, body: req.body });
    res.status(200).json({ received: true });
  }
}