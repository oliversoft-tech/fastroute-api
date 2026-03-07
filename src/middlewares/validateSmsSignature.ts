import { Request, Response, NextFunction } from 'express';
import crypto from 'crypto';

const SMS_GATEWAY_SECRET = process.env.SMS_GATEWAY_SECRET || '';

export const validateSmsSignature = (req: Request, res: Response, next: NextFunction): void => {
  const signature = req.headers['x-sms-signature'] as string;
  const rawBody = JSON.stringify(req.body);
  const expectedSignature = crypto.createHmac('sha256', SMS_GATEWAY_SECRET).update(rawBody).digest('hex');

  if (!signature || signature !== expectedSignature) {
    res.status(401).json({ error: 'Invalid signature' });
    return;
  }

  next();
};