import { Router, Request, Response } from 'express';
import { ISMSAdapter } from '../adapters/sms/ISMSAdapter';

export function createWebhookRouter(smsAdapter: ISMSAdapter, allowedIPs: string[]): Router {
  const router = Router();

  router.post('/sms/reply', (req: Request, res: Response) => {
    const clientIP = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '';
    if (!allowedIPs.some(ip => clientIP.toString().includes(ip))) {
      return res.status(403).json({ error: 'Forbidden IP' });
    }

    const signature = req.headers['x-twilio-signature'] as string;
    const bodyString = JSON.stringify(req.body);
    if (!smsAdapter.validateWebhook(signature, bodyString)) {
      return res.status(401).json({ error: 'Invalid signature' });
    }

    const reply = smsAdapter.receiveReply(req.body);
    if (!reply) {
      return res.status(400).json({ error: 'Invalid payload' });
    }

    console.log(`SMS reply from ${reply.from}: ${reply.message}`);
    // Process reply logic here (e.g., update delivery confirmation)
    res.status(200).json({ status: 'received' });
  });

  return router;
}

// file: src/index.ts
import express from 'express';
import { TwilioAdapter } from './adapters/sms/TwilioAdapter';
import { createWebhookRouter } from './routes/webhooks';
import { NotificationService } from './services/NotificationService';
import { EmailService } from './services/EmailService';
import { PushService } from './services/PushService';

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const twilioAdapter = new TwilioAdapter(
  process.env.TWILIO_ACCOUNT_SID!,
  process.env.TWILIO_AUTH_TOKEN!,
  process.env.TWILIO_PHONE_NUMBER!,
  (process.env.TWILIO_ALLOWED_IPS || '').split(',')
);

const notificationService = new NotificationService(
  twilioAdapter,
  new EmailService(),
  new PushService()
);

app.use('/webhooks', createWebhookRouter(twilioAdapter, (process.env.TWILIO_ALLOWED_IPS || '').split(',')));

app.listen(3000, () => console.log('API running on port 3000'));