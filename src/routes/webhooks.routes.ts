import { Router } from 'express';
import { smsWebhookController } from '../controllers/webhooks.controller';
import { validateSmsSignature } from '../middlewares/validateSmsSignature';

const router = Router();

router.post('/webhooks/sms', validateSmsSignature, smsWebhookController);

export default router;