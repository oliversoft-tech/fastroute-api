import { Router } from 'express';
import { sendConfirmationSMS, getConfirmationHistory } from '../handlers/deliveryHandlers';
import { receiveSMSReply } from '../handlers/webhookHandlers';
import { authMiddleware } from '../middlewares/auth';

const router = Router();

router.post('/deliveries/:id/send-confirmation-sms', authMiddleware, sendConfirmationSMS);
router.get('/deliveries/:id/confirmation-history', authMiddleware, getConfirmationHistory);
router.post('/webhooks/sms-replies', receiveSMSReply);

export default router;