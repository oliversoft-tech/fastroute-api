import { Router } from 'express';
import { DeliveryNotificationController } from '../controllers/delivery-notification.controller';
import { authMiddleware } from '../middlewares/auth.middleware';

const router = Router();
const controller = new DeliveryNotificationController();

router.post('/deliveries/:id/notifications', authMiddleware, controller.sendNotification);
router.post('/webhooks/sms-response', controller.handleSmsWebhook);

export default router;