import { Router } from 'express';
import { ApplyIntentHandler } from '../handlers/deliveries/applyIntent.handler';
import { DeliveryService } from '../services/delivery.service';
import { IntentService } from '../services/intent.service';
import { EventService } from '../services/event.service';
import { asyncHandler } from '../middlewares/asyncHandler';

const router = Router();

const deliveryService = new DeliveryService();
const intentService = new IntentService();
const eventService = new EventService();
const applyIntentHandler = new ApplyIntentHandler(deliveryService, intentService, eventService);

router.patch('/:id/intents/:intentId', asyncHandler((req, res) => applyIntentHandler.handle(req, res)));

export { router as deliveriesRouter };