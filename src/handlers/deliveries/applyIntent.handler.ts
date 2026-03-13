import { Request, Response } from 'express';
import { DeliveryService } from '../../services/delivery.service';
import { IntentService } from '../../services/intent.service';
import { EventService } from '../../services/event.service';
import { AppError } from '../../errors/AppError';

export class ApplyIntentHandler {
  constructor(
    private deliveryService: DeliveryService,
    private intentService: IntentService,
    private eventService: EventService
  ) {}

  async handle(req: Request, res: Response): Promise<Response> {
    const { id, intentId } = req.params;
    const deliveryId = parseInt(id, 10);
    const intentIdNum = parseInt(intentId, 10);

    const delivery = await this.deliveryService.findById(deliveryId);
    if (!delivery) {
      throw new AppError('Delivery not found', 404);
    }

    const intent = await this.intentService.findById(intentIdNum);
    if (!intent || intent.delivery_id !== deliveryId) {
      throw new AppError('Intent not found or does not belong to delivery', 404);
    }

    if (intent.applied_at) {
      return res.json({ message: 'Intent already applied', delivery });
    }

    let result;
    switch (intent.type) {
      case 'reschedule':
        result = await this.handleReschedule(delivery, intent);
        break;
      case 'address_change':
        result = await this.handleAddressChange(delivery, intent);
        break;
      case 'confirm':
        result = await this.handleConfirm(delivery, intent);
        break;
      default:
        throw new AppError('Unknown intent type', 400);
    }

    await this.intentService.markApplied(intentIdNum);

    return res.json(result);
  }

  private async handleReschedule(delivery: any, intent: any) {
    const newDate = intent.data.scheduled_date;
    
    if (delivery.route_id && delivery.route_status === 'in_progress') {
      await this.eventService.emit('ConflictDetected', {
        delivery_id: delivery.id,
        type: 'reschedule_after_route_start',
        requested_date: newDate,
        current_route_id: delivery.route_id
      });
      throw new AppError('Cannot reschedule: route already in progress', 409);
    }

    const updated = await this.deliveryService.update(delivery.id, {
      scheduled_date: newDate
    });

    await this.eventService.emit('DeliveryRescheduled', {
      delivery_id: delivery.id,
      old_date: delivery.scheduled_date,
      new_date: newDate,
      intent_id: intent.id
    });

    return { message: 'Delivery rescheduled', delivery: updated };
  }

  private async handleAddressChange(delivery: any, intent: any) {
    if (delivery.route_id && delivery.route_status === 'in_progress') {
      await this.eventService.emit('ConflictDetected', {
        delivery_id: delivery.id,
        type: 'address_change_after_route_start',
        requested_address: intent.data.address,
        current_route_id: delivery.route_id
      });
      throw new AppError('Cannot change address: route already in progress', 409);
    }

    const newAddress = await this.deliveryService.createAddress({
      street: intent.data.address.street,
      number: intent.data.address.number,
      complement: intent.data.address.complement,
      neighborhood: intent.data.address.neighborhood,
      city: intent.data.address.city,
      state: intent.data.address.state,
      zip_code: intent.data.address.zip_code,
      lat: intent.data.address.lat,
      lng: intent.data.address.lng
    });

    const updated = await this.deliveryService.update(delivery.id, {
      address_id: newAddress.id
    });

    await this.eventService.emit('AddressChanged', {
      delivery_id: delivery.id,
      old_address_id: delivery.address_id,
      new_address_id: newAddress.id,
      intent_id: intent.id
    });

    return { message: 'Address changed', delivery: updated, new_address: newAddress };
  }

  private async handleConfirm(delivery: any, intent: any) {
    const updated = await this.deliveryService.update(delivery.id, {
      recipient_confirmed: true,
      confirmed_at: new Date()
    });

    return { message: 'Delivery confirmed by recipient', delivery: updated };
  }
}