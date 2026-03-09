import Bull from 'bull';
import { sendSMS } from './smsGatewayService';
import { createDeliveryConfirmation, updateConfirmationStatus } from './deliveryConfirmationService';
import logger from '../utils/logger';

const smsQueue = new Bull('sms-queue', process.env.REDIS_URL || 'redis://localhost:6379');
const deadLetterQueue = new Bull('sms-dlq', process.env.REDIS_URL || 'redis://localhost:6379');

interface SMSJob {
  deliveryId: number;
  phoneNumber: string;
  message: string;
  attempt: number;
}

export async function enqueueSMSJob(data: SMSJob) {
  await smsQueue.add(data, { attempts: 3, backoff: { type: 'exponential', delay: 5000 } });
}

smsQueue.process(async (job) => {
  const { deliveryId, phoneNumber, message, attempt } = job.data;
  
  const confirmationId = await createDeliveryConfirmation({
    deliveryId,
    phoneNumber,
    originalMessage: message,
    status: 'sending',
  });

  try {
    const result = await sendSMS(phoneNumber, message);
    await updateConfirmationStatus(confirmationId, 'sent', result.externalId);
    logger.info('SMS sent', { deliveryId, confirmationId, externalId: result.externalId });
  } catch (error) {
    logger.error('SMS send failed', { deliveryId, confirmationId, attempt: job.attemptsMade, error });
    if (job.attemptsMade >= 3) {
      await deadLetterQueue.add({ deliveryId, phoneNumber, message, error: error.message });
      await updateConfirmationStatus(confirmationId, 'failed');
    }
    throw error;
  }
});

// file: src/services/smsGatewayService.ts
import axios from 'axios';

export async function sendSMS(phoneNumber: string, message: string) {
  const response = await axios.post(process.env.SMS_GATEWAY_URL!, {
    to: phoneNumber,
    body: message,
    from: process.env.SMS_FROM_NUMBER,
  }, {
    headers: { 'Authorization': `Bearer ${process.env.SMS_API_KEY}` },
    timeout: 10000,
  });
  return { externalId: response.data.messageId, status: response.data.status };
}

// file: src/services/deliveryConfirmationService.ts
import db from '../config/database';

export async function createDeliveryConfirmation(data: {
  deliveryId: number;
  phoneNumber: string;
  originalMessage: string;
  status: string;
}) {
  const [result] = await db('delivery_confirmations').insert({
    delivery_id: data.deliveryId,
    phone_number: data.phoneNumber,
    original_message: data.originalMessage,
    status: data.status,
    sent_at: db.fn.now(),
  }).returning('id');
  return result.id;
}

export async function updateConfirmationStatus(id: number, status: string, externalId?: string) {
  await db('delivery_confirmations').where({ id }).update({ status, external_id: externalId });
}

export async function getDeliveryConfirmations(deliveryId: number) {
  return db('delivery_confirmations')
    .where({ delivery_id: deliveryId })
    .orderBy('sent_at', 'desc')
    .select('*');
}

// file: src/services/smsReplyService.ts
import db from '../config/database';
import logger from '../utils/logger';

export async function processSMSReply(data: {
  phoneNumber: string;
  message: string;
  timestamp: string;
  externalId: string;
}) {
  const confirmation = await db('delivery_confirmations')
    .where({ phone_number: data.phoneNumber, status: 'sent' })
    .orderBy('sent_at', 'desc')
    .first();

  if (!confirmation) {
    logger.warn('No matching confirmation for SMS reply', { phoneNumber: data.phoneNumber });
    return;
  }

  const parsedAction = parseAction(data.message);
  
  await db('delivery_confirmations').where({ id: confirmation.id }).update({
    status: 'replied',
    replied_at: new Date(data.timestamp),
    reply_message: data.message,
    parsed_action: parsedAction,
    reply_external_id: data.externalId,
  });

  logger.info('SMS reply processed', { confirmationId: confirmation.id, action: parsedAction });
}

function parseAction(message: string): string | null {
  const lowerMsg = message.toLowerCase().trim();
  if (lowerMsg.includes('confirm') || lowerMsg === 'sim' || lowerMsg === 'yes') return 'confirmed';
  if (lowerMsg.includes('cancel') || lowerMsg === 'nao' || lowerMsg === 'no') return 'cancelled';
  return null;
}