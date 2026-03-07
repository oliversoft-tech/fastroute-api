import cron from 'node-cron';
import { sendDeliveryNotifications } from '../services/smsService';
import { logger } from '../utils/logger';

export function scheduleDailySmsJob() {
  cron.schedule('0 18 * * *', async () => {
    logger.info('Starting daily SMS notification job');
    
    try {
      const results = await sendDeliveryNotifications();
      const sent = results.filter(r => r.sent).length;
      const failed = results.filter(r => !r.sent).length;
      
      logger.info(`SMS job completed: ${sent} sent, ${failed} failed`);
    } catch (error: any) {
      logger.error('SMS job failed:', error);
    }
  });

  logger.info('Daily SMS job scheduled at 18:00');
}