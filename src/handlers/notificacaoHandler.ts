import { Request, Response } from 'express';
import { enviarNotificacoesSMS } from '../services/notificacaoService';

export async function enviarSMSHandler(req: Request, res: Response): Promise<void> {
  try {
    const resultados = await enviarNotificacoesSMS();
    
    const sucessos = resultados.filter(r => r.sucesso);
    const falhas = resultados.filter(r => !r.sucesso);

    res.status(201).json({
      total: resultados.length,
      enviados: sucessos.length,
      falhas: falhas.length,
      notificacoes_criadas: sucessos.map(r => r.notificacao_id),
      erros: falhas.map(r => ({
        encomenda_id: r.encomenda_id,
        erro: r.erro
      }))
    });
  } catch (error: any) {
    res.status(500).json({
      erro: 'Erro ao processar envio de notificações',
      detalhes: error.message
    });
  }
}

// file: src/routes/notificacaoRoutes.ts
import { Router } from 'express';
import { enviarSMSHandler } from '../handlers/notificacaoHandler';
import { authMiddleware } from '../middlewares/authMiddleware';

const router = Router();

router.post('/notificacoes/enviar-sms', authMiddleware, enviarSMSHandler);

export default router;