import { Router } from 'express';
import { webhookSmsHandler } from '../handlers/notificacoes/webhook-sms.handler';

const router = Router();

router.post('/webhook-sms', webhookSmsHandler);

export default router;

// file: src/domain/events/reagendamento-solicitado.event.ts
export interface ReagendamentoSolicitadoPayload {
  entregaId: number;
  clienteTelefone: string;
  motivoTexto: string;
  timestampSolicitacao: Date;
}

export class ReagendamentoSolicitadoEvent {
  readonly eventName = 'entrega.reagendamento.solicitado';
  readonly payload: ReagendamentoSolicitadoPayload;
  readonly timestamp: Date;

  constructor(payload: ReagendamentoSolicitadoPayload) {
    this.payload = payload;
    this.timestamp = new Date();
  }
}

// file: src/repositories/notificacao-entrega.repository.ts
import { pool } from '../config/database';

export interface NotificacaoEntrega {
  id: number;
  entregaId: number;
  telefone: string;
  status: string;
}

export class NotificacaoEntregaRepository {
  async findByTelefone(telefone: string): Promise<NotificacaoEntrega | null> {
    const result = await pool.query(
      'SELECT * FROM notificacoes_entrega WHERE telefone = $1 ORDER BY created_at DESC LIMIT 1',
      [telefone]
    );
    return result.rows[0] || null;
  }

  async updateResposta(id: number, resposta: any): Promise<void> {
    await pool.query(
      `UPDATE notificacoes_entrega 
       SET resposta_texto = $1, intencao_detectada = $2, confianca = $3, timestamp_resposta = $4, updated_at = NOW()
       WHERE id = $5`,
      [resposta.respostaTexto, resposta.intencaoDetectada, resposta.confianca, resposta.timestampResposta, id]
    );
  }
}