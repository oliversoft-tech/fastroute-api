import { Request, Response } from 'express';
import { NotificacaoEntregaRepository } from '../../repositories/notificacao-entrega.repository';
import { EventBus } from '../../domain/events/event-bus';
import { ReagendamentoSolicitadoEvent } from '../../domain/events/reagendamento-solicitado.event';
import { parseIntencaoCliente } from '../../services/parser-intencao.service';

export async function webhookSmsHandler(req: Request, res: Response) {
  try {
    const { telefone, mensagem, timestamp } = req.body;
    
    const notificacaoRepo = new NotificacaoEntregaRepository();
    const notificacao = await notificacaoRepo.findByTelefone(telefone);
    
    if (!notificacao) {
      return res.status(200).json({ received: true, processed: false });
    }

    const intencao = parseIntencaoCliente(mensagem);
    
    await notificacaoRepo.updateResposta(notificacao.id, {
      respostaTexto: mensagem,
      intencaoDetectada: intencao.tipo,
      confianca: intencao.confianca,
      timestampResposta: new Date(timestamp)
    });

    if (intencao.tipo === 'reagendar' && intencao.confianca > 0.7) {
      await EventBus.publish(new ReagendamentoSolicitadoEvent({
        entregaId: notificacao.entregaId,
        clienteTelefone: telefone,
        motivoTexto: mensagem,
        timestampSolicitacao: new Date()
      }));
    }

    return res.status(200).json({ received: true, processed: true, intencao: intencao.tipo });
  } catch (error) {
    console.error('Erro processando webhook SMS:', error);
    return res.status(200).json({ received: true, processed: false });
  }
}