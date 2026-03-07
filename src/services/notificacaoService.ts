import { pool } from '../config/database';
import { StatusNotificacao, EnvioSMSResultado } from '../domain/entities/NotificacaoEntrega';
import { Twilio } from 'twilio';

const twilioClient = new Twilio(
  process.env.TWILIO_ACCOUNT_SID || '',
  process.env.TWILIO_AUTH_TOKEN || ''
);

const TWILIO_FROM = process.env.TWILIO_PHONE_NUMBER || '';

export async function enviarNotificacoesSMS(): Promise<EnvioSMSResultado[]> {
  const client = await pool.connect();
  const resultados: EnvioSMSResultado[] = [];

  try {
    const query = `
      SELECT e.id, e.numero_encomenda, e.data_entrega_prevista, 
             c.telefone, c.nome
      FROM encomendas e
      JOIN clientes c ON e.cliente_id = c.id
      WHERE e.data_entrega_prevista = CURRENT_DATE + INTERVAL '1 day'
        AND e.status = 'Planejado'
        AND NOT EXISTS (
          SELECT 1 FROM notificacoes_entrega n
          WHERE n.encomenda_id = e.id
            AND n.status IN ('Enviado', 'Confirmado')
        )
    `;
    
    const { rows } = await client.query(query);

    for (const encomenda of rows) {
      try {
        const dataFormatada = new Date(encomenda.data_entrega_prevista)
          .toLocaleDateString('pt-BR');
        
        const mensagem = `Ola ${encomenda.nome}! Sua encomenda ${encomenda.numero_encomenda} esta programada para entrega em ${dataFormatada}. Responda SIM para confirmar presenca ou NAO para reagendar.`;

        const message = await twilioClient.messages.create({
          body: mensagem,
          from: TWILIO_FROM,
          to: encomenda.telefone
        });

        const insertResult = await client.query(
          `INSERT INTO notificacoes_entrega 
           (encomenda_id, telefone, mensagem, status, data_envio, sid_externo)
           VALUES ($1, $2, $3, $4, NOW(), $5)
           RETURNING id`,
          [encomenda.id, encomenda.telefone, mensagem, StatusNotificacao.Enviado, message.sid]
        );

        resultados.push({
          encomenda_id: encomenda.id,
          notificacao_id: insertResult.rows[0].id,
          sucesso: true
        });
      } catch (erro: any) {
        await client.query(
          `INSERT INTO notificacoes_entrega 
           (encomenda_id, telefone, mensagem, status, data_envio, erro)
           VALUES ($1, $2, $3, $4, NOW(), $5)`,
          [encomenda.id, encomenda.telefone, '', StatusNotificacao.Falhou, erro.message]
        );

        resultados.push({
          encomenda_id: encomenda.id,
          sucesso: false,
          erro: erro.message
        });
      }
    }

    return resultados;
  } finally {
    client.release();
  }
}