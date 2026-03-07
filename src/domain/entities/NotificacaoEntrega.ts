export enum StatusNotificacao {
  Enviado = 'Enviado',
  Confirmado = 'Confirmado',
  Falhou = 'Falhou'
}

export interface NotificacaoEntrega {
  id: number;
  encomenda_id: number;
  telefone: string;
  mensagem: string;
  status: StatusNotificacao;
  data_envio: Date;
  sid_externo?: string;
  erro?: string;
}

export interface EnvioSMSResultado {
  encomenda_id: number;
  notificacao_id?: number;
  sucesso: boolean;
  erro?: string;
}