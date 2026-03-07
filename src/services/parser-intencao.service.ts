export type IntencaoTipo = 'confirmado' | 'reagendar' | 'endereco' | 'desconhecido';

export interface IntencaoDetectada {
  tipo: IntencaoTipo;
  confianca: number;
  detalhes?: string;
}

const PATTERNS = {
  confirmado: [
    /\b(sim|confirmo|confirmado|ok|certo|perfeito|blz|beleza|pode vir|pode trazer)\b/i,
    /\b(est[aáã]rei|vou estar|estarei|aguardo|esperando)\b/i
  ],
  reagendar: [
    /\b(n[aã]o|n consegui|imprevisto|problema|reagendar|outro dia|outra hora|mudar)\b/i,
    /\b(ausente|viagem|hospital|compromisso|reunião|ocupad[oa])\b/i,
    /\b(pode ser|melhor|prefiro|amanhã|depois|próxim[oa])\b/i
  ],
  endereco: [
    /\b(endere[çc]o|local|lugar|onde|rua|avenida|n[uú]mero|apto|apartamento|casa)\b/i,
    /\b(errado|incorreto|mudei|novo endere[çc]o|outro endere[çc]o)\b/i
  ]
};

export function parseIntencaoCliente(mensagem: string): IntencaoDetectada {
  const texto = mensagem.toLowerCase().trim();
  const scores: Record<IntencaoTipo, number> = {
    confirmado: 0,
    reagendar: 0,
    endereco: 0,
    desconhecido: 0
  };

  for (const [intencao, patterns] of Object.entries(PATTERNS)) {
    for (const pattern of patterns) {
      if (pattern.test(texto)) {
        scores[intencao as IntencaoTipo] += 1;
      }
    }
  }

  const maxScore = Math.max(scores.confirmado, scores.reagendar, scores.endereco);
  
  if (maxScore === 0) {
    return { tipo: 'desconhecido', confianca: 0 };
  }

  const tipoDetectado = (Object.keys(scores) as IntencaoTipo[]).find(
    k => scores[k] === maxScore
  ) || 'desconhecido';

  const totalPatterns = PATTERNS[tipoDetectado]?.length || 1;
  const confianca = Math.min(maxScore / totalPatterns, 1.0);

  return {
    tipo: tipoDetectado,
    confianca,
    detalhes: texto.substring(0, 100)
  };
}