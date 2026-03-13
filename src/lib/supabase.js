'use strict';
const { createClient } = require('@supabase/supabase-js');

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars');
}

function decodeJwtPayload(token) {
  const raw = String(token || '').trim();
  if (!raw || raw.startsWith('sb_secret_')) {
    return null;
  }

  const parts = raw.split('.');
  if (parts.length < 2) {
    return null;
  }

  try {
    const payloadBase64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const padding = '='.repeat((4 - (payloadBase64.length % 4)) % 4);
    const payloadJson = Buffer.from(payloadBase64 + padding, 'base64').toString('utf8');
    return JSON.parse(payloadJson);
  } catch {
    return null;
  }
}

function readServiceKeyDiagnostics(serviceKey) {
  const payload = decodeJwtPayload(serviceKey);
  if (!payload || typeof payload !== 'object') {
    return {
      keyType: String(serviceKey || '').startsWith('sb_secret_') ? 'supabase-secret' : 'opaque',
      role: null
    };
  }

  return {
    keyType: 'jwt',
    role: typeof payload.role === 'string' ? payload.role : null
  };
}

const serviceKeyDiagnostics = readServiceKeyDiagnostics(process.env.SUPABASE_SERVICE_ROLE_KEY);
if (serviceKeyDiagnostics.keyType === 'jwt' && serviceKeyDiagnostics.role && serviceKeyDiagnostics.role !== 'service_role') {
  throw new Error(
    `SUPABASE_SERVICE_ROLE_KEY inválida: role "${serviceKeyDiagnostics.role}" (esperado: "service_role").`
  );
}

const supabaseAdmin = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { persistSession: false } }
);

const supabaseAnon = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { persistSession: false } }
);

module.exports = {
  supabaseAdmin,
  supabaseAnon,
  serviceKeyDiagnostics
};
