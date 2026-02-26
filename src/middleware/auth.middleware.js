'use strict';

const { normalizeJwtProviderResult, validateJwtInput } = require('../lib/domain');
const { supabaseAnon } = require('../lib/supabase');

async function requireAuth(req, res, next) {
  try {
    const validated = validateJwtInput({ authorization: req.headers.authorization });
    if (!validated.ok) {
      return res.status(401).json({
        ok: false,
        error: validated.error,
        code: validated.code
      });
    }

    const authHeader = validated.value.token;
    const accessToken = authHeader.toLowerCase().startsWith('bearer ')
      ? authHeader.slice(7).trim()
      : authHeader.trim();

    const { data, error } = await supabaseAnon.auth.getUser(accessToken);
    const normalized = normalizeJwtProviderResult({
      id: data?.user?.id,
      error: error?.message
    });

    if (!normalized.ok) {
      return res.status(401).json({
        ok: false,
        error: normalized.error,
        code: normalized.code
      });
    }

    req.auth = {
      userId: normalized.value.userId,
      accessToken
    };

    next();
  } catch (err) {
    next(err);
  }
}

module.exports = { requireAuth };
