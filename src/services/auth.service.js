'use strict';

const { AppError } = require('../lib/appError');
const { supabaseAnon } = require('../lib/supabase');

async function login(payload) {
  const email = payload?.email;
  const password = payload?.password;

  if (!email || !password) {
    throw new AppError(400, 'Missing email or password');
  }

  const { data, error } = await supabaseAnon.auth.signInWithPassword({
    email: String(email),
    password: String(password)
  });

  if (error) {
    throw new AppError(401, error.message);
  }

  return {
    user: data?.user || null,
    session: data?.session || null,
    access_token: data?.session?.access_token || null,
    refresh_token: data?.session?.refresh_token || null,
    expires_in: data?.session?.expires_in || null,
    token_type: data?.session?.token_type || null
  };
}

module.exports = { login };
