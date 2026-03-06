'use strict';
const { supabaseAdmin } = require('../lib/supabase');
const { AppError } = require('../lib/appError');

function toNormalizedObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return { ...value };
}

function toPositiveInt(value) {
  const parsed = Math.trunc(Number(value));
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

function toNonNegativeInt(value) {
  const parsed = Math.trunc(Number(value));
  if (!Number.isFinite(parsed) || parsed < 0) {
    return null;
  }
  return parsed;
}

function isMissingColumnError(error, columnName) {
  const message = String(error?.message || '').toLowerCase();
  return message.includes('column') && message.includes(columnName.toLowerCase()) && message.includes('does not exist');
}

function readVersion(row) {
  return toNonNegativeInt(row?.version ?? row?.versao ?? 0) ?? 0;
}

function pickVersionField(row) {
  if (row && Object.prototype.hasOwnProperty.call(row, 'version')) {
    return 'version';
  }
  if (row && Object.prototype.hasOwnProperty.call(row, 'versao')) {
    return 'versao';
  }
  return 'version';
}

async function insertWithVersion(tableName, payload) {
  const firstPayload = { ...payload, version: 1 };
  const { error: firstError } = await supabaseAdmin.from(tableName).insert(firstPayload);
  if (!firstError) {
    return { versionField: 'version', version: 1 };
  }

  if (!isMissingColumnError(firstError, 'version')) {
    throw firstError;
  }

  const secondPayload = { ...payload, versao: 1 };
  const { error: secondError } = await supabaseAdmin.from(tableName).insert(secondPayload);
  if (secondError) {
    throw secondError;
  }

  return { versionField: 'versao', version: 1 };
}

function compactObject(payload) {
  const out = {};
  for (const [key, value] of Object.entries(payload)) {
    if (value !== undefined) {
      out[key] = value;
    }
  }
  return out;
}

const ROUTE_TRANSIENT_FIELDS = new Set([
  'waypoint_count',
  'waypointCount',
  'waypoints_count',
  'waypointsCount',
  'waypoints',
  'stops'
]);

function sanitizeRoutePayload(rawPayload) {
  const payload = toNormalizedObject(rawPayload);
  for (const field of ROUTE_TRANSIENT_FIELDS) {
    delete payload[field];
  }
  return payload;
}

async function resolveDriverIdFromAuthUserId(authUserId) {
  const normalizedAuthUserId = String(authUserId || '').trim();
  if (!normalizedAuthUserId) {
    return null;
  }

  const { data, error } = await supabaseAdmin
    .from('users')
    .select('id')
    .eq('auth_user_id', normalizedAuthUserId)
    .order('id', { ascending: true })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new AppError(500, `Erro ao resolver motorista para sync: ${error.message}`);
  }

  return toPositiveInt(data?.id);
}

async function enrichRoutePayload(rawPayload) {
  const payload = sanitizeRoutePayload(rawPayload);
  const rawDriverId = payload.driver_id ?? payload.user_id;
  const parsedDriverId = toPositiveInt(rawDriverId);

  if (parsedDriverId) {
    payload.driver_id = parsedDriverId;
    payload.user_id = parsedDriverId;
  } else {
    const authUserId = String(payload.auth_user_id ?? '').trim();
    const resolvedDriverId = await resolveDriverIdFromAuthUserId(authUserId);
    if (resolvedDriverId) {
      payload.driver_id = resolvedDriverId;
      payload.user_id = resolvedDriverId;
    }
  }

  delete payload.auth_user_id;
  return compactObject(payload);
}

async function wasApplied(deviceId, mutationId) {
  const { data, error } = await supabaseAdmin
    .from('mutations_applied')
    .select('*')
    .eq('device_id', deviceId)
    .eq('mutation_id', mutationId)
    .maybeSingle();
  if (error) {
    throw new AppError(500, `Erro ao consultar mutações aplicadas: ${error.message}`);
  }
  return !!data;
}

async function markApplied(deviceId, mutationId) {
  const { error } = await supabaseAdmin.from('mutations_applied').insert({
    device_id: deviceId,
    mutation_id: mutationId
  });
  if (error) {
    throw new AppError(500, `Erro ao registrar mutação aplicada: ${error.message}`);
  }
}

async function logChange(entityType, entityId, op, version, payload) {
  const { error } = await supabaseAdmin.from('change_log').insert({
    entity_type: entityType,
    entity_id: entityId,
    op,
    version,
    payload
  });
  if (error) {
    throw new AppError(500, `Erro ao registrar change_log: ${error.message}`);
  }
}

async function applyMutation(m) {

  if (await wasApplied(m.deviceId, m.mutationId)) {
    return { mutationId: m.mutationId, status: 'DUPLICATE' };
  }

  if (m.entityType === 'route') {
    const routePayload = await enrichRoutePayload(m.payload);
    const { data: route, error: routeError } = await supabaseAdmin
      .from('routes')
      .select('*')
      .eq('id', m.entityId)
      .maybeSingle();

    if (routeError) {
      throw new AppError(500, `Erro ao buscar rota para sync: ${routeError.message}`);
    }

    if (!route) {
      try {
        const inserted = await insertWithVersion('routes', { id: m.entityId, ...routePayload });
        void inserted;
      } catch (insertError) {
        throw new AppError(500, `Erro ao criar rota via sync: ${insertError.message}`);
      }

      await logChange('route', m.entityId, m.op, 1, routePayload);
    } else {
      const currentVersion = readVersion(route);
      if (currentVersion !== m.baseVersion)
        return { mutationId: m.mutationId, status: 'CONFLICT', serverVersion: currentVersion };

      const nextVersion = currentVersion + 1;
      const versionField = pickVersionField(route);

      const { error: updateError } = await supabaseAdmin.from('routes')
        .update({ ...routePayload, [versionField]: nextVersion })
        .eq('id', m.entityId);
      if (updateError) {
        throw new AppError(500, `Erro ao atualizar rota via sync: ${updateError.message}`);
      }

      await logChange('route', route.id, m.op, nextVersion, routePayload);
    }
  }

  if (m.entityType === 'route_waypoint') {

    const { data: wp, error: waypointError } = await supabaseAdmin
      .from('route_waypoints')
      .select('*')
      .eq('id', m.entityId)
      .maybeSingle();

    if (waypointError) {
      throw new AppError(500, `Erro ao buscar waypoint para sync: ${waypointError.message}`);
    }

    if (!wp) {
      const op = String(m.op || '').toUpperCase();
      if (op !== 'CREATE') {
        return { mutationId: m.mutationId, status: 'NOT_FOUND' };
      }

      try {
        const inserted = await insertWithVersion('route_waypoints', { id: m.entityId, ...m.payload });
        void inserted;
      } catch (insertWaypointError) {
        throw new AppError(500, `Erro ao criar waypoint via sync: ${insertWaypointError.message}`);
      }

      await logChange('route_waypoint', m.entityId, m.op, 1, m.payload);

      await markApplied(m.deviceId, m.mutationId);
      return { mutationId: m.mutationId, status: 'APPLIED' };
    }

    const currentVersion = readVersion(wp);
    if (currentVersion !== m.baseVersion)
      return { mutationId: m.mutationId, status: 'CONFLICT', serverVersion: currentVersion };

    const nextVersion = currentVersion + 1;
    const versionField = pickVersionField(wp);

    const { error: updateWaypointError } = await supabaseAdmin.from('route_waypoints')
      .update({ ...m.payload, [versionField]: nextVersion })
      .eq('id', m.entityId);
    if (updateWaypointError) {
      throw new AppError(500, `Erro ao atualizar waypoint via sync: ${updateWaypointError.message}`);
    }

    await logChange('route_waypoint', wp.id, m.op, nextVersion, m.payload);
  }

  await markApplied(m.deviceId, m.mutationId);
  return { mutationId: m.mutationId, status: 'APPLIED' };
}

async function push(body) {
  const results = [];
  for (const m of body.mutations || []) {
    results.push(await applyMutation(m));
  }
  return { ok: true, results };
}

async function pull(sinceTs) {
  const { data, error } = await supabaseAdmin
    .from('change_log')
    .select('*')
    .gt('created_at', sinceTs || '1970-01-01')
    .order('created_at', { ascending: true });

  if (error) {
    throw new AppError(500, `Erro ao consultar change_log: ${error.message}`);
  }

  return { ok: true, changes: data };
}

module.exports = { push, pull };
