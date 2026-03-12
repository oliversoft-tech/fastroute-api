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

function readMissingColumnName(error) {
  const normalizeColumn = (value) => {
    const normalized = String(value || '')
      .trim()
      .replace(/["']/g, '');
    if (!normalized) {
      return null;
    }
    const parts = normalized.split('.').filter(Boolean);
    return parts[parts.length - 1] || null;
  };

  const message = String(error?.message || '');
  const schemaCacheMatch = message.match(/Could not find the '([^']+)' column/i);
  if (schemaCacheMatch && schemaCacheMatch[1]) {
    return normalizeColumn(schemaCacheMatch[1]);
  }
  const postgresMatch = message.match(/column "([^"]+)" does not exist/i);
  if (postgresMatch && postgresMatch[1]) {
    return normalizeColumn(postgresMatch[1]);
  }
  const postgresUnquotedMatch = message.match(/column\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s+does not exist/i);
  if (postgresUnquotedMatch && postgresUnquotedMatch[1]) {
    return normalizeColumn(postgresUnquotedMatch[1]);
  }
  const qualifiedUnquotedMatch = message.match(/\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s+does not exist/i);
  if (qualifiedUnquotedMatch && qualifiedUnquotedMatch[2]) {
    return normalizeColumn(qualifiedUnquotedMatch[2]);
  }
  return null;
}

function isMissingTableError(error, tableName) {
  const message = String(error?.message || '').toLowerCase();
  return message.includes('could not find the table') && message.includes(`'public.${String(tableName || '').toLowerCase()}'`);
}

function isRlsPolicyError(error) {
  const message = String(error?.message || '').toLowerCase();
  return message.includes('row-level security policy');
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

const importIdMappingByDeviceAndDriver = new Map();

function buildImportMappingKey(deviceId, driverId, rawImportId) {
  const normalizedDeviceId = String(deviceId || 'unknown-device').trim() || 'unknown-device';
  const normalizedDriverId = String(toPositiveInt(driverId) || 'unknown-driver');
  const normalizedImportId = String(toPositiveInt(rawImportId) || 'missing-import-id');
  return `${normalizedDeviceId}:${normalizedDriverId}:${normalizedImportId}`;
}

async function findExistingImportId(importId) {
  const parsedImportId = toPositiveInt(importId);
  if (!parsedImportId) {
    return null;
  }

  for (const tableName of ['orders_import', 'imports']) {
    const { data, error } = await supabaseAdmin
      .from(tableName)
      .select('id')
      .eq('id', parsedImportId)
      .maybeSingle();

    if (!error && toPositiveInt(data?.id)) {
      return toPositiveInt(data.id);
    }

    if (error && !isMissingTableError(error, tableName)) {
      throw error;
    }
  }

  return null;
}

async function createImportIdForDriver(driverId) {
  const parsedDriverId = toPositiveInt(driverId);
  if (!parsedDriverId) {
    return null;
  }

  const attempts = [
    { table: 'orders_import', payload: { user_id: parsedDriverId } },
    { table: 'imports', payload: { user_id: parsedDriverId, file_name: 'sync-import' } },
    { table: 'imports', payload: { user_id: parsedDriverId, filename: 'sync-import' } }
  ];

  let lastError = null;
  for (const attempt of attempts) {
    const { data, error } = await supabaseAdmin.from(attempt.table).insert(attempt.payload).select('id').single();
    if (!error && toPositiveInt(data?.id)) {
      return toPositiveInt(data.id);
    }
    if (error && !isMissingTableError(error, attempt.table)) {
      lastError = error;
    }
  }

  if (lastError) {
    throw lastError;
  }

  return null;
}

async function ensureRouteImportId(routePayload, mutation) {
  const payload = toNormalizedObject(routePayload);
  const driverId =
    toPositiveInt(payload.driver_id) ??
    toPositiveInt(mutation?.payload?.driver_id) ??
    toPositiveInt(mutation?.payload?.driverId) ??
    toPositiveInt(mutation?.payload?.user_id) ??
    toPositiveInt(mutation?.payload?.userId);

  if (!driverId) {
    return null;
  }

  const requestedImportId =
    toPositiveInt(payload.import_id) ??
    toPositiveInt(mutation?.payload?.import_id) ??
    toPositiveInt(mutation?.payload?.importId);

  const mappingKey = buildImportMappingKey(mutation?.deviceId, driverId, requestedImportId);
  const mappedImportId = toPositiveInt(importIdMappingByDeviceAndDriver.get(mappingKey));
  if (mappedImportId) {
    return mappedImportId;
  }

  const existingImportId = await findExistingImportId(requestedImportId);
  if (existingImportId) {
    importIdMappingByDeviceAndDriver.set(mappingKey, existingImportId);
    return existingImportId;
  }

  const createdImportId = await createImportIdForDriver(driverId);
  if (!createdImportId) {
    return null;
  }

  importIdMappingByDeviceAndDriver.set(mappingKey, createdImportId);
  return createdImportId;
}

function sanitizeRoutePayload(rawPayload) {
  const payload = toNormalizedObject(rawPayload);
  for (const field of ROUTE_TRANSIENT_FIELDS) {
    delete payload[field];
  }
  return payload;
}

const WAYPOINT_TRANSIENT_FIELDS = new Set([
  'detailed_address',
  'lat',
  'long',
  'latitude',
  'longitude',
  'title',
  'subtitle'
]);

function sanitizeWaypointPayload(rawPayload) {
  const payload = toNormalizedObject(rawPayload);
  for (const field of WAYPOINT_TRANSIENT_FIELDS) {
    delete payload[field];
  }
  return payload;
}

async function insertWaypointWithCompatiblePayload(entityId, rawPayload) {
  const payload = compactObject(sanitizeWaypointPayload(rawPayload));
  let lastError = null;

  for (let attempt = 0; attempt < 8; attempt += 1) {
    try {
      await insertWithVersion('route_waypoints', { id: entityId, ...payload });
      return payload;
    } catch (error) {
      const missingColumn = readMissingColumnName(error);
      if (!missingColumn || !Object.prototype.hasOwnProperty.call(payload, missingColumn)) {
        throw error;
      }
      delete payload[missingColumn];
      lastError = error;
    }
  }

  if (lastError) {
    throw lastError;
  }

  return payload;
}

async function updateWaypointWithCompatiblePayload(entityId, payload, versionField, nextVersion) {
  const updatePayload = compactObject(sanitizeWaypointPayload(payload));
  let lastError = null;

  for (let attempt = 0; attempt < 8; attempt += 1) {
    const { error } = await supabaseAdmin
      .from('route_waypoints')
      .update({ ...updatePayload, [versionField]: nextVersion })
      .eq('id', entityId);

    if (!error) {
      return updatePayload;
    }

    const missingColumn = readMissingColumnName(error);
    if (!missingColumn || !Object.prototype.hasOwnProperty.call(updatePayload, missingColumn)) {
      throw error;
    }

    delete updatePayload[missingColumn];
    lastError = error;
  }

  if (lastError) {
    throw lastError;
  }

  return updatePayload;
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

async function resolveDriverIdFromImportId(importId) {
  const parsedImportId = toPositiveInt(importId);
  if (!parsedImportId) {
    return null;
  }

  const tables = ['orders_import', 'imports'];

  for (const tableName of tables) {
    for (const columnName of ['user_id', 'driver_id']) {
      const { data, error } = await supabaseAdmin
        .from(tableName)
        .select(columnName)
        .eq('id', parsedImportId)
        .maybeSingle();

      if (!error) {
        const resolvedDriverId = toPositiveInt(data?.[columnName]);
        if (resolvedDriverId) {
          return resolvedDriverId;
        }
        continue;
      }

      if (isMissingTableError(error, tableName)) {
        break;
      }

      const missingColumn = readMissingColumnName(error);
      if (missingColumn === columnName) {
        continue;
      }

      throw new AppError(500, `Erro ao resolver motorista via import_id: ${error.message}`);
    }
  }

  return null;
}

async function enrichRoutePayload(rawPayload) {
  const payload = sanitizeRoutePayload(rawPayload);
  const rawDriverId = payload.driver_id ?? payload.user_id;
  const parsedDriverId = toPositiveInt(rawDriverId);
  const authUserId = String(payload.auth_user_id ?? '').trim();
  const parsedDriverIdFromAuthUserId = toPositiveInt(authUserId);

  if (parsedDriverId) {
    payload.driver_id = parsedDriverId;
    payload.user_id = parsedDriverId;
  } else if (parsedDriverIdFromAuthUserId) {
    // Backward compatibility: some clients persisted driver id in auth_user_id.
    payload.driver_id = parsedDriverIdFromAuthUserId;
    payload.user_id = parsedDriverIdFromAuthUserId;
  } else {
    const resolvedDriverId = await resolveDriverIdFromAuthUserId(authUserId);
    if (resolvedDriverId) {
      payload.driver_id = resolvedDriverId;
      payload.user_id = resolvedDriverId;
    }
  }

  delete payload.auth_user_id;
  delete payload.user_id;
  return compactObject(payload);
}

async function buildRouteCreatePayload(routePayload, mutation) {
  const payload = compactObject(toNormalizedObject(routePayload));
  const routeIdForHints =
    toPositiveInt(payload.id) ??
    toPositiveInt(payload.route_id) ??
    toPositiveInt(mutation?.entityId);

  if (!toPositiveInt(payload.import_id)) {
    const fallbackImportId =
      toPositiveInt(mutation?.payload?.import_id) ??
      toPositiveInt(mutation?.payload?.importId) ??
      toPositiveInt(payload.id) ??
      toPositiveInt(mutation?.entityId);
    if (fallbackImportId) {
      payload.import_id = fallbackImportId;
    }
  }

  if (!toPositiveInt(payload.driver_id)) {
    const fallbackDriverId =
      toPositiveInt(payload.user_id) ??
      toPositiveInt(mutation?.context?.routeDriverHintsByRouteId?.get(routeIdForHints)) ??
      toPositiveInt(mutation?.payload?.driver_id) ??
      toPositiveInt(mutation?.payload?.driverId) ??
      toPositiveInt(mutation?.payload?.user_id) ??
      toPositiveInt(mutation?.payload?.userId) ??
      toPositiveInt(mutation?.payload?.auth_user_id) ??
      toPositiveInt(mutation?.payload?.authUserId);

    if (fallbackDriverId) {
      payload.driver_id = fallbackDriverId;
    }
  }

  if (!toPositiveInt(payload.driver_id)) {
    const importIdForDriverResolution =
      toPositiveInt(payload.import_id) ??
      toPositiveInt(mutation?.payload?.import_id) ??
      toPositiveInt(mutation?.payload?.importId);
    const driverIdFromImport = await resolveDriverIdFromImportId(importIdForDriverResolution);
    if (driverIdFromImport) {
      payload.driver_id = driverIdFromImport;
    }
  }

  if (!String(payload.status ?? '').trim()) {
    payload.status = 'CRIADA';
  }

  if (!toPositiveInt(payload.cluster_id)) {
    const fallbackClusterId =
      toPositiveInt(mutation?.payload?.cluster_id) ??
      toPositiveInt(mutation?.payload?.clusterId) ??
      1;
    payload.cluster_id = fallbackClusterId;
  }

  if (payload.ativa === undefined || payload.ativa === null) {
    payload.ativa = false;
  }

  if (!payload.created_at) {
    payload.created_at = new Date().toISOString();
  }

  const missingColumns = [];
  if (!toPositiveInt(payload.import_id)) {
    missingColumns.push('import_id');
  }
  if (!toPositiveInt(payload.driver_id)) {
    missingColumns.push('driver_id');
  }
  if (!toPositiveInt(payload.cluster_id)) {
    missingColumns.push('cluster_id');
  }

  if (missingColumns.length > 0) {
    throw new AppError(
      400,
      `Payload de rota incompleto para sync CREATE: colunas obrigatórias ausentes (${missingColumns.join(', ')}).`,
      {
        mutationId: mutation?.mutationId ?? null,
        entityId: mutation?.entityId ?? null,
        payloadKeys: Object.keys(payload).sort()
      }
    );
  }

  delete payload.user_id;
  return payload;
}

async function wasApplied(deviceId, mutationId) {
  const { data, error } = await supabaseAdmin
    .from('mutations_applied')
    .select('*')
    .eq('device_id', deviceId)
    .eq('mutation_id', mutationId)
    .maybeSingle();
  if (error) {
    if (isRlsPolicyError(error)) {
      return false;
    }
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
    if (isRlsPolicyError(error)) {
      return;
    }
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
    if (isRlsPolicyError(error)) {
      return;
    }
    throw new AppError(500, `Erro ao registrar change_log: ${error.message}`);
  }
}

async function applyMutation(m) {

  if (await wasApplied(m.deviceId, m.mutationId)) {
    return { mutationId: m.mutationId, status: 'DUPLICATE' };
  }

  if (m.entityType === 'route') {
    let routePayload = await enrichRoutePayload(m.payload);
    const routeOperation = String(m.op || '').toUpperCase();
    const { data: route, error: routeError } = await supabaseAdmin
      .from('routes')
      .select('*')
      .eq('id', m.entityId)
      .maybeSingle();

    if (routeError) {
      throw new AppError(500, `Erro ao buscar rota para sync: ${routeError.message}`);
    }

    if (!route) {
      const routeCreatePayload = await buildRouteCreatePayload(routePayload, m);
      const ensuredImportId = await ensureRouteImportId(routeCreatePayload, m);
      if (ensuredImportId) {
        routeCreatePayload.import_id = ensuredImportId;
      }

      try {
        const inserted = await insertWithVersion('routes', { id: m.entityId, ...routeCreatePayload });
        void inserted;
      } catch (insertError) {
        throw new AppError(500, `Erro ao criar rota via sync: ${insertError.message}`);
      }

      await logChange('route', m.entityId, m.op, 1, routeCreatePayload);
    } else {
      if (routeOperation === 'CREATE') {
        await markApplied(m.deviceId, m.mutationId);
        return { mutationId: m.mutationId, status: 'APPLIED' };
      }

      const currentVersion = readVersion(route);
      if (currentVersion !== m.baseVersion)
        return { mutationId: m.mutationId, status: 'CONFLICT', serverVersion: currentVersion };

      const nextVersion = currentVersion + 1;
      const versionField = pickVersionField(route);
      const routeUpdatePayload = compactObject({ ...routePayload });
      delete routeUpdatePayload.import_id;

      const { error: updateError } = await supabaseAdmin.from('routes')
        .update({ ...routeUpdatePayload, [versionField]: nextVersion })
        .eq('id', m.entityId);
      if (updateError) {
        throw new AppError(500, `Erro ao atualizar rota via sync: ${updateError.message}`);
      }

      await logChange('route', route.id, m.op, nextVersion, routeUpdatePayload);
    }
  }

  if (m.entityType === 'route_waypoint') {
    const waypointOperation = String(m.op || '').toUpperCase();
    const waypointChangePayload = compactObject(toNormalizedObject(m.payload));
    const waypointPayload = compactObject(sanitizeWaypointPayload(m.payload));

    const { data: wp, error: waypointError } = await supabaseAdmin
      .from('route_waypoints')
      .select('*')
      .eq('id', m.entityId)
      .maybeSingle();

    if (waypointError) {
      throw new AppError(500, `Erro ao buscar waypoint para sync: ${waypointError.message}`);
    }

    if (!wp) {
      if (waypointOperation !== 'CREATE') {
        return { mutationId: m.mutationId, status: 'NOT_FOUND' };
      }

      try {
        await insertWaypointWithCompatiblePayload(m.entityId, waypointPayload);
      } catch (insertWaypointError) {
        throw new AppError(500, `Erro ao criar waypoint via sync: ${insertWaypointError.message}`);
      }

      await logChange('route_waypoint', m.entityId, m.op, 1, waypointChangePayload);

      await markApplied(m.deviceId, m.mutationId);
      return { mutationId: m.mutationId, status: 'APPLIED' };
    }

    if (waypointOperation === 'CREATE') {
      await markApplied(m.deviceId, m.mutationId);
      return { mutationId: m.mutationId, status: 'APPLIED' };
    }

    const currentVersion = readVersion(wp);
    if (currentVersion !== m.baseVersion)
      return { mutationId: m.mutationId, status: 'CONFLICT', serverVersion: currentVersion };

    const nextVersion = currentVersion + 1;
    const versionField = pickVersionField(wp);

    try {
      await updateWaypointWithCompatiblePayload(m.entityId, waypointPayload, versionField, nextVersion);
    } catch (updateWaypointError) {
      throw new AppError(500, `Erro ao atualizar waypoint via sync: ${updateWaypointError.message}`);
    }

    await logChange('route_waypoint', wp.id, m.op, nextVersion, waypointChangePayload);
  }

  await markApplied(m.deviceId, m.mutationId);
  return { mutationId: m.mutationId, status: 'APPLIED' };
}

async function push(body) {
  const routeDriverHintsByRouteId = new Map();
  for (const mutation of body?.mutations || []) {
    const entityType = String(mutation?.entityType || '').toLowerCase();
    const mutationOp = String(mutation?.op || '').toUpperCase();
    if (entityType !== 'route_waypoint' || mutationOp !== 'CREATE') {
      continue;
    }

    const mutationPayload = toNormalizedObject(mutation?.payload);
    const routeId =
      toPositiveInt(mutationPayload.route_id) ??
      toPositiveInt(mutationPayload.routeId);
    const driverId =
      toPositiveInt(mutationPayload.user_id) ??
      toPositiveInt(mutationPayload.userId) ??
      toPositiveInt(mutationPayload.driver_id) ??
      toPositiveInt(mutationPayload.driverId);

    if (!routeId || !driverId || routeDriverHintsByRouteId.has(routeId)) {
      continue;
    }
    routeDriverHintsByRouteId.set(routeId, driverId);
  }

  const results = [];
  for (const m of body.mutations || []) {
    results.push(
      await applyMutation({
        ...m,
        context: {
          routeDriverHintsByRouteId
        }
      })
    );
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
    if (isRlsPolicyError(error)) {
      return { ok: true, changes: [] };
    }
    throw new AppError(500, `Erro ao consultar change_log: ${error.message}`);
  }

  return { ok: true, changes: data };
}

module.exports = { push, pull };
