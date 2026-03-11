'use strict';

const { createClient } = require('@supabase/supabase-js');
const { z } = require('zod');
const {
  canFinishRoute,
  dbscanDefaults,
  generateWaypointsForCluster,
  groupByClusterId,
  validateDbscanPoints
} = require('../lib/domain');
const { AppError } = require('../lib/appError');
const { supabaseAdmin } = require('../lib/supabase');

function isRelationMissing(error) {
  const message = String(error?.message || '').toLowerCase();
  return message.includes('does not exist') || message.includes('could not find');
}

function isMissingTableError(error, tableName) {
  const message = String(error?.message || '').toLowerCase();
  return message.includes('could not find the table') && message.includes(`'public.${String(tableName || '').toLowerCase()}'`);
}

function toPositiveInt(value, fieldName) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new AppError(400, `${fieldName} inválido.`);
  }
  return Math.trunc(parsed);
}

function pickNonEmptyString(...values) {
  for (const value of values) {
    if (value === null || value === undefined) {
      continue;
    }
    const text = String(value).trim();
    if (text.length > 0) {
      return text;
    }
  }
  return null;
}

function toNullableNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toOptionalPositiveInt(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  return Math.trunc(parsed);
}

function readDetailedAddress(row = {}) {
  return pickNonEmptyString(
    row.detailed_address,
    row.detailedAddress,
    row['Detailed address'],
    row['detailed address'],
    row['Detailed Address'],
    row['Detailed address '],
    row.address,
    row.full_address,
    row.fullAddress,
    row['full address'],
    row['Full address'],
    row['Receiver to Street'],
    row.receiver_to_street
  );
}

function readZipCode(row = {}) {
  return pickNonEmptyString(
    row.zipcode,
    row.zip_code,
    row['Zip Code'],
    row['zip code'],
    row.cep
  );
}

function readCity(row = {}) {
  return pickNonEmptyString(
    row.city,
    row['The destination city'],
    row['destination city'],
    row['Receiver to City'],
    row['receiver to city']
  );
}

function readLatitude(row = {}) {
  return toNullableNumber(
    row.lat ??
      row.latitude ??
      row['Receiver to Latitude'] ??
      row.receiver_to_latitude
  );
}

function readLongitude(row = {}) {
  return toNullableNumber(
    row.long ??
      row.lng ??
      row.longitude ??
      row['Receiver to Longitude'] ??
      row.receiver_to_longitude
  );
}

function buildUserScopedSupabaseClient(accessToken) {
  const token = String(accessToken || '').trim();
  if (!token || !process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
    return null;
  }

  return createClient(process.env.SUPABASE_URL, process.env.SUPABASE_ANON_KEY, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
      detectSessionInUrl: false
    },
    global: {
      headers: {
        Authorization: `Bearer ${token}`
      }
    }
  });
}

function setAddressDetails(detailsById, key, candidate) {
  if (!Number.isFinite(key) || key <= 0) {
    return;
  }

  const normalizedKey = Math.trunc(key);
  const nextDetails = {
    detailed_address: candidate?.detailed_address ?? null,
    zipcode: candidate?.zipcode ?? null,
    city: candidate?.city ?? null,
    lat: candidate?.lat ?? null,
    long: candidate?.long ?? null
  };

  const current = detailsById.get(normalizedKey);
  const currentHasDetailed = Boolean(pickNonEmptyString(current?.detailed_address));
  const nextHasDetailed = Boolean(pickNonEmptyString(nextDetails.detailed_address));
  if (!current || (!currentHasDetailed && nextHasDetailed)) {
    detailsById.set(normalizedKey, nextDetails);
  }
}

async function loadAddressDetailsByIds(addressIds, options = {}) {
  if (!addressIds.length) {
    return new Map();
  }

  const normalizedIds = [...new Set(
    addressIds
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value) && value > 0)
      .map((value) => Math.trunc(value))
  )];

  if (!normalizedIds.length) {
    return new Map();
  }

  const detailsById = new Map();
  const normalizedTextIds = normalizedIds.map((value) => String(value));
  const scopedClient = buildUserScopedSupabaseClient(options.accessToken);
  const readers = scopedClient ? [scopedClient, supabaseAdmin] : [supabaseAdmin];
  const importId = toOptionalPositiveInt(options.importId);

  for (const reader of readers) {
    let firstCriticalError = null;

    const ordersIdQuery = reader.from('orders').select('*').in('id', normalizedIds);
    const { data: ordersByIdRows, error: ordersByIdError } = importId
      ? await ordersIdQuery.eq('import_id', importId)
      : await ordersIdQuery;

    if (ordersByIdError) {
      if (!isMissingTableError(ordersByIdError, 'orders') && !isRelationMissing(ordersByIdError)) {
        firstCriticalError = ordersByIdError;
      }
    } else {
      for (const row of ordersByIdRows || []) {
        const id = Number(row.id);
        setAddressDetails(detailsById, id, {
          detailed_address: readDetailedAddress(row),
          zipcode: readZipCode(row),
          city: readCity(row),
          lat: readLatitude(row),
          long: readLongitude(row)
        });
      }
    }

    const ordersWaybillQuery = reader
      .from('orders')
      .select('*')
      .in('Waybill Number', normalizedTextIds);
    const { data: ordersByWaybillRows, error: ordersByWaybillError } = importId
      ? await ordersWaybillQuery.eq('import_id', importId)
      : await ordersWaybillQuery;

    if (ordersByWaybillError) {
      if (
        !firstCriticalError &&
        !isMissingTableError(ordersByWaybillError, 'orders') &&
        !isRelationMissing(ordersByWaybillError)
      ) {
        firstCriticalError = ordersByWaybillError;
      }
    } else {
      for (const row of ordersByWaybillRows || []) {
        const waybillAsInt = toOptionalPositiveInt(row['Waybill Number'] ?? row.waybill_number ?? row.waybillNumber);
        setAddressDetails(detailsById, waybillAsInt || 0, {
          detailed_address: readDetailedAddress(row),
          zipcode: readZipCode(row),
          city: readCity(row),
          lat: readLatitude(row),
          long: readLongitude(row)
        });
      }
    }

    const addressesByIdQuery = reader.from('addresses').select('*').in('id', normalizedIds);
    const { data: addressesByIdRows, error: addressesByIdError } = await addressesByIdQuery;

    if (addressesByIdError) {
      if (
        !firstCriticalError &&
        !isMissingTableError(addressesByIdError, 'addresses') &&
        !isRelationMissing(addressesByIdError)
      ) {
        firstCriticalError = addressesByIdError;
      }
    } else {
      for (const row of addressesByIdRows || []) {
        const id = Number(row.id);
        const orderId = Number(row.order_id);
        const details = {
          detailed_address: readDetailedAddress(row),
          zipcode: readZipCode(row),
          city: readCity(row),
          lat: readLatitude(row),
          long: readLongitude(row)
        };
        setAddressDetails(detailsById, id, details);
        setAddressDetails(detailsById, orderId, details);
      }
    }

    const addressesByOrderIdQuery = reader.from('addresses').select('*').in('order_id', normalizedIds);
    const { data: addressesByOrderRows, error: addressesByOrderError } = await addressesByOrderIdQuery;
    if (addressesByOrderError) {
      if (
        !firstCriticalError &&
        !isMissingTableError(addressesByOrderError, 'addresses') &&
        !isRelationMissing(addressesByOrderError)
      ) {
        firstCriticalError = addressesByOrderError;
      }
    } else {
      for (const row of addressesByOrderRows || []) {
        const id = Number(row.id);
        const orderId = Number(row.order_id);
        const details = {
          detailed_address: readDetailedAddress(row),
          zipcode: readZipCode(row),
          city: readCity(row),
          lat: readLatitude(row),
          long: readLongitude(row)
        };
        setAddressDetails(detailsById, id, details);
        setAddressDetails(detailsById, orderId, details);
      }
    }

    if (detailsById.size >= normalizedIds.length) {
      return detailsById;
    }

    if (firstCriticalError && reader === readers[readers.length - 1]) {
      throw new AppError(500, `Erro ao buscar endereços da rota: ${firstCriticalError.message}`);
    }
  }

  return detailsById;
}

async function hydrateRowsWithAddressDetails(rows, options = {}) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return rows;
  }

  const missingAddressIds = rows
    .filter((row) => {
      const waypointId = row.waypoint_id ?? row.id;
      if (waypointId === null || waypointId === undefined) {
        return false;
      }
      return !readDetailedAddress(row);
    })
    .map((row) => Number(row.address_id))
    .filter((value) => Number.isFinite(value) && value > 0)
    .map((value) => Math.trunc(value));

  if (missingAddressIds.length === 0) {
    return rows;
  }

  const detailsById = await loadAddressDetailsByIds(missingAddressIds, options);
  if (detailsById.size === 0) {
    return rows;
  }

  return rows.map((row) => {
    const waypointId = row.waypoint_id ?? row.id;
    if (waypointId === null || waypointId === undefined) {
      return row;
    }
    if (readDetailedAddress(row)) {
      return row;
    }
    const addressId = Number(row.address_id);
    if (!Number.isFinite(addressId) || addressId <= 0) {
      return row;
    }
    const details = detailsById.get(Math.trunc(addressId));
    if (!details) {
      return row;
    }

    return {
      ...row,
      detailed_address: details.detailed_address,
      zipcode: details.zipcode,
      city: details.city,
      lat: details.lat,
      long: details.long
    };
  });
}

async function getDriverByAuthUserId(authUserId) {
  const { data, error } = await supabaseAdmin
    .from('users')
    .select('*')
    .eq('auth_user_id', authUserId)
    .maybeSingle();

  if (error) {
    throw new AppError(500, `Erro ao buscar usuário motorista: ${error.message}`);
  }

  if (!data) {
    throw new AppError(401, 'Usuario nao encontrado');
  }

  return data;
}

function formatRoutesResponse(rows, driverId) {
  const routesMap = new Map();
  let importId = null;

  for (const row of rows) {
    const routeId = Number(row.route_id ?? row.id);
    if (!Number.isFinite(routeId)) {
      continue;
    }

    if (importId === null && row.import_id !== undefined && row.import_id !== null) {
      importId = row.import_id;
    }

    if (!routesMap.has(routeId)) {
      routesMap.set(routeId, {
        route_id: routeId,
        cluster_id: row.cluster_id ?? null,
        status: row.route_status ?? row.status ?? null,
        ativa: row.ativa ?? null,
        versao: row.versao ?? null,
        planejada_para: row.planejada_para ?? null,
        stops: []
      });
    }

    const route = routesMap.get(routeId);
    const waypointId = row.waypoint_id ?? row.id ?? null;

    if (waypointId === null || waypointId === undefined) {
      continue;
    }

    route.stops.push({
      waypoint_id: waypointId,
      address_id: row.address_id ?? null,
      seq_order: row.seq_order ?? null,
      status: row.waypoint_status ?? row.status ?? null,
      detailed_address: readDetailedAddress(row),
      zipcode: readZipCode(row),
      city: readCity(row),
      lat: readLatitude(row),
      long: readLongitude(row)
    });
  }

  const routes = Array.from(routesMap.values()).map((route) => {
    route.stops.sort((a, b) => Number(a.seq_order || 0) - Number(b.seq_order || 0));
    return route;
  });

  return {
    ok: true,
    import_id: importId,
    user_id: driverId,
    routes
  };
}

async function getRoutesFromView({ driverId, routeId, importId }) {
  let query = supabaseAdmin
    .from('v_routes_full')
    .select('*')
    .eq('driver_id', driverId)
    .order('route_id', { ascending: true })
    .order('seq_order', { ascending: true });

  if (routeId) {
    query = query.eq('route_id', routeId);
  }

  if (importId) {
    query = query.eq('import_id', importId);
  }

  const { data, error } = await query;
  if (error) {
    if (isRelationMissing(error)) {
      return null;
    }
    throw new AppError(500, `Erro ao consultar v_routes_full: ${error.message}`);
  }

  return data || [];
}

async function getFallbackRows({ driverId, routeId, importId, accessToken }) {
  let routesQuery = supabaseAdmin.from('routes').select('*').eq('driver_id', driverId);

  if (routeId) {
    routesQuery = routesQuery.eq('id', routeId);
  }

  if (importId) {
    routesQuery = routesQuery.eq('import_id', importId);
  }

  const { data: routes, error: routesError } = await routesQuery.order('id', { ascending: true });
  if (routesError) {
    throw new AppError(500, `Erro ao buscar rotas: ${routesError.message}`);
  }

  if (!routes || routes.length === 0) {
    return [];
  }

  const routeIds = routes.map((route) => route.id);
  const { data: waypoints, error: waypointsError } = await supabaseAdmin
    .from('route_waypoints')
    .select('*')
    .in('route_id', routeIds)
    .order('route_id', { ascending: true })
    .order('seq_order', { ascending: true });

  if (waypointsError) {
    throw new AppError(500, `Erro ao buscar waypoints: ${waypointsError.message}`);
  }

  const byRouteId = new Map();
  for (const waypoint of waypoints || []) {
    const key = Number(waypoint.route_id);
    if (!byRouteId.has(key)) {
      byRouteId.set(key, []);
    }
    byRouteId.get(key).push(waypoint);
  }

  const waypointAddressIds = (waypoints || [])
    .map((waypoint) => Number(waypoint.address_id))
    .filter((value) => Number.isFinite(value) && value > 0);
  const fallbackImportId = toOptionalPositiveInt(routes?.[0]?.import_id);
  const addressDetailsById = await loadAddressDetailsByIds(waypointAddressIds, {
    accessToken,
    importId: toOptionalPositiveInt(importId) || fallbackImportId
  });

  const rows = [];
  for (const route of routes) {
    const routeWaypoints = byRouteId.get(Number(route.id)) || [];
    if (routeWaypoints.length === 0) {
      rows.push({
        route_id: route.id,
        import_id: route.import_id,
        cluster_id: route.cluster_id,
        route_status: route.status,
        ativa: route.ativa,
        versao: route.versao,
        planejada_para: route.planejada_para,
        waypoint_id: null
      });
      continue;
    }

    for (const waypoint of routeWaypoints) {
      const addressId = Number(waypoint.address_id);
      const addressDetails =
        Number.isFinite(addressId) && addressId > 0
          ? addressDetailsById.get(Math.trunc(addressId))
          : null;

      rows.push({
        route_id: route.id,
        import_id: route.import_id,
        cluster_id: route.cluster_id,
        route_status: route.status,
        ativa: route.ativa,
        versao: route.versao,
        planejada_para: route.planejada_para,
        waypoint_id: waypoint.id,
        address_id: waypoint.address_id,
        seq_order: waypoint.seq_order,
        waypoint_status: waypoint.status,
        detailed_address: addressDetails?.detailed_address ?? null,
        zipcode: addressDetails?.zipcode ?? null,
        city: addressDetails?.city ?? null,
        lat: addressDetails?.lat ?? null,
        long: addressDetails?.long ?? null
      });
    }
  }

  return rows;
}

async function getRoute(authUserId, routeIdParam, accessToken) {
  const driver = await getDriverByAuthUserId(authUserId);

  if (routeIdParam !== undefined && routeIdParam !== null && routeIdParam !== '') {
    const routeId = toPositiveInt(routeIdParam, 'route_id');
    const viewRows = await getRoutesFromView({ driverId: driver.id, routeId });
    const rows =
      viewRows === null
        ? await getFallbackRows({ driverId: driver.id, routeId, accessToken })
        : viewRows;

    if (!rows.length) {
      throw new AppError(404, 'Nehuma rota encontrada');
    }

    const hydratedRows = await hydrateRowsWithAddressDetails(rows, { accessToken });
    return formatRoutesResponse(hydratedRows, driver.id);
  }

  const { data: latestRoute, error: latestRouteError } = await supabaseAdmin
    .from('routes')
    .select('*')
    .eq('driver_id', driver.id)
    .eq('status', 'CRIADA')
    .eq('ativa', false)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();

  if (latestRouteError) {
    throw new AppError(500, `Erro ao buscar rota disponível: ${latestRouteError.message}`);
  }

  if (!latestRoute) {
    throw new AppError(404, 'Nehuma rota encontrada');
  }

  const viewRows = await getRoutesFromView({ driverId: driver.id, importId: latestRoute.import_id });
  const rows =
    viewRows === null
      ? await getFallbackRows({
          driverId: driver.id,
          importId: latestRoute.import_id,
          accessToken
        })
      : viewRows;

  if (!rows.length) {
    throw new AppError(404, 'Nehuma rota encontrada');
  }

  const hydratedRows = await hydrateRowsWithAddressDetails(rows, {
    accessToken,
    importId: latestRoute.import_id
  });
  return formatRoutesResponse(hydratedRows, driver.id);
}

async function startRoute(authUserId, payload) {
  const schema = z.object({
    route_id: z.union([z.number(), z.string()])
  });
  const parsed = schema.safeParse(payload || {});

  if (!parsed.success) {
    throw new AppError(400, 'route_id não informado.');
  }

  const routeId = toPositiveInt(parsed.data.route_id, 'route_id');
  const driver = await getDriverByAuthUserId(authUserId);

  const { data: route, error: routeError } = await supabaseAdmin
    .from('routes')
    .select('*')
    .eq('id', routeId)
    .maybeSingle();

  if (routeError) {
    throw new AppError(500, `Erro ao buscar rota: ${routeError.message}`);
  }

  if (!route) {
    throw new AppError(404, 'Rota não encontrada.');
  }

  if (Number(route.driver_id) !== Number(driver.id)) {
    throw new AppError(403, 'rota de outro motorista. Acesso negado!');
  }

  const { data: updated, error: updateError } = await supabaseAdmin
    .from('routes')
    .update({
      status: 'EM_ANDAMENTO',
      iniciada_em: new Date().toISOString(),
      ativa: true
    })
    .eq('id', routeId)
    .select('*')
    .single();

  if (updateError) {
    throw new AppError(500, `Erro ao iniciar rota: ${updateError.message}`);
  }

  return {
    ok: true,
    statusCode: 200,
    msg: 'Rota iniciada com sucesso!',
    route: updated
  };
}

async function finishRoute(authUserId, routeIdParam) {
  const routeId = toPositiveInt(routeIdParam, 'route_id');
  const driver = await getDriverByAuthUserId(authUserId);

  const { data: route, error: routeError } = await supabaseAdmin
    .from('routes')
    .select('*')
    .eq('id', routeId)
    .maybeSingle();

  if (routeError) {
    throw new AppError(500, `Erro ao buscar rota: ${routeError.message}`);
  }

  if (!route) {
    throw new AppError(404, 'Rota não encontrada.');
  }

  if (Number(route.driver_id) !== Number(driver.id)) {
    throw new AppError(403, 'rota de outro motorista. Acesso negado!');
  }

  const { data: waypoints, error: waypointsError } = await supabaseAdmin
    .from('route_waypoints')
    .select('status')
    .eq('route_id', routeId);

  if (waypointsError) {
    throw new AppError(500, `Erro ao buscar waypoints da rota: ${waypointsError.message}`);
  }

  const ruleResult = canFinishRoute({
    route: { id: route.id, status: route.status },
    waypoints: (waypoints || []).map((waypoint) => ({ status: waypoint.status }))
  });

  if (!ruleResult.ok) {
    const status = ruleResult.code === 'NOT_FOUND' ? 404 : 500;
    throw new AppError(status, ruleResult.error, {
      code: ruleResult.code,
      details: ruleResult.details
    });
  }

  const { data: updatedRoute, error: updateError } = await supabaseAdmin
    .from('routes')
    .update({
      status: 'CONCLUÍDA',
      finalizada_em: new Date().toISOString(),
      ativa: false
    })
    .eq('id', routeId)
    .select('*')
    .single();

  if (updateError) {
    throw new AppError(500, `Erro ao finalizar rota: ${updateError.message}`);
  }

  return {
    ok: true,
    statusCode: 200,
    msg: 'Rota Finalizada com sucesso!',
    route: updatedRoute
  };
}

function parseCsvLine(line) {
  const out = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === ',' && !inQuotes) {
      out.push(current.trim());
      current = '';
      continue;
    }

    current += ch;
  }

  out.push(current.trim());
  return out;
}

function parseImportRows(file) {
  const filename = String(file.originalname || '').toLowerCase();
  const mimetype = String(file.mimetype || '').toLowerCase();
  const raw = file.buffer.toString('utf8').trim();

  if (!raw) {
    throw new AppError(400, 'Arquivo vazio.');
  }

  const isJson = mimetype.includes('json') || filename.endsWith('.json');
  if (isJson) {
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw new AppError(400, 'Arquivo JSON inválido.');
    }
    if (Array.isArray(parsed)) {
      return parsed;
    }
    if (Array.isArray(parsed?.addresses)) {
      return parsed.addresses;
    }
    throw new AppError(400, 'JSON inválido: esperado array de endereços.');
  }

  const lines = raw.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) {
    throw new AppError(400, 'Arquivo sem linhas suficientes para importação.');
  }

  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = parseCsvLine(line);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = cols[index] ?? null;
    });
    return row;
  });
}

function toFiniteNumber(value) {
  if (value === null || value === undefined || value === '') {
    return NaN;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : NaN;
}

function extractAddressPoints(rows) {
  const out = [];

  rows.forEach((row, index) => {
    const addressIdRaw =
      row.address_id ??
      row.addressId ??
      row.id ??
      row['Address ID'] ??
      row['address id'] ??
      row['Waybill Number'];

    const latRaw =
      row.lat ??
      row.latitude ??
      row['Receiver to Latitude'] ??
      row['Actual delivery Latitude'] ??
      row['actual_delivery_latitude'];

    const lonRaw =
      row.longitude ??
      row.long ??
      row.lng ??
      row['Receiver to Longitude'] ??
      row['Actual delivery  Longitude'] ??
      row['actual_delivery_longitude'];

    const clusterRaw = row.cluster_id ?? row.clusterId ?? row.cluster ?? 1;

    const lat = toFiniteNumber(latRaw);
    const longitude = toFiniteNumber(lonRaw);
    if (!Number.isFinite(lat) || !Number.isFinite(longitude)) {
      return;
    }

    const addressIdMaybe = toFiniteNumber(addressIdRaw);
    const addressId =
      Number.isFinite(addressIdMaybe) && addressIdMaybe > 0 ? Math.trunc(addressIdMaybe) : index + 1;

    const cluster = toFiniteNumber(clusterRaw);

    out.push({
      address_id: addressId,
      lat,
      longitude,
      cluster_id: Number.isFinite(cluster) ? Math.trunc(cluster) : 1
    });
  });

  return out;
}

async function createImportRecord(driverId, originalName) {
  const candidates = [
    { table: 'orders_import', payload: { user_id: driverId } },
    { table: 'imports', payload: { user_id: driverId, file_name: originalName } },
    { table: 'imports', payload: { user_id: driverId, filename: originalName } }
  ];

  let lastError = null;
  for (const candidate of candidates) {
    const { data, error } = await supabaseAdmin.from(candidate.table).insert(candidate.payload).select('id').single();
    if (!error && data?.id) {
      return data.id;
    }
    if (error && !isMissingTableError(error, candidate.table)) {
      lastError = error;
    }
  }

  if (lastError) {
    throw new AppError(500, `Erro ao criar importação: ${lastError.message}`);
  }

  throw new AppError(500, 'Erro ao criar importação: tabela de importação indisponível.');
}

async function importRoute(authUserId, file, options = {}) {
  if (!file) {
    throw new AppError(400, 'Arquivo de importação não enviado (campo esperado: file).');
  }

  const driver = await getDriverByAuthUserId(authUserId);
  const rows = parseImportRows(file);
  const points = extractAddressPoints(rows);

  const dbscan = dbscanDefaults({
    eps: toFiniteNumber(options.eps),
    minPts: toFiniteNumber(options.minPts)
  });

  const pointsValidation = validateDbscanPoints(
    points.map((point) => ({
      address_id: point.address_id,
      lat: point.lat,
      longitude: point.longitude
    }))
  );

  if (!pointsValidation.ok) {
    throw new AppError(400, pointsValidation.error, {
      code: pointsValidation.code,
      details: pointsValidation.details
    });
  }

  const grouped = groupByClusterId(points);
  const importId = await createImportRecord(driver.id, file.originalname || 'import.csv');

  const clusterIds = Object.keys(grouped)
    .map((value) => Number(value))
    .filter(Number.isFinite)
    .sort((a, b) => a - b);

  const routes = [];

  for (const clusterId of clusterIds) {
    const { data: route, error: routeError } = await supabaseAdmin
      .from('routes')
      .insert({
        import_id: importId,
        driver_id: driver.id,
        cluster_id: clusterId,
        status: 'CRIADA',
        ativa: false,
        versao: 1
      })
      .select('*')
      .single();

    if (routeError) {
      throw new AppError(500, `Erro ao criar rota importada: ${routeError.message}`);
    }

    const generated = generateWaypointsForCluster({
      route_id: route.id,
      cluster_id: clusterId,
      addresses: grouped[clusterId].map((point) => ({ address_id: point.address_id }))
    });

    if (!generated.ok) {
      throw new AppError(400, generated.error, {
        code: generated.code,
        details: generated.details
      });
    }

    const waypointPayload = generated.value.map((waypoint) => ({
      route_id: waypoint.route_id,
      cluster_id: waypoint.cluster_id,
      address_id: waypoint.address_id,
      seq_order: waypoint.seq_order,
      status: waypoint.status
    }));

    const { data: insertedWaypoints, error: waypointError } = await supabaseAdmin
      .from('route_waypoints')
      .insert(waypointPayload)
      .select('*');

    if (waypointError) {
      throw new AppError(500, `Erro ao criar waypoints importados: ${waypointError.message}`);
    }

    routes.push({
      route_id: route.id,
      cluster_id: route.cluster_id,
      status: route.status,
      ativa: route.ativa,
      stops: (insertedWaypoints || []).map((waypoint) => ({
        waypoint_id: waypoint.id,
        address_id: waypoint.address_id,
        seq_order: waypoint.seq_order,
        status: waypoint.status
      }))
    });
  }

  return {
    ok: true,
    import_id: importId,
    user_id: driver.id,
    dbscan,
    routes
  };
}

module.exports = {
  getRoute,
  importRoute,
  startRoute,
  finishRoute
};
