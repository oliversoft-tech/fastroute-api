'use strict';

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

function toPositiveInt(value, fieldName) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new AppError(400, `${fieldName} inválido.`);
  }
  return Math.trunc(parsed);
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
      lat: row.lat ?? null,
      long: row.long ?? row.longitude ?? null
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

async function getFallbackRows({ driverId, routeId, importId }) {
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
        waypoint_status: waypoint.status
      });
    }
  }

  return rows;
}

async function getRoute(authUserId, routeIdParam) {
  const driver = await getDriverByAuthUserId(authUserId);

  if (routeIdParam !== undefined && routeIdParam !== null && routeIdParam !== '') {
    const routeId = toPositiveInt(routeIdParam, 'route_id');
    const viewRows = await getRoutesFromView({ driverId: driver.id, routeId });
    const rows = viewRows === null ? await getFallbackRows({ driverId: driver.id, routeId }) : viewRows;

    if (!rows.length) {
      throw new AppError(404, 'Nehuma rota encontrada');
    }

    return formatRoutesResponse(rows, driver.id);
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
    viewRows === null ? await getFallbackRows({ driverId: driver.id, importId: latestRoute.import_id }) : viewRows;

  if (!rows.length) {
    throw new AppError(404, 'Nehuma rota encontrada');
  }

  return formatRoutesResponse(rows, driver.id);
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
    } catch (error) {
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
    { user_id: driverId, file_name: originalName },
    { user_id: driverId, filename: originalName }
  ];

  for (const payload of candidates) {
    const { data, error } = await supabaseAdmin.from('imports').insert(payload).select('id').single();
    if (!error && data?.id) {
      return data.id;
    }
  }

  return Date.now();
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
