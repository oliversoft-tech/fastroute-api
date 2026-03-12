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
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === 'string' && value.trim().length === 0) {
    return null;
  }
  const parsed = Math.trunc(Number(value));
  if (!Number.isFinite(parsed) || parsed < 0) {
    return null;
  }
  return parsed;
}

function toNonEmptyString(value) {
  const normalized = String(value ?? '').trim();
  if (!normalized) {
    return null;
  }
  return normalized;
}

function decodeBase64UrlToUtf8(value) {
  const normalized = toNonEmptyString(value);
  if (!normalized) {
    return null;
  }

  const base64 = normalized.replace(/-/g, '+').replace(/_/g, '/');
  const padded = base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), '=');
  try {
    return Buffer.from(padded, 'base64').toString('utf8');
  } catch {
    return null;
  }
}

function readAuthUserIdFromAuthorizationHeader(headerValue) {
  const rawHeader = toNonEmptyString(headerValue);
  if (!rawHeader) {
    return null;
  }

  const bearerPrefix = /^bearer\s+/i;
  const token = rawHeader.replace(bearerPrefix, '').trim();
  if (!token) {
    return null;
  }

  const parts = token.split('.');
  if (parts.length < 2) {
    return null;
  }

  const payloadRaw = decodeBase64UrlToUtf8(parts[1]);
  if (!payloadRaw) {
    return null;
  }

  try {
    const payload = toNormalizedObject(JSON.parse(payloadRaw));
    return (
      toNonEmptyString(payload.sub) ??
      toNonEmptyString(payload.auth_user_id) ??
      toNonEmptyString(payload.authUserId) ??
      toNonEmptyString(payload.user_id) ??
      toNonEmptyString(payload.userId)
    );
  } catch {
    return null;
  }
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
  'subtitle',
  'photo',
  'file_name',
  'fileName',
  'filename',
  'user_id',
  'userId',
  'image_uri',
  'imageUri',
  'image_base64',
  'imageBase64',
  'image_mime_type',
  'imageMimeType',
  'photo_url',
  'photoUrl',
  'object_path',
  'objectPath',
  'bucket',
  'file_size_bytes',
  'fileSizeBytes'
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

function basenameFromPath(pathValue) {
  const normalized = toNonEmptyString(pathValue);
  if (!normalized) {
    return null;
  }
  const withoutQuery = normalized.split('?')[0]?.split('#')[0] ?? normalized;
  const segments = withoutQuery.replace(/\\/g, '/').split('/').filter(Boolean);
  if (segments.length === 0) {
    return null;
  }
  return segments[segments.length - 1];
}

function buildObjectPathFromImageReference(imageReference, fallbackFileName) {
  const normalized = toNonEmptyString(imageReference);
  if (!normalized) {
    return toNonEmptyString(fallbackFileName);
  }

  const withoutQuery = normalized.split('?')[0]?.split('#')[0] ?? normalized;
  const marker = '/delivery-photos/';
  const markerIndex = withoutQuery.lastIndexOf(marker);
  if (markerIndex >= 0) {
    const fromMarker = withoutQuery.slice(markerIndex + marker.length).trim();
    if (fromMarker.length > 0) {
      return fromMarker;
    }
  }

  return basenameFromPath(withoutQuery) ?? toNonEmptyString(fallbackFileName);
}

function parseWaypointPhotoInput(rawPayload, entityId, fallbackRouteId) {
  const payload = toNormalizedObject(rawPayload);
  const photo = toNormalizedObject(payload.photo);

  const fileName =
    toNonEmptyString(photo.file_name) ??
    toNonEmptyString(photo.filename) ??
    toNonEmptyString(payload.file_name) ??
    toNonEmptyString(payload.fileName);
  const imageReference =
    toNonEmptyString(photo.image_uri) ??
    toNonEmptyString(photo.imageUri) ??
    toNonEmptyString(photo.photo_url) ??
    toNonEmptyString(photo.photoUrl) ??
    toNonEmptyString(payload.image_uri) ??
    toNonEmptyString(payload.imageUri) ??
    toNonEmptyString(payload.photo_url) ??
    toNonEmptyString(payload.photoUrl);
  const imageBase64 =
    toNonEmptyString(photo.image_base64) ??
    toNonEmptyString(photo.imageBase64) ??
    toNonEmptyString(payload.image_base64) ??
    toNonEmptyString(payload.imageBase64);
  const imageMimeType =
    toNonEmptyString(photo.image_mime_type) ??
    toNonEmptyString(photo.imageMimeType) ??
    toNonEmptyString(payload.image_mime_type) ??
    toNonEmptyString(payload.imageMimeType);
  const objectPathFromPayload =
    toNonEmptyString(photo.object_path) ??
    toNonEmptyString(photo.objectPath) ??
    toNonEmptyString(payload.object_path) ??
    toNonEmptyString(payload.objectPath);
  const objectPathFromReference = buildObjectPathFromImageReference(imageReference, fileName);

  const fileSizeBytes =
    toNonNegativeInt(photo.file_size_bytes ?? photo.fileSizeBytes) ??
    toNonNegativeInt(payload.file_size_bytes ?? payload.fileSizeBytes);
  const routeId =
    toPositiveInt(photo.route_id ?? photo.routeId) ??
    toPositiveInt(payload.route_id ?? payload.routeId) ??
    toPositiveInt(fallbackRouteId);
  const userCandidate =
    photo.user_id ??
    photo.userId ??
    payload.user_id ??
    payload.userId ??
    payload.driver_id ??
    payload.driverId ??
    payload.auth_user_id ??
    payload.authUserId;
  const bucket =
    toNonEmptyString(photo.bucket) ??
    toNonEmptyString(payload.bucket) ??
    process.env.WAYPOINT_PHOTOS_BUCKET ??
    'delivery-photos';

  const hasAnyPhotoField =
    Object.keys(photo).length > 0 ||
    Boolean(fileName) ||
    Boolean(imageReference) ||
    Boolean(imageBase64) ||
    Boolean(objectPathFromPayload) ||
    Boolean(objectPathFromReference) ||
    fileSizeBytes !== null;

  if (!hasAnyPhotoField) {
    return null;
  }

  const fallbackFileName = fileName ?? (toPositiveInt(entityId) ? `photo_${toPositiveInt(entityId)}.jpg` : null);
  const objectPath = objectPathFromPayload ?? objectPathFromReference ?? fallbackFileName;
  const finalFileName = fileName ?? basenameFromPath(objectPath) ?? fallbackFileName;

  return {
    waypointId: toPositiveInt(photo.waypoint_id ?? photo.waypointId) ?? toPositiveInt(entityId) ?? null,
    routeId,
    userCandidate,
    fileName: finalFileName,
    objectPath,
    fileSizeBytes,
    bucket,
    imageBase64,
    imageMimeType
  };
}

function parseBase64Input(rawValue) {
  const normalizedValue = toNonEmptyString(rawValue);
  if (!normalizedValue) {
    return null;
  }

  const dataUriMatch = normalizedValue.match(/^data:([^;]+);base64,(.+)$/i);
  const mimeTypeFromDataUri = toNonEmptyString(dataUriMatch?.[1]);
  const rawBase64 = dataUriMatch?.[2] ?? normalizedValue;
  const compactBase64 = rawBase64.replace(/\s+/g, '').trim();
  if (!compactBase64) {
    return null;
  }

  try {
    const bytes = Buffer.from(compactBase64, 'base64');
    if (!bytes || bytes.length === 0) {
      return null;
    }

    return {
      bytes,
      mimeTypeFromDataUri
    };
  } catch {
    throw new AppError(400, 'Payload de waypoint com foto inválido: image_base64 malformado.');
  }
}

async function uploadWaypointPhotoFromSync(photoInput) {
  const parsedBase64 = parseBase64Input(photoInput?.imageBase64);
  if (!parsedBase64) {
    return null;
  }

  const bucket = toNonEmptyString(photoInput?.bucket) ?? 'delivery-photos';
  const objectPath = toNonEmptyString(photoInput?.objectPath);
  if (!objectPath) {
    throw new AppError(400, 'Payload de waypoint com foto incompleto para sync: object_path ausente.');
  }

  const contentType =
    toNonEmptyString(photoInput?.imageMimeType) ??
    toNonEmptyString(parsedBase64.mimeTypeFromDataUri) ??
    'image/jpeg';

  const { error } = await supabaseAdmin.storage.from(bucket).upload(objectPath, parsedBase64.bytes, {
    contentType,
    upsert: true
  });

  if (error) {
    throw new AppError(500, `Erro no upload da foto via sync: ${error.message}`);
  }

  return {
    fileSizeBytes: parsedBase64.bytes.length
  };
}

async function insertWaypointPhotoWithCompatiblePayload(rawPayload) {
  const payload = compactObject(toNormalizedObject(rawPayload));
  let lastError = null;

  for (let attempt = 0; attempt < 8; attempt += 1) {
    const { error } = await supabaseAdmin.from('waypoint_delivery_photo').insert(payload);
    if (!error) {
      return payload;
    }

    const missingColumn = readMissingColumnName(error);
    if (!missingColumn || !Object.prototype.hasOwnProperty.call(payload, missingColumn)) {
      throw error;
    }

    delete payload[missingColumn];
    lastError = error;
  }

  if (lastError) {
    throw lastError;
  }

  return payload;
}

async function updateWaypointPhotoWithCompatiblePayload(waypointId, rawPayload) {
  const payload = compactObject(toNormalizedObject(rawPayload));
  delete payload.waypoint_id;

  if (Object.keys(payload).length === 0) {
    return payload;
  }

  let lastError = null;
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const { error } = await supabaseAdmin
      .from('waypoint_delivery_photo')
      .update(payload)
      .eq('waypoint_id', waypointId);

    if (!error) {
      return payload;
    }

    const missingColumn = readMissingColumnName(error);
    if (!missingColumn || !Object.prototype.hasOwnProperty.call(payload, missingColumn)) {
      throw error;
    }

    delete payload[missingColumn];
    lastError = error;
  }

  if (lastError) {
    throw lastError;
  }

  return payload;
}

async function resolveWaypointPhotoUserId(photoInput, mutation) {
  const routeId = toPositiveInt(photoInput?.routeId);
  const payload = toNormalizedObject(mutation?.payload);
  const candidates = [
    photoInput?.userCandidate,
    routeId ? mutation?.context?.routeDriverHintsByRouteId?.get(routeId) : null,
    mutation?.context?.deviceDriverIdHint,
    payload.user_id,
    payload.userId,
    payload.driver_id,
    payload.driverId,
    payload.auth_user_id,
    payload.authUserId,
    mutation?.context?.authUserIdHint
  ];

  for (const candidate of candidates) {
    const resolved = await resolveDriverIdCandidate(candidate);
    if (resolved) {
      return resolved;
    }
  }

  return null;
}

async function persistWaypointDeliveryPhotoFromSyncMutation(mutation, currentWaypoint) {
  const photoInput = parseWaypointPhotoInput(mutation?.payload, mutation?.entityId, currentWaypoint?.route_id);
  if (!photoInput) {
    return false;
  }

  const waypointId = toPositiveInt(photoInput.waypointId ?? mutation?.entityId);
  if (!waypointId) {
    throw new AppError(400, 'Payload de waypoint com foto incompleto para sync: waypoint_id ausente.');
  }
  if (!toPositiveInt(photoInput.routeId)) {
    throw new AppError(400, 'Payload de waypoint com foto incompleto para sync: route_id ausente.');
  }
  if (!toNonEmptyString(photoInput.fileName)) {
    throw new AppError(400, 'Payload de waypoint com foto incompleto para sync: file_name ausente.');
  }
  if (!toNonEmptyString(photoInput.objectPath)) {
    throw new AppError(400, 'Payload de waypoint com foto incompleto para sync: object_path ausente.');
  }

  const uploaded = await uploadWaypointPhotoFromSync(photoInput);
  const resolvedUserId = await resolveWaypointPhotoUserId(photoInput, mutation);
  if (!resolvedUserId) {
    throw new AppError(400, 'Payload de waypoint com foto incompleto para sync: user_id ausente.');
  }

  const normalizedFileSizeBytes =
    toNonNegativeInt(photoInput.fileSizeBytes) ??
    toNonNegativeInt(uploaded?.fileSizeBytes);

  const normalizedPhotoPayload = compactObject({
    waypoint_id: waypointId,
    route_id: toPositiveInt(photoInput.routeId),
    user_id: resolvedUserId,
    bucket: toNonEmptyString(photoInput.bucket) ?? 'delivery-photos',
    object_path: toNonEmptyString(photoInput.objectPath),
    file_name: toNonEmptyString(photoInput.fileName),
    file_size_bytes: normalizedFileSizeBytes
  });

  const { data: existing, error: selectPhotoError } = await supabaseAdmin
    .from('waypoint_delivery_photo')
    .select('waypoint_id')
    .eq('waypoint_id', waypointId)
    .maybeSingle();
  if (selectPhotoError) {
    throw selectPhotoError;
  }

  if (existing) {
    await updateWaypointPhotoWithCompatiblePayload(waypointId, normalizedPhotoPayload);
  } else {
    await insertWaypointPhotoWithCompatiblePayload(normalizedPhotoPayload);
  }

  return true;
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

async function resolveDriverIdCandidate(value) {
  const directDriverId = toPositiveInt(value);
  if (directDriverId) {
    return directDriverId;
  }

  const candidate = toNonEmptyString(value);
  if (!candidate) {
    return null;
  }

  return resolveDriverIdFromAuthUserId(candidate);
}

async function resolveDriverIdFromImportId(importId) {
  const parsedImportId = toPositiveInt(importId);
  if (!parsedImportId) {
    return null;
  }

  const tables = ['orders_import', 'imports'];

  for (const tableName of tables) {
    for (const columnName of ['user_id', 'driver_id', 'auth_user_id', 'authUserId']) {
      const { data, error } = await supabaseAdmin
        .from(tableName)
        .select(columnName)
        .eq('id', parsedImportId)
        .maybeSingle();

      if (!error) {
        const resolvedDriverId = await resolveDriverIdCandidate(data?.[columnName]);
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

async function resolveDriverIdFromExistingRoutesByImportId(importId) {
  const parsedImportId = toPositiveInt(importId);
  if (!parsedImportId) {
    return null;
  }

  const { data, error } = await supabaseAdmin
    .from('routes')
    .select('driver_id')
    .eq('import_id', parsedImportId)
    .order('id', { ascending: true })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new AppError(500, `Erro ao resolver motorista via rotas do import_id: ${error.message}`);
  }

  return resolveDriverIdCandidate(data?.driver_id);
}

async function resolveDriverIdFromSingleUserFallback() {
  const { data, error } = await supabaseAdmin
    .from('users')
    .select('id')
    .order('id', { ascending: true })
    .limit(2);

  if (error) {
    throw new AppError(500, `Erro ao resolver fallback de motorista: ${error.message}`);
  }

  const users = Array.isArray(data) ? data : [];
  if (users.length !== 1) {
    return null;
  }

  return resolveDriverIdCandidate(users[0]?.id);
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
    const driverCandidates = [
      payload.user_id,
      mutation?.context?.routeDriverHintsByRouteId?.get(routeIdForHints),
      mutation?.context?.deviceDriverIdHint,
      mutation?.payload?.driver_id,
      mutation?.payload?.driverId,
      mutation?.payload?.user_id,
      mutation?.payload?.userId,
      mutation?.payload?.auth_user_id,
      mutation?.payload?.authUserId,
      mutation?.context?.authUserIdHint
    ];

    let fallbackDriverId = null;
    for (const candidate of driverCandidates) {
      // Aceita ids numéricos e também auth_user_id (uuid/string) como fallback.
      fallbackDriverId = await resolveDriverIdCandidate(candidate);
      if (fallbackDriverId) {
        break;
      }
    }

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

  if (!toPositiveInt(payload.driver_id)) {
    const importIdForRouteFallback =
      toPositiveInt(payload.import_id) ??
      toPositiveInt(mutation?.payload?.import_id) ??
      toPositiveInt(mutation?.payload?.importId);
    const driverIdFromExistingRoutes = await resolveDriverIdFromExistingRoutesByImportId(importIdForRouteFallback);
    if (driverIdFromExistingRoutes) {
      payload.driver_id = driverIdFromExistingRoutes;
    }
  }

  if (!toPositiveInt(payload.driver_id)) {
    const driverIdFromSingleUser = await resolveDriverIdFromSingleUserFallback();
    if (driverIdFromSingleUser) {
      payload.driver_id = driverIdFromSingleUser;
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

      try {
        await persistWaypointDeliveryPhotoFromSyncMutation(m, {
          id: m.entityId,
          route_id: waypointPayload.route_id
        });
      } catch (photoPersistError) {
        throw new AppError(500, `Erro ao persistir foto de waypoint via sync: ${photoPersistError.message}`);
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

    try {
      await persistWaypointDeliveryPhotoFromSyncMutation(m, wp);
    } catch (photoPersistError) {
      throw new AppError(500, `Erro ao persistir foto de waypoint via sync: ${photoPersistError.message}`);
    }

    await logChange('route_waypoint', wp.id, m.op, nextVersion, waypointChangePayload);
  }

  await markApplied(m.deviceId, m.mutationId);
  return { mutationId: m.mutationId, status: 'APPLIED' };
}

async function push(body, options = {}) {
  const authUserIdHint =
    toNonEmptyString(options?.authUserIdHint) ??
    readAuthUserIdFromAuthorizationHeader(options?.authorizationHeader);

  const driverIdCandidateCache = new Map();
  const resolveDriverIdCandidateCached = async (candidate) => {
    const cacheKey = JSON.stringify(candidate ?? null);
    if (driverIdCandidateCache.has(cacheKey)) {
      return driverIdCandidateCache.get(cacheKey);
    }

    const resolved = await resolveDriverIdCandidate(candidate);
    driverIdCandidateCache.set(cacheKey, resolved);
    return resolved;
  };

  const authHeaderDriverIdHint = await resolveDriverIdCandidateCached(authUserIdHint);
  const routeDriverHintsByRouteId = new Map();
  const deviceDriverHintsByDeviceId = new Map();
  for (const mutation of body?.mutations || []) {
    const mutationPayload = toNormalizedObject(mutation?.payload);
    const entityType = String(mutation?.entityType || '').toLowerCase();
    const mutationOp = String(mutation?.op || '').toUpperCase();
    const deviceId = toNonEmptyString(mutation?.deviceId);
    let mutationDriverId =
      (await resolveDriverIdCandidateCached(mutationPayload.user_id)) ??
      (await resolveDriverIdCandidateCached(mutationPayload.userId)) ??
      (await resolveDriverIdCandidateCached(mutationPayload.driver_id)) ??
      (await resolveDriverIdCandidateCached(mutationPayload.driverId)) ??
      (await resolveDriverIdCandidateCached(mutationPayload.auth_user_id)) ??
      (await resolveDriverIdCandidateCached(mutationPayload.authUserId)) ??
      authHeaderDriverIdHint;

    if (deviceId && mutationDriverId && !deviceDriverHintsByDeviceId.has(deviceId)) {
      deviceDriverHintsByDeviceId.set(deviceId, mutationDriverId);
    }

    if (entityType !== 'route_waypoint' || mutationOp !== 'CREATE') {
      continue;
    }

    const routeId = toPositiveInt(mutationPayload.route_id) ?? toPositiveInt(mutationPayload.routeId);
    if (!routeId || !mutationDriverId || routeDriverHintsByRouteId.has(routeId)) {
      continue;
    }
    routeDriverHintsByRouteId.set(routeId, mutationDriverId);
  }

  const results = [];
  for (const m of body?.mutations || []) {
    const deviceId = toNonEmptyString(m?.deviceId);
    const deviceDriverIdHint = deviceId ? (deviceDriverHintsByDeviceId.get(deviceId) ?? null) : null;
    results.push(
      await applyMutation({
        ...m,
        context: {
          routeDriverHintsByRouteId,
          deviceDriverIdHint,
          authUserIdHint
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
