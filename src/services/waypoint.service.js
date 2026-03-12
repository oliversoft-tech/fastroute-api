'use strict';

const { z } = require('zod');
const { validateFinishWaypoint } = require('../lib/domain');
const { AppError } = require('../lib/appError');
const { supabaseAdmin } = require('../lib/supabase');

function toPositiveInt(value, fieldName) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new AppError(400, `${fieldName} inválido.`);
  }
  return Math.trunc(parsed);
}

function parseStoragePath(urlPhoto, fallbackBucket) {
  if (!urlPhoto) {
    return null;
  }

  const raw = String(urlPhoto);

  const signedOrPublic = raw.match(/\/storage\/v1\/object\/(?:public|sign)\/([^/]+)\/(.+?)(?:\?|$)/);
  if (signedOrPublic) {
    return {
      bucket: signedOrPublic[1],
      path: signedOrPublic[2]
    };
  }

  if (!raw.startsWith('http')) {
    const parts = raw.split('/').filter(Boolean);
    if (parts.length >= 2) {
      return {
        bucket: parts[0],
        path: parts.slice(1).join('/')
      };
    }
    return {
      bucket: fallbackBucket,
      path: raw
    };
  }

  return null;
}

function isMissingColumnError(error, columnName) {
  const message = String(error?.message || '').toLowerCase();
  return message.includes('column') && message.includes(String(columnName || '').toLowerCase()) && message.includes('does not exist');
}

async function getWaypointWithCompatiblePhotoColumns(waypointId) {
  const withUrlPhoto = await supabaseAdmin
    .from('route_waypoints')
    .select('id, route_id, url_photo')
    .eq('id', waypointId)
    .maybeSingle();

  if (!withUrlPhoto.error) {
    return withUrlPhoto;
  }

  if (!isMissingColumnError(withUrlPhoto.error, 'url_photo')) {
    return withUrlPhoto;
  }

  const fallback = await supabaseAdmin
    .from('route_waypoints')
    .select('id, route_id')
    .eq('id', waypointId)
    .maybeSingle();

  if (fallback.error) {
    return fallback;
  }

  return {
    data: {
      ...fallback.data,
      url_photo: null
    },
    error: null
  };
}

async function finishWaypoint(authUserId, payload, file) {
  const schema = z.object({
    waypoint_id: z.union([z.number(), z.string()]),
    status: z.string().min(1),
    file_name: z.string().optional(),
    obs_falha: z.any().optional(),
    user_id: z.union([z.number(), z.string()]).optional(),
    route_id: z.union([z.number(), z.string()]).optional()
  });
  const parsed = schema.safeParse(payload || {});

  if (!parsed.success) {
    throw new AppError(400, 'Campos obrigatórios ausentes para finalizar waypoint.');
  }

  if (!file || !file.buffer) {
    throw new AppError(500, 'Campos Obrigatorios Não preenchidos');
  }

  const waypointId = toPositiveInt(parsed.data.waypoint_id, 'waypoint_id');

  const { data: waypoint, error: waypointError } = await supabaseAdmin
    .from('route_waypoints')
    .select('*')
    .eq('id', waypointId)
    .maybeSingle();

  if (waypointError) {
    throw new AppError(500, `Erro ao buscar waypoint: ${waypointError.message}`);
  }

  const routeId = parsed.data.route_id
    ? toPositiveInt(parsed.data.route_id, 'route_id')
    : Number(waypoint?.route_id || 0);

  const filename = (parsed.data.file_name || `photo_${Date.now()}.jpg`).trim();
  const objectPath = `${routeId}/${waypointId}/${filename}`;
  const bucket = process.env.WAYPOINT_PHOTOS_BUCKET || 'delivery-photos';
  const fileSizeBytes = file.size || 1;
  const userId = parsed.data.user_id || authUserId;
  const photoUrl = `${process.env.SUPABASE_URL}/storage/v1/object/${bucket}/${objectPath}`;

  const ruleResult = validateFinishWaypoint({
    currentWaypoint: waypoint
      ? {
          id: Number(waypoint.id),
          route_id: Number(waypoint.route_id),
          status: waypoint.status
        }
      : null,
    targetStatus: parsed.data.status,
    obs_falha: parsed.data.obs_falha ?? null,
    photo: {
      waypoint_id: waypointId,
      filename,
      user_id: userId,
      object_path: objectPath,
      file_size_bytes: fileSizeBytes,
      photo_url: photoUrl
    }
  });

  if (!ruleResult.ok) {
    const status = ruleResult.code === 'NOT_FOUND' ? 404 : 500;
    throw new AppError(status, ruleResult.error, {
      code: ruleResult.code,
      details: ruleResult.details
    });
  }

  const { error: uploadError } = await supabaseAdmin.storage.from(bucket).upload(objectPath, file.buffer, {
    contentType: file.mimetype || 'image/jpeg',
    upsert: true
  });

  if (uploadError) {
    throw new AppError(500, `Erro no upload da foto: ${uploadError.message}`);
  }

  const photoPayload = {
    waypoint_id: waypointId,
    route_id: routeId,
    file_name: filename,
    user_id: userId,
    object_path: objectPath,
    file_size_bytes: fileSizeBytes
  };

  const { error: photoError } = await supabaseAdmin
    .from('waypoint_delivery_photo')
    .upsert(photoPayload, { onConflict: 'waypoint_id' });

  if (photoError) {
    throw new AppError(500, `Erro ao gravar metadados da foto: ${photoError.message}`);
  }

  const { data: updatedWaypoint, error: updateError } = await supabaseAdmin
    .from('route_waypoints')
    .update({
      status: ruleResult.value.new_status,
      entregue_em: new Date().toISOString(),
      obs_falha: ruleResult.value.obs_falha ?? null,
      url_photo: `${bucket}/${objectPath}`
    })
    .eq('id', waypointId)
    .select('*')
    .single();

  if (updateError) {
    throw new AppError(500, `Erro ao atualizar waypoint: ${updateError.message}`);
  }

  return {
    ok: true,
    statusCode: 200,
    msg: 'Encomenda Entregue com Sucesso',
    waypoint: updatedWaypoint,
    photo_url: photoUrl
  };
}

async function getWaypointPhoto(waypointIdParam) {
  const waypointId = toPositiveInt(waypointIdParam, 'waypoint_id');
  const bucketDefault = process.env.WAYPOINT_PHOTOS_BUCKET || 'delivery-photos';
  const expiresIn = Number(process.env.SIGNED_URL_EXPIRES_IN || 300);

  const { data: waypoint, error: waypointError } = await getWaypointWithCompatiblePhotoColumns(waypointId);

  if (waypointError) {
    throw new AppError(500, `Erro ao buscar waypoint: ${waypointError.message}`);
  }

  if (!waypoint) {
    throw new AppError(404, 'waypoint nao encontrado!');
  }

  let storageTarget = parseStoragePath(waypoint.url_photo, bucketDefault);

  if (!storageTarget) {
    const { data: photoRow, error: photoError } = await supabaseAdmin
      .from('waypoint_delivery_photo')
      .select('bucket, object_path')
      .eq('waypoint_id', waypointId)
      .order('id', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (photoError) {
      throw new AppError(500, `Erro ao buscar metadados da foto: ${photoError.message}`);
    }

    if (!photoRow?.object_path) {
      throw new AppError(404, 'Waypoint não possui foto.');
    }

    storageTarget = {
      bucket: String(photoRow.bucket || bucketDefault),
      path: photoRow.object_path
    };
  }

  const { data: signed, error: signedError } = await supabaseAdmin.storage
    .from(storageTarget.bucket)
    .createSignedUrl(storageTarget.path, expiresIn);

  if (signedError) {
    throw new AppError(500, `Erro ao gerar signed URL: ${signedError.message}`);
  }

  return {
    success: true,
    signed_url: signed?.signedUrl,
    expires_in: expiresIn
  };
}

async function reorderWaypoints(payload) {
  const schema = z.object({
    route_id: z.union([z.number(), z.string()]),
    reordered_waypoints: z
      .array(
        z.object({
          waypoint_id: z.union([z.number(), z.string()]),
          seqorder: z.union([z.number(), z.string()]).optional(),
          seq_order: z.union([z.number(), z.string()]).optional()
        })
      )
      .min(1)
  });
  const parsed = schema.safeParse(payload || {});

  if (!parsed.success) {
    throw new AppError(400, 'Payload inválido para reordenação.');
  }

  const routeId = toPositiveInt(parsed.data.route_id, 'route_id');
  const reorderedWaypoints = parsed.data.reordered_waypoints.map((waypoint) => ({
    waypoint_id: toPositiveInt(waypoint.waypoint_id, 'waypoint_id'),
    seq_order: toPositiveInt(waypoint.seqorder ?? waypoint.seq_order, 'seqorder')
  }));

  const { data: route, error: routeError } = await supabaseAdmin
    .from('routes')
    .select('id')
    .eq('id', routeId)
    .maybeSingle();

  if (routeError) {
    throw new AppError(500, `Erro ao buscar rota: ${routeError.message}`);
  }

  if (!route) {
    throw new AppError(404, 'rota nao encontrada!');
  }

  const waypointIds = reorderedWaypoints.map((waypoint) => waypoint.waypoint_id);
  const { data: waypoints, error: waypointsError } = await supabaseAdmin
    .from('route_waypoints')
    .select('id, route_id')
    .in('id', waypointIds);

  if (waypointsError) {
    throw new AppError(500, `Erro ao buscar waypoints: ${waypointsError.message}`);
  }

  if (!waypoints || waypoints.length !== waypointIds.length) {
    throw new AppError(404, 'waypoint nao encontrado!');
  }

  const uniqueRouteIds = new Set(waypoints.map((waypoint) => Number(waypoint.route_id)));
  if (uniqueRouteIds.size > 1 || !uniqueRouteIds.has(routeId)) {
    throw new AppError(500, 'Reordenação não permitida! Waypoints pertencem a rotas diferentes.');
  }

  for (const waypoint of reorderedWaypoints) {
    const { error: updateError } = await supabaseAdmin
      .from('route_waypoints')
      .update({
        seq_order: waypoint.seq_order,
        status: 'REORDENADO'
      })
      .eq('id', waypoint.waypoint_id);

    if (updateError) {
      throw new AppError(500, `Erro ao reordenar waypoint ${waypoint.waypoint_id}: ${updateError.message}`);
    }
  }

  return {
    ok: true,
    statusCode: 200,
    route_id: routeId,
    reordered_waypoints: reorderedWaypoints
  };
}

module.exports = {
  finishWaypoint,
  getWaypointPhoto,
  reorderWaypoints
};
