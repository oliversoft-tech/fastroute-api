'use strict';
const { supabaseAdmin } = require('../lib/supabase');

async function wasApplied(deviceId, mutationId) {
  const { data } = await supabaseAdmin
    .from('mutations_applied')
    .select('*')
    .eq('device_id', deviceId)
    .eq('mutation_id', mutationId)
    .maybeSingle();
  return !!data;
}

async function markApplied(deviceId, mutationId) {
  await supabaseAdmin.from('mutations_applied').insert({
    device_id: deviceId,
    mutation_id: mutationId
  });
}

async function logChange(entityType, entityId, op, version, payload) {
  await supabaseAdmin.from('change_log').insert({
    entity_type: entityType,
    entity_id: entityId,
    op,
    version,
    payload
  });
}

async function applyMutation(m) {

  if (await wasApplied(m.deviceId, m.mutationId)) {
    return { mutationId: m.mutationId, status: 'DUPLICATE' };
  }

  if (m.entityType === 'route') {

    const { data: route } = await supabaseAdmin
      .from('routes')
      .select('*')
      .eq('id', m.entityId)
      .single();

    if (!route) {
      await supabaseAdmin.from('routes')
        .insert({ id: m.entityId, ...m.payload, version: 1 });

      await logChange('route', m.entityId, m.op, 1, m.payload);
    } else {
      if (route.version !== m.baseVersion)
        return { mutationId: m.mutationId, status: 'CONFLICT', serverVersion: route.version };

      const nextVersion = route.version + 1;

      await supabaseAdmin.from('routes')
        .update({ ...m.payload, version: nextVersion })
        .eq('id', m.entityId);

      await logChange('route', route.id, m.op, nextVersion, m.payload);
    }
  }

  if (m.entityType === 'route_waypoint') {

    const { data: wp } = await supabaseAdmin
      .from('route_waypoints')
      .select('*')
      .eq('id', m.entityId)
      .single();

    if (!wp) return { mutationId: m.mutationId, status: 'NOT_FOUND' };

    if (wp.version !== m.baseVersion)
      return { mutationId: m.mutationId, status: 'CONFLICT', serverVersion: wp.version };

    const nextVersion = wp.version + 1;

    await supabaseAdmin.from('route_waypoints')
      .update({ ...m.payload, version: nextVersion })
      .eq('id', m.entityId);

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
  const { data } = await supabaseAdmin
    .from('change_log')
    .select('*')
    .gt('created_at', sinceTs || '1970-01-01')
    .order('created_at', { ascending: true });

  return { ok: true, changes: data };
}

module.exports = { push, pull };
