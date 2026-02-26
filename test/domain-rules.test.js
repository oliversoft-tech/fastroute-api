'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { canFinishRoute, validateFinishWaypoint } = require('@oliverbill/fastroute-domain');

test('canFinishRoute blocks route with pending waypoints', () => {
  const result = canFinishRoute({
    route: { id: 1, status: 'EM_ANDAMENTO' },
    waypoints: [{ status: 'PENDENTE' }, { status: 'ENTREGUE' }]
  });

  assert.equal(result.ok, false);
  assert.equal(result.code, 'INVALID_STATE');
});

test('validateFinishWaypoint accepts valid payload', () => {
  const result = validateFinishWaypoint({
    currentWaypoint: { id: 10, route_id: 20, status: 'PENDENTE' },
    targetStatus: 'ENTREGUE',
    obs_falha: null,
    photo: {
      waypoint_id: 10,
      filename: 'photo.jpg',
      user_id: 99,
      object_path: '20/10/photo.jpg',
      file_size_bytes: 1000,
      photo_url: 'https://example.com/photo.jpg'
    }
  });

  assert.equal(result.ok, true);
  if (result.ok) {
    assert.equal(result.value.waypoint_id, 10);
    assert.equal(result.value.new_status, 'ENTREGUE');
  }
});
