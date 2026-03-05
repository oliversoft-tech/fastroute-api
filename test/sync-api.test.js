'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');

function createFakeSupabase(initial = {}) {
  const state = {
    routes: [...(initial.routes || [])],
    route_waypoints: [...(initial.route_waypoints || [])],
    mutations_applied: [...(initial.mutations_applied || [])],
    change_log: [...(initial.change_log || [])]
  };

  let tick = 0;

  class Query {
    constructor(tableName) {
      this.tableName = tableName;
      this.filters = [];
      this.mode = 'select';
      this.updatePayload = null;
    }

    _rows() {
      return state[this.tableName];
    }

    _applyFilters(rows) {
      return rows.filter((row) =>
        this.filters.every((filter) => {
          const value = row[filter.column];
          if (filter.type === 'eq') return value === filter.value;
          if (filter.type === 'gt') return String(value || '') > String(filter.value);
          return true;
        })
      );
    }

    select() {
      this.mode = 'select';
      return this;
    }

    eq(column, value) {
      this.filters.push({ type: 'eq', column, value });
      if (this.mode === 'update') {
        const rows = this._applyFilters(this._rows());
        rows.forEach((row) => Object.assign(row, this.updatePayload));
        return Promise.resolve({ data: rows, error: null });
      }
      return this;
    }

    gt(column, value) {
      this.filters.push({ type: 'gt', column, value });
      return this;
    }

    maybeSingle() {
      const rows = this._applyFilters(this._rows());
      return Promise.resolve({ data: rows[0] || null, error: null });
    }

    single() {
      const rows = this._applyFilters(this._rows());
      return Promise.resolve({ data: rows[0] || null, error: null });
    }

    insert(payload) {
      const batch = Array.isArray(payload) ? payload : [payload];
      const rows = this._rows();
      const inserted = batch.map((entry) => {
        const row = { ...entry };
        if (this.tableName === 'change_log' && !row.created_at) {
          tick += 1;
          row.created_at = new Date(1700000000000 + tick * 1000).toISOString();
        }
        rows.push(row);
        return row;
      });
      return Promise.resolve({ data: inserted, error: null });
    }

    update(payload) {
      this.mode = 'update';
      this.updatePayload = { ...payload };
      return this;
    }

    order(column, options = {}) {
      const rows = this._applyFilters(this._rows());
      const ascending = options.ascending !== false;
      rows.sort((a, b) => {
        const av = a[column];
        const bv = b[column];
        if (av === bv) return 0;
        if (ascending) return av > bv ? 1 : -1;
        return av > bv ? -1 : 1;
      });
      return Promise.resolve({ data: rows, error: null });
    }
  }

  return {
    state,
    from(tableName) {
      if (!Object.prototype.hasOwnProperty.call(state, tableName)) {
        throw new Error(`Tabela fake não implementada: ${tableName}`);
      }
      return new Query(tableName);
    }
  };
}

function loadSyncAppWithSupabase(fakeSupabase) {
  const supabasePath = require.resolve('../src/lib/supabase');
  const syncServicePath = require.resolve('../src/services/sync.service');
  const syncRoutesPath = require.resolve('../src/routes/sync.routes');

  delete require.cache[supabasePath];
  delete require.cache[syncServicePath];
  delete require.cache[syncRoutesPath];

  require.cache[supabasePath] = {
    id: supabasePath,
    filename: supabasePath,
    loaded: true,
    exports: { supabaseAdmin: fakeSupabase }
  };

  const syncRoutes = require('../src/routes/sync.routes');
  const { errorHandler } = require('../src/middleware/errorHandler');

  const app = express();
  app.use(express.json());
  app.use('/sync', syncRoutes);
  app.use(errorHandler);

  return app;
}

async function withServer(app, fn) {
  const server = await new Promise((resolve) => {
    const s = app.listen(0, '127.0.0.1', () => resolve(s));
  });

  try {
    const address = server.address();
    const baseUrl = `http://127.0.0.1:${address.port}`;
    await fn(baseUrl);
  } finally {
    await new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
  }
}

async function postJson(baseUrl, path, body) {
  const response = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body)
  });

  const payload = await response.json();
  return { status: response.status, payload };
}

test('sync API: push/pull cobre todas as operações do app + cenários de duplicate/conflict/not_found', async () => {
  const fakeSupabase = createFakeSupabase({
    routes: [
      { id: 101, status: 'CRIADA', version: 1, ativa: true },
      { id: 102, status: 'EM_ANDAMENTO', version: 2, ativa: true }
    ],
    route_waypoints: [
      { id: 1001, route_id: 101, seq_order: 1, status: 'PENDENTE', version: 1 },
      { id: 1002, route_id: 101, seq_order: 2, status: 'PENDENTE', version: 1 }
    ]
  });

  const app = loadSyncAppWithSupabase(fakeSupabase);

  await withServer(app, async (baseUrl) => {
    const mutationBatch = {
      mutations: [
        {
          deviceId: 'device-1',
          mutationId: 'm-import-route',
          entityType: 'route',
          entityId: 910,
          op: 'CREATE',
          baseVersion: 0,
          payload: { status: 'CRIADA', ativa: false, waypoints_count: 2 }
        },
        {
          deviceId: 'device-1',
          mutationId: 'm-start-route',
          entityType: 'route',
          entityId: 101,
          op: 'UPDATE',
          baseVersion: 1,
          payload: { status: 'EM_ANDAMENTO', ativa: true }
        },
        {
          deviceId: 'device-1',
          mutationId: 'm-reorder-wp-1001',
          entityType: 'route_waypoint',
          entityId: 1001,
          op: 'UPDATE',
          baseVersion: 1,
          payload: { seq_order: 2, status: 'REORDENADO' }
        },
        {
          deviceId: 'device-1',
          mutationId: 'm-reorder-wp-1002',
          entityType: 'route_waypoint',
          entityId: 1002,
          op: 'UPDATE',
          baseVersion: 1,
          payload: { seq_order: 1, status: 'REORDENADO' }
        },
        {
          deviceId: 'device-1',
          mutationId: 'm-finish-waypoint-1001',
          entityType: 'route_waypoint',
          entityId: 1001,
          op: 'UPDATE',
          baseVersion: 2,
          payload: { status: 'ENTREGUE' }
        },
        {
          deviceId: 'device-1',
          mutationId: 'm-finish-route',
          entityType: 'route',
          entityId: 101,
          op: 'UPDATE',
          baseVersion: 2,
          payload: { status: 'CONCLUÍDA', ativa: false }
        },
        {
          deviceId: 'device-1',
          mutationId: 'm-delete-route',
          entityType: 'route',
          entityId: 102,
          op: 'DELETE',
          baseVersion: 2,
          payload: { status: 'CANCELADA', ativa: false }
        }
      ]
    };

    const pushResponse = await postJson(baseUrl, '/sync/push', mutationBatch);
    assert.equal(pushResponse.status, 200);
    assert.equal(pushResponse.payload.ok, true);
    assert.equal(pushResponse.payload.results.length, mutationBatch.mutations.length);
    assert.deepEqual(
      pushResponse.payload.results.map((r) => r.status),
      ['APPLIED', 'APPLIED', 'APPLIED', 'APPLIED', 'APPLIED', 'APPLIED', 'APPLIED']
    );

    const duplicateResponse = await postJson(baseUrl, '/sync/push', {
      mutations: [mutationBatch.mutations[1]]
    });
    assert.equal(duplicateResponse.status, 200);
    assert.equal(duplicateResponse.payload.results[0].status, 'DUPLICATE');

    const conflictResponse = await postJson(baseUrl, '/sync/push', {
      mutations: [
        {
          deviceId: 'device-1',
          mutationId: 'm-conflict-route-101',
          entityType: 'route',
          entityId: 101,
          op: 'UPDATE',
          baseVersion: 1,
          payload: { status: 'EM_ANDAMENTO' }
        }
      ]
    });
    assert.equal(conflictResponse.status, 200);
    assert.equal(conflictResponse.payload.results[0].status, 'CONFLICT');
    assert.equal(conflictResponse.payload.results[0].serverVersion, 3);

    const notFoundResponse = await postJson(baseUrl, '/sync/push', {
      mutations: [
        {
          deviceId: 'device-1',
          mutationId: 'm-not-found-waypoint',
          entityType: 'route_waypoint',
          entityId: 9999,
          op: 'UPDATE',
          baseVersion: 1,
          payload: { status: 'ENTREGUE' }
        }
      ]
    });
    assert.equal(notFoundResponse.status, 200);
    assert.equal(notFoundResponse.payload.results[0].status, 'NOT_FOUND');

    const pullResponse = await postJson(baseUrl, '/sync/pull', {
      sinceTs: '1970-01-01T00:00:00.000Z'
    });

    assert.equal(pullResponse.status, 200);
    assert.equal(pullResponse.payload.ok, true);
    assert.equal(pullResponse.payload.changes.length, 7);

    const pullOps = pullResponse.payload.changes.map((change) => change.op);
    assert.ok(pullOps.includes('CREATE'));
    assert.ok(pullOps.includes('UPDATE'));
    assert.ok(pullOps.includes('DELETE'));

    const persistedRoute910 = fakeSupabase.state.routes.find((route) => route.id === 910);
    assert.ok(persistedRoute910);
    assert.equal(persistedRoute910.status, 'CRIADA');
    assert.equal(persistedRoute910.version, 1);

    const route101 = fakeSupabase.state.routes.find((route) => route.id === 101);
    assert.ok(route101);
    assert.equal(route101.status, 'CONCLUÍDA');
    assert.equal(route101.ativa, false);
    assert.equal(route101.version, 3);

    const route102 = fakeSupabase.state.routes.find((route) => route.id === 102);
    assert.ok(route102);
    assert.equal(route102.status, 'CANCELADA');
    assert.equal(route102.version, 3);

    const waypoint1001 = fakeSupabase.state.route_waypoints.find((waypoint) => waypoint.id === 1001);
    assert.ok(waypoint1001);
    assert.equal(waypoint1001.seq_order, 2);
    assert.equal(waypoint1001.status, 'ENTREGUE');
    assert.equal(waypoint1001.version, 3);
  });
});
