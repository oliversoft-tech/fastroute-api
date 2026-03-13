'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');

function pgError(message) {
  return { message };
}

function createFakeSupabase(initial = {}, options = {}) {
  const defaultTables = [
    'routes',
    'route_waypoints',
    'orders_import',
    'imports',
    'users',
    'mutations_applied',
    'change_log'
  ];

  const state = {};
  for (const tableName of defaultTables) {
    state[tableName] = [...(initial[tableName] || [])];
  }
  for (const [tableName, rows] of Object.entries(initial)) {
    if (!Object.prototype.hasOwnProperty.call(state, tableName)) {
      state[tableName] = [...(rows || [])];
    }
  }

  const missingTables = new Set(options.missingTables || []);
  const rlsSelect = new Set(options.rls?.select || []);
  const rlsInsert = new Set(options.rls?.insert || []);
  const rlsUpdate = new Set(options.rls?.update || []);

  const idCounters = {};
  for (const [tableName, rows] of Object.entries(state)) {
    let maxId = 0;
    for (const row of rows) {
      const id = Number(row?.id);
      if (Number.isFinite(id)) {
        maxId = Math.max(maxId, Math.trunc(id));
      }
    }
    idCounters[tableName] = maxId;
  }

  let tick = 0;

  function toError(value) {
    if (!value) {
      return null;
    }
    if (typeof value === 'string') {
      return pgError(value);
    }
    if (value && typeof value.message === 'string') {
      return value;
    }
    return pgError(String(value));
  }

  function missingTableError(tableName) {
    return pgError(`Could not find the table 'public.${tableName}' in the schema cache`);
  }

  function rlsError(tableName) {
    return pgError(`new row violates row-level security policy for table "${tableName}"`);
  }

  function projectRow(row, columns) {
    if (!columns || columns === '*') {
      return { ...row };
    }

    const selected = String(columns)
      .split(',')
      .map((column) => column.trim())
      .filter(Boolean);

    const projected = {};
    for (const column of selected) {
      if (Object.prototype.hasOwnProperty.call(row, column)) {
        projected[column] = row[column];
      }
    }
    return projected;
  }

  function ensureRouteConstraints(row) {
    for (const required of ['import_id', 'driver_id', 'cluster_id', 'status']) {
      if (row[required] === undefined || row[required] === null) {
        return pgError(`null value in column "${required}" of relation "routes" violates not-null constraint`);
      }
    }

    const importId = Number(row.import_id);
    const driverId = Number(row.driver_id);

    const hasImport = state.orders_import.some((entry) => Number(entry.id) === importId);
    if (!hasImport) {
      return pgError('insert or update on table "routes" violates foreign key constraint "route_import_id_fkey"');
    }

    const hasDriver = state.users.some((entry) => Number(entry.id) === driverId);
    if (!hasDriver) {
      return pgError('insert or update on table "routes" violates foreign key constraint "routes_user_id_fkey"');
    }

    return null;
  }

  function ensureOrdersImportConstraints(row) {
    if (row.user_id === undefined || row.user_id === null) {
      return pgError('null value in column "user_id" of relation "orders_import" violates not-null constraint');
    }

    const userId = Number(row.user_id);
    const hasUser = state.users.some((entry) => Number(entry.id) === userId);
    if (!hasUser) {
      return pgError('insert or update on table "orders_import" violates foreign key constraint "orders_import_user_id_fkey"');
    }

    return null;
  }

  function ensureWaypointConstraints(row) {
    if (row.status === undefined || row.status === null) {
      return pgError(
        'null value in column "status" of relation "route_waypoints" violates not-null constraint'
      );
    }

    if (row.route_id !== undefined && row.route_id !== null) {
      const routeId = Number(row.route_id);
      const hasRoute = state.routes.some((entry) => Number(entry.id) === routeId);
      if (!hasRoute) {
        return pgError('insert or update on table "route_waypoints" violates foreign key constraint "route_waypoints_route_id_fkey"');
      }
    }

    return null;
  }

  function ensureConstraints(tableName, row) {
    if (!options.enforceConstraints) {
      return null;
    }

    if (tableName === 'routes') {
      return ensureRouteConstraints(row);
    }
    if (tableName === 'orders_import') {
      return ensureOrdersImportConstraints(row);
    }
    if (tableName === 'route_waypoints') {
      return ensureWaypointConstraints(row);
    }

    return null;
  }

  class Query {
    constructor(tableName) {
      this.tableName = tableName;
      this.filters = [];
      this.mode = 'select';
      this.selectedColumns = '*';
      this.orderBy = null;
      this.limitCount = null;
      this.insertBatch = [];
      this.updatePayload = {};
    }

    _rows() {
      if (!Object.prototype.hasOwnProperty.call(state, this.tableName)) {
        state[this.tableName] = [];
      }
      return state[this.tableName];
    }

    _applyFilters(rows) {
      return rows.filter((row) =>
        this.filters.every((filter) => {
          const value = row[filter.column];
          if (filter.type === 'eq') {
            return value === filter.value;
          }
          if (filter.type === 'gt') {
            return String(value || '') > String(filter.value);
          }
          return true;
        })
      );
    }

    _selectError() {
      if (missingTables.has(this.tableName)) {
        return missingTableError(this.tableName);
      }
      if (rlsSelect.has(this.tableName)) {
        return rlsError(this.tableName);
      }

      const hookError = toError(
        options.onSelect?.({
          tableName: this.tableName,
          filters: [...this.filters],
          selectedColumns: this.selectedColumns,
          state
        })
      );
      if (hookError) {
        return hookError;
      }
      return null;
    }

    _insertError(row) {
      if (missingTables.has(this.tableName)) {
        return missingTableError(this.tableName);
      }
      if (rlsInsert.has(this.tableName)) {
        return rlsError(this.tableName);
      }

      const hookError = toError(
        options.onInsert?.({ tableName: this.tableName, row: { ...row }, state })
      );
      if (hookError) {
        return hookError;
      }

      return ensureConstraints(this.tableName, row);
    }

    _updateError(currentRow, nextRow) {
      if (missingTables.has(this.tableName)) {
        return missingTableError(this.tableName);
      }
      if (rlsUpdate.has(this.tableName)) {
        return rlsError(this.tableName);
      }

      const hookError = toError(
        options.onUpdate?.({
          tableName: this.tableName,
          currentRow: { ...currentRow },
          nextRow: { ...nextRow },
          payload: { ...this.updatePayload },
          state
        })
      );
      if (hookError) {
        return hookError;
      }

      return ensureConstraints(this.tableName, nextRow);
    }

    async _executeSelect() {
      const error = this._selectError();
      if (error) {
        return { data: null, error };
      }

      let rows = this._applyFilters(this._rows());

      if (this.orderBy) {
        const { column, ascending } = this.orderBy;
        rows = [...rows].sort((a, b) => {
          const av = a[column];
          const bv = b[column];
          if (av === bv) return 0;
          if (ascending) return av > bv ? 1 : -1;
          return av > bv ? -1 : 1;
        });
      }

      if (Number.isFinite(this.limitCount) && this.limitCount >= 0) {
        rows = rows.slice(0, this.limitCount);
      }

      return {
        data: rows.map((row) => projectRow(row, this.selectedColumns)),
        error: null
      };
    }

    async _executeInsert(single = false) {
      const rows = this._rows();
      const inserted = [];

      for (const entry of this.insertBatch) {
        const row = { ...entry };

        if ((row.id === undefined || row.id === null) && this.tableName !== 'mutations_applied') {
          idCounters[this.tableName] = (idCounters[this.tableName] || 0) + 1;
          row.id = idCounters[this.tableName];
        }

        if (this.tableName === 'change_log' && !row.created_at) {
          tick += 1;
          row.created_at = new Date(1700000000000 + tick * 1000).toISOString();
        }

        const error = this._insertError(row);
        if (error) {
          return { data: null, error };
        }

        rows.push(row);
        inserted.push(row);
      }

      const projected = inserted.map((row) => projectRow(row, this.selectedColumns));
      return {
        data: single ? projected[0] || null : projected,
        error: null
      };
    }

    async _executeUpdate() {
      if (missingTables.has(this.tableName)) {
        return { data: null, error: missingTableError(this.tableName) };
      }
      if (rlsUpdate.has(this.tableName)) {
        return { data: null, error: rlsError(this.tableName) };
      }

      const rows = this._applyFilters(this._rows());
      for (const row of rows) {
        const nextRow = { ...row, ...this.updatePayload };
        const error = this._updateError(row, nextRow);
        if (error) {
          return { data: null, error };
        }
      }

      rows.forEach((row) => Object.assign(row, this.updatePayload));
      return { data: rows.map((row) => projectRow(row, this.selectedColumns)), error: null };
    }

    select(columns = '*') {
      this.selectedColumns = columns;
      return this;
    }

    eq(column, value) {
      this.filters.push({ type: 'eq', column, value });
      return this;
    }

    gt(column, value) {
      this.filters.push({ type: 'gt', column, value });
      return this;
    }

    limit(value) {
      const parsed = Number(value);
      this.limitCount = Number.isFinite(parsed) ? parsed : null;
      return this;
    }

    order(column, optionsArg = {}) {
      this.orderBy = {
        column,
        ascending: optionsArg.ascending !== false
      };
      return this;
    }

    insert(payload) {
      this.mode = 'insert';
      this.insertBatch = Array.isArray(payload) ? payload : [payload];
      return this;
    }

    update(payload) {
      this.mode = 'update';
      this.updatePayload = { ...payload };
      return this;
    }

    async maybeSingle() {
      const { data, error } = await this._executeSelect();
      if (error) {
        return { data: null, error };
      }
      return { data: data[0] || null, error: null };
    }

    async single() {
      if (this.mode === 'insert') {
        return this._executeInsert(true);
      }

      const { data, error } = await this._executeSelect();
      if (error) {
        return { data: null, error };
      }
      return { data: data[0] || null, error: null };
    }

    then(resolve, reject) {
      let pending;
      if (this.mode === 'insert') {
        pending = this._executeInsert(false);
      } else if (this.mode === 'update') {
        pending = this._executeUpdate();
      } else {
        pending = this._executeSelect();
      }
      return pending.then(resolve, reject);
    }
  }

  const storageUploads = [];

  return {
    state,
    storageUploads,
    from(tableName) {
      return new Query(tableName);
    },
    storage: {
      from(bucket) {
        return {
          async upload(objectPath, body, uploadOptions = {}) {
            const asString = typeof body === 'string' ? body : null;
            const asBuffer = Buffer.isBuffer(body) ? body : null;
            const byteLength = asBuffer
              ? asBuffer.length
              : asString
                ? Buffer.byteLength(asString)
                : Number(body?.byteLength || 0);
            const uploadEntry = {
              bucket,
              objectPath,
              contentType: uploadOptions.contentType,
              upsert: Boolean(uploadOptions.upsert),
              byteLength
            };
            storageUploads.push(uploadEntry);

            const hookError = toError(options.onStorageUpload?.(uploadEntry));
            if (hookError) {
              return { data: null, error: hookError };
            }

            return { data: { path: objectPath }, error: null };
          }
        };
      }
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

async function postJson(baseUrl, path, body, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(options.headers || {}) },
    body: JSON.stringify(body)
  });

  const payload = await response.json();
  return { status: response.status, payload };
}

async function pushOne(baseUrl, mutation) {
  return postJson(baseUrl, '/sync/push', { mutations: [mutation] });
}

function buildUnsignedJwt(payload) {
  const encode = (value) => Buffer.from(JSON.stringify(value)).toString('base64url');
  return `${encode({ alg: 'none', typ: 'JWT' })}.${encode(payload)}.`;
}

test('sync API: push/pull cobre operações principais + duplicate/conflict/not_found', async () => {
  const fakeSupabase = createFakeSupabase({
    orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }],
    users: [{ id: 77, auth_user_id: '77' }],
    routes: [
      { id: 101, import_id: 700, driver_id: 77, cluster_id: 1, status: 'CRIADA', version: 1, ativa: true },
      {
        id: 102,
        import_id: 700,
        driver_id: 77,
        cluster_id: 1,
        status: 'EM_ANDAMENTO',
        version: 2,
        ativa: true
      }
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
          payload: {
            import_id: 700,
            auth_user_id: '77',
            status: 'CRIADA',
            ativa: false,
            waypoint_count: 2,
            waypoints_count: 2,
            waypointsCount: 2
          }
        },
        {
          deviceId: 'device-1',
          mutationId: 'm-import-waypoint',
          entityType: 'route_waypoint',
          entityId: 9910,
          op: 'CREATE',
          baseVersion: 0,
          payload: { route_id: 910, seq_order: 1, status: 'PENDENTE' }
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
    assert.deepEqual(
      pushResponse.payload.results.map((result) => result.status),
      ['APPLIED', 'APPLIED', 'APPLIED', 'APPLIED', 'APPLIED', 'APPLIED', 'APPLIED', 'APPLIED']
    );

    const duplicateResponse = await postJson(baseUrl, '/sync/push', {
      mutations: [mutationBatch.mutations[1]]
    });
    assert.equal(duplicateResponse.status, 200);
    assert.equal(duplicateResponse.payload.results[0].status, 'DUPLICATE');

    const conflictResponse = await pushOne(baseUrl, {
      deviceId: 'device-1',
      mutationId: 'm-conflict-route-101',
      entityType: 'route',
      entityId: 101,
      op: 'UPDATE',
      baseVersion: 1,
      payload: { status: 'EM_ANDAMENTO' }
    });
    assert.equal(conflictResponse.status, 200);
    assert.equal(conflictResponse.payload.results[0].status, 'CONFLICT');

    const notFoundResponse = await pushOne(baseUrl, {
      deviceId: 'device-1',
      mutationId: 'm-not-found-waypoint',
      entityType: 'route_waypoint',
      entityId: 9999,
      op: 'UPDATE',
      baseVersion: 1,
      payload: { status: 'ENTREGUE' }
    });
    assert.equal(notFoundResponse.status, 200);
    assert.equal(notFoundResponse.payload.results[0].status, 'NOT_FOUND');

    const pullResponse = await postJson(baseUrl, '/sync/pull', {
      sinceTs: '1970-01-01T00:00:00.000Z'
    });

    assert.equal(pullResponse.status, 200);
    assert.equal(pullResponse.payload.ok, true);
    assert.equal(pullResponse.payload.changes.length, 8);

    const persistedRoute910 = fakeSupabase.state.routes.find((route) => route.id === 910);
    assert.ok(persistedRoute910);
    assert.equal(persistedRoute910.import_id, 700);
    assert.equal(persistedRoute910.driver_id, 77);
    assert.equal(persistedRoute910.cluster_id, 1);
    assert.equal(persistedRoute910.waypoint_count, undefined);
    assert.equal(persistedRoute910.waypoints_count, undefined);
    assert.equal(persistedRoute910.waypointsCount, undefined);
  });
});

test('sync API: route CREATE define campos obrigatórios/defaults do modelo resolvendo driver por import_id', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 77, auth_user_id: '77' }],
      orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }]
    },
    { enforceConstraints: true }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-2',
      mutationId: 'm-route-required-defaults',
      entityType: 'route',
      entityId: 920,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        import_id: 700
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const route = fakeSupabase.state.routes.find((item) => item.id === 920);
    assert.ok(route);
    assert.equal(route.import_id, 700);
    assert.equal(route.driver_id, 77);
    assert.equal(route.cluster_id, 1);
    assert.equal(route.status, 'CRIADA');
    assert.equal(route.ativa, false);
    assert.ok(route.created_at);
    assert.equal(route.version, 1);
  });
});

test('sync API: route CREATE falha com 400 quando driver_id não pode ser resolvido', async () => {
  const fakeSupabase = createFakeSupabase();

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-3',
      mutationId: 'm-route-missing-driver',
      entityType: 'route',
      entityId: 930,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        import_id: 700,
        status: 'CRIADA'
      }
    });

    assert.equal(response.status, 400);
    assert.equal(response.payload.ok, false);
    assert.match(response.payload.error, /driver_id/);
  });
});

test('sync API: route CREATE resolve driver_id pelo auth_user_id uuid em users', async () => {
  const authUserId = '11111111-1111-1111-1111-111111111111';
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 88, auth_user_id: authUserId }],
      orders_import: [{ id: 701, user_id: 88, status: 'SEM_ROTA' }]
    },
    { enforceConstraints: true }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-4',
      mutationId: 'm-route-auth-user-resolution',
      entityType: 'route',
      entityId: 940,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        import_id: 701,
        auth_user_id: authUserId,
        status: 'CRIADA'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const route = fakeSupabase.state.routes.find((item) => item.id === 940);
    assert.ok(route);
    assert.equal(route.driver_id, 88);
  });
});

test('sync API: route CREATE resolve driver_id pelo JWT Authorization quando payload não contém usuário', async () => {
  const authUserId = '33333333-3333-3333-3333-333333333333';
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 89, auth_user_id: authUserId }],
      orders_import: []
    },
    { enforceConstraints: true }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const token = buildUnsignedJwt({ sub: authUserId });
    const response = await postJson(
      baseUrl,
      '/sync/push',
      {
        mutations: [
          {
            deviceId: 'device-4jwt',
            mutationId: 'm-route-auth-header-fallback',
            entityType: 'route',
            entityId: 944,
            op: 'CREATE',
            baseVersion: 0,
            payload: {
              import_id: 705,
              status: 'CRIADA'
            }
          }
        ]
      },
      {
        headers: {
          authorization: `Bearer ${token}`
        }
      }
    );

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const route = fakeSupabase.state.routes.find((item) => item.id === 944);
    assert.ok(route);
    assert.equal(route.driver_id, 89);
    assert.equal(fakeSupabase.state.orders_import.length, 1);
    assert.equal(fakeSupabase.state.orders_import[0].user_id, 89);
  });
});

test('sync API: route CREATE resolve driver_id por user_id do import quando orders_import.driver_id não existe', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 77, auth_user_id: '11111111-1111-1111-1111-111111111111' }],
      orders_import: [{ id: 702, user_id: 77, status: 'SEM_ROTA' }]
    },
    {
      enforceConstraints: true,
      onSelect: ({ tableName, selectedColumns }) => {
        if (tableName === 'orders_import' && String(selectedColumns).includes('driver_id')) {
          return 'orders_import.driver_id does not exist';
        }
        return null;
      }
    }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-4b',
      mutationId: 'm-route-import-user-fallback',
      entityType: 'route',
      entityId: 941,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        import_id: 702,
        status: 'CRIADA'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const route = fakeSupabase.state.routes.find((item) => item.id === 941);
    assert.ok(route);
    assert.equal(route.driver_id, 77);
  });
});

test('sync API: route CREATE resolve driver_id via rotas já existentes do mesmo import quando tabela de import não responde', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 77, auth_user_id: '77' }],
      orders_import: [{ id: 706, user_id: 77, status: 'SEM_ROTA' }],
      routes: [{ id: 7001, import_id: 706, driver_id: 77, cluster_id: 1, status: 'CRIADA', version: 1 }]
    },
    {
      enforceConstraints: true,
      missingTables: ['orders_import', 'imports']
    }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-4e',
      mutationId: 'm-route-existing-route-driver-fallback',
      entityType: 'route',
      entityId: 945,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        import_id: 706,
        status: 'CRIADA'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const route = fakeSupabase.state.routes.find((item) => item.id === 945);
    assert.ok(route);
    assert.equal(route.driver_id, 77);
  });
});

test('sync API: route CREATE resolve driver_id por fallback de usuário único quando payload/import não informam motorista', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 91, auth_user_id: '44444444-4444-4444-4444-444444444444' }],
      orders_import: []
    },
    { enforceConstraints: true }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-4f',
      mutationId: 'm-route-single-user-driver-fallback',
      entityType: 'route',
      entityId: 946,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        import_id: 9999,
        status: 'CRIADA'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const route = fakeSupabase.state.routes.find((item) => item.id === 946);
    assert.ok(route);
    assert.equal(route.driver_id, 91);
  });
});

test('sync API: route CREATE resolve driver_id quando import.user_id está em auth_user_id (uuid)', async () => {
  const authUserId = '22222222-2222-2222-2222-222222222222';
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 79, auth_user_id: authUserId }],
      orders_import: [{ id: 704, user_id: authUserId, status: 'SEM_ROTA' }]
    },
    { enforceConstraints: true }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-4d',
      mutationId: 'm-route-import-auth-user-fallback',
      entityType: 'route',
      entityId: 943,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        import_id: 704,
        status: 'CRIADA'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const route = fakeSupabase.state.routes.find((item) => item.id === 943);
    assert.ok(route);
    assert.equal(route.driver_id, 79);
  });
});

test('sync API: route CREATE resolve driver_id via waypoint CREATE do mesmo batch', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 77, auth_user_id: '77' }],
      orders_import: [{ id: 703, user_id: 77, status: 'SEM_ROTA' }]
    },
    { enforceConstraints: true }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await postJson(baseUrl, '/sync/push', {
      mutations: [
        {
          deviceId: 'device-4c',
          mutationId: 'm-route-create-hint',
          entityType: 'route',
          entityId: 942,
          op: 'CREATE',
          baseVersion: 0,
          payload: {
            import_id: 703,
            status: 'CRIADA'
          }
        },
        {
          deviceId: 'device-4c',
          mutationId: 'm-waypoint-create-hint',
          entityType: 'route_waypoint',
          entityId: 94201,
          op: 'CREATE',
          baseVersion: 0,
          payload: {
            route_id: 942,
            user_id: 77,
            seq_order: 1,
            status: 'PENDENTE'
          }
        }
      ]
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');
    assert.equal(response.payload.results[1].status, 'APPLIED');

    const route = fakeSupabase.state.routes.find((item) => item.id === 942);
    assert.ok(route);
    assert.equal(route.driver_id, 77);
  });
});

test('sync API: route CREATE remapeia import_id inexistente criando orders_import para o motorista', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 77, auth_user_id: '77' }],
      orders_import: []
    },
    { enforceConstraints: true }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-5',
      mutationId: 'm-route-create-import-map',
      entityType: 'route',
      entityId: 950,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        import_id: 999,
        driver_id: 77,
        status: 'CRIADA'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    assert.equal(fakeSupabase.state.orders_import.length, 1);
    const createdImport = fakeSupabase.state.orders_import[0];
    assert.equal(createdImport.user_id, 77);

    const route = fakeSupabase.state.routes.find((item) => item.id === 950);
    assert.ok(route);
    assert.equal(route.import_id, createdImport.id);
  });
});

test('sync API: route CREATE expõe erro de FK de import_id quando tabelas de import não existem', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 77, auth_user_id: '77' }],
      orders_import: []
    },
    {
      enforceConstraints: true,
      missingTables: ['orders_import', 'imports']
    }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-6',
      mutationId: 'm-route-fk-import',
      entityType: 'route',
      entityId: 960,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        import_id: 700,
        driver_id: 77,
        status: 'CRIADA'
      }
    });

    assert.equal(response.status, 500);
    assert.equal(response.payload.ok, false);
    assert.match(response.payload.error, /route_import_id_fkey/);
  });
});

test('sync API: route CREATE expõe erro de FK de driver_id', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      users: [],
      orders_import: [{ id: 700, user_id: 10, status: 'SEM_ROTA' }]
    },
    { enforceConstraints: true }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-7',
      mutationId: 'm-route-fk-driver',
      entityType: 'route',
      entityId: 970,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        import_id: 700,
        driver_id: 77,
        status: 'CRIADA'
      }
    });

    assert.equal(response.status, 500);
    assert.equal(response.payload.ok, false);
    assert.match(response.payload.error, /routes_user_id_fkey/);
  });
});

test('sync API: route UPDATE não altera import_id (imutável no sync)', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      users: [{ id: 77, auth_user_id: '77' }],
      orders_import: [
        { id: 700, user_id: 77, status: 'SEM_ROTA' },
        { id: 701, user_id: 77, status: 'SEM_ROTA' }
      ],
      routes: [
        {
          id: 980,
          import_id: 700,
          driver_id: 77,
          cluster_id: 1,
          status: 'CRIADA',
          ativa: true,
          version: 1
        }
      ]
    },
    { enforceConstraints: true }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-8',
      mutationId: 'm-route-update-import-immutable',
      entityType: 'route',
      entityId: 980,
      op: 'UPDATE',
      baseVersion: 1,
      payload: {
        import_id: 701,
        status: 'EM_ANDAMENTO'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const route = fakeSupabase.state.routes.find((item) => item.id === 980);
    assert.ok(route);
    assert.equal(route.import_id, 700);
    assert.equal(route.status, 'EM_ANDAMENTO');
    assert.equal(route.version, 2);
  });
});

test('sync API: route_waypoint CREATE salva payload completo no change_log e persiste apenas colunas suportadas', async () => {
  const fakeSupabase = createFakeSupabase({
    routes: [{ id: 990, import_id: 700, driver_id: 77, cluster_id: 1, status: 'CRIADA', version: 1 }],
    orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }],
    users: [{ id: 77, auth_user_id: '77' }]
  });

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-9',
      mutationId: 'm-waypoint-create-rich-payload',
      entityType: 'route_waypoint',
      entityId: 9901,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        route_id: 990,
        seq_order: 1,
        status: 'PENDENTE',
        detailed_address: 'Rua 1, 123',
        lat: '-23.5',
        long: '-46.6',
        title: 'Casa',
        subtitle: 'Portao azul'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const waypoint = fakeSupabase.state.route_waypoints.find((item) => item.id === 9901);
    assert.ok(waypoint);
    assert.equal(waypoint.route_id, 990);
    assert.equal(waypoint.status, 'PENDENTE');
    assert.equal(waypoint.detailed_address, undefined);
    assert.equal(waypoint.title, undefined);

    const change = fakeSupabase.state.change_log.find(
      (item) => item.entity_type === 'route_waypoint' && item.entity_id === 9901
    );
    assert.ok(change);
    assert.equal(change.payload.detailed_address, 'Rua 1, 123');
    assert.equal(change.payload.title, 'Casa');
    assert.equal(change.payload.subtitle, 'Portao azul');
  });
});

test('sync API: route_waypoint CREATE remove colunas faltantes do schema por tentativas até aplicar', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      routes: [{ id: 991, import_id: 700, driver_id: 77, cluster_id: 1, status: 'CRIADA', version: 1 }],
      orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }],
      users: [{ id: 77, auth_user_id: '77' }]
    },
    {
      enforceConstraints: true,
      onInsert: ({ tableName, row }) => {
        if (tableName !== 'route_waypoints') {
          return null;
        }
        if (Object.prototype.hasOwnProperty.call(row, 'foo')) {
          return "Could not find the 'foo' column of 'route_waypoints' in the schema cache";
        }
        if (Object.prototype.hasOwnProperty.call(row, 'bar')) {
          return 'column "bar" does not exist';
        }
        return null;
      }
    }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-10',
      mutationId: 'm-waypoint-create-missing-columns',
      entityType: 'route_waypoint',
      entityId: 9911,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        route_id: 991,
        seq_order: 1,
        status: 'PENDENTE',
        foo: 'x',
        bar: 'y'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const waypoint = fakeSupabase.state.route_waypoints.find((item) => item.id === 9911);
    assert.ok(waypoint);
    assert.equal(waypoint.foo, undefined);
    assert.equal(waypoint.bar, undefined);
  });
});

test('sync API: route_waypoint UPDATE remove colunas faltantes do schema por tentativas até aplicar', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      route_waypoints: [{ id: 9921, route_id: 992, seq_order: 1, status: 'PENDENTE', version: 1 }],
      routes: [{ id: 992, import_id: 700, driver_id: 77, cluster_id: 1, status: 'CRIADA', version: 1 }],
      orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }],
      users: [{ id: 77, auth_user_id: '77' }]
    },
    {
      enforceConstraints: true,
      onUpdate: ({ tableName, nextRow }) => {
        if (tableName !== 'route_waypoints') {
          return null;
        }
        if (Object.prototype.hasOwnProperty.call(nextRow, 'extra_a')) {
          return "Could not find the 'extra_a' column of 'route_waypoints' in the schema cache";
        }
        if (Object.prototype.hasOwnProperty.call(nextRow, 'extra_b')) {
          return 'column "extra_b" does not exist';
        }
        return null;
      }
    }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-11',
      mutationId: 'm-waypoint-update-missing-columns',
      entityType: 'route_waypoint',
      entityId: 9921,
      op: 'UPDATE',
      baseVersion: 1,
      payload: {
        status: 'ENTREGUE',
        extra_a: '1',
        extra_b: '2'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const waypoint = fakeSupabase.state.route_waypoints.find((item) => item.id === 9921);
    assert.ok(waypoint);
    assert.equal(waypoint.status, 'ENTREGUE');
    assert.equal(waypoint.version, 2);
    assert.equal(waypoint.extra_a, undefined);
    assert.equal(waypoint.extra_b, undefined);
  });
});

test('sync API: route_waypoint UPDATE persiste foto em waypoint_delivery_photo quando payload inclui metadados', async () => {
  const fakeSupabase = createFakeSupabase({
    route_waypoints: [{ id: 9951, route_id: 995, seq_order: 1, status: 'PENDENTE', version: 1 }],
    routes: [{ id: 995, import_id: 700, driver_id: 77, cluster_id: 1, status: 'EM_ANDAMENTO', version: 1 }],
    orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }],
    users: [{ id: 77, auth_user_id: '77' }]
  });

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-11b',
      mutationId: 'm-waypoint-update-photo-persist',
      entityType: 'route_waypoint',
      entityId: 9951,
      op: 'UPDATE',
      baseVersion: 1,
      payload: {
        route_id: 995,
        status: 'CONCLUIDO',
        file_name: 'photo_9951.jpg',
        user_id: 77,
        image_uri: 'file:///var/mobile/Containers/Data/Application/XYZ/Documents/delivery-photos/photo_9951.jpg',
        file_size_bytes: 123456
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const photoRows = fakeSupabase.state.waypoint_delivery_photo || [];
    assert.equal(photoRows.length, 1);
    assert.deepEqual(photoRows[0], {
      id: 1,
      waypoint_id: 9951,
      route_id: 995,
      user_id: 77,
      bucket: 'delivery-photos',
      object_path: '995/9951/photo_9951.jpg',
      file_name: 'photo_9951.jpg',
      file_size_bytes: 123456
    });
  });
});

test('sync API: route_waypoint UPDATE faz upload do image_base64 e persiste metadados da foto', async () => {
  const fakeSupabase = createFakeSupabase({
    route_waypoints: [{ id: 9952, route_id: 995, seq_order: 2, status: 'PENDENTE', version: 1 }],
    routes: [{ id: 995, import_id: 700, driver_id: 77, cluster_id: 1, status: 'EM_ANDAMENTO', version: 1 }],
    orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }],
    users: [{ id: 77, auth_user_id: '77' }]
  });

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-11bb',
      mutationId: 'm-waypoint-update-photo-base64-upload',
      entityType: 'route_waypoint',
      entityId: 9952,
      op: 'UPDATE',
      baseVersion: 1,
      payload: {
        route_id: 995,
        status: 'CONCLUIDO',
        file_name: 'photo_9952.jpg',
        user_id: 77,
        image_base64: Buffer.from('hello-world').toString('base64'),
        image_mime_type: 'image/jpeg'
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    assert.equal(fakeSupabase.storageUploads.length, 1);
    assert.deepEqual(fakeSupabase.storageUploads[0], {
      bucket: 'delivery-photos',
      objectPath: '995/9952/photo_9952.jpg',
      contentType: 'image/jpeg',
      upsert: true,
      byteLength: 11
    });

    const photoRows = fakeSupabase.state.waypoint_delivery_photo || [];
    assert.equal(photoRows.length, 1);
    assert.deepEqual(photoRows[0], {
      id: 1,
      waypoint_id: 9952,
      route_id: 995,
      user_id: 77,
      bucket: 'delivery-photos',
      object_path: '995/9952/photo_9952.jpg',
      file_name: 'photo_9952.jpg',
      file_size_bytes: 11
    });
  });
});

test('sync API: route_waypoint UPDATE atualiza foto existente sem duplicar waypoint_delivery_photo', async () => {
  const fakeSupabase = createFakeSupabase({
    route_waypoints: [{ id: 9961, route_id: 996, seq_order: 1, status: 'PENDENTE', version: 3 }],
    routes: [{ id: 996, import_id: 700, driver_id: 77, cluster_id: 1, status: 'EM_ANDAMENTO', version: 1 }],
    orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }],
    users: [{ id: 77, auth_user_id: '77' }],
    waypoint_delivery_photo: [
      {
        id: 10,
        waypoint_id: 9961,
        route_id: 996,
        user_id: 77,
        bucket: 'delivery-photos',
        object_path: 'old_photo.jpg',
        file_name: 'old_photo.jpg',
        file_size_bytes: 111
      }
    ]
  });

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-11c',
      mutationId: 'm-waypoint-update-photo-overwrite',
      entityType: 'route_waypoint',
      entityId: 9961,
      op: 'UPDATE',
      baseVersion: 3,
      payload: {
        route_id: 996,
        status: 'CONCLUIDO',
        file_name: 'new_photo.jpg',
        user_id: 77,
        object_path: 'delivery/new_photo.jpg',
        file_size_bytes: 222
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const photoRows = fakeSupabase.state.waypoint_delivery_photo || [];
    assert.equal(photoRows.length, 1);
    assert.deepEqual(photoRows[0], {
      id: 10,
      waypoint_id: 9961,
      route_id: 996,
      user_id: 77,
      bucket: 'delivery-photos',
      object_path: '996/9961/new_photo.jpg',
      file_name: 'new_photo.jpg',
      file_size_bytes: 222
    });
  });
});

test('sync API: route_waypoint UPDATE canonicaliza object_path da foto para evitar bloqueio de RLS no storage', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      route_waypoints: [{ id: 9981, route_id: 998, seq_order: 1, status: 'PENDENTE', version: 1 }],
      routes: [{ id: 998, import_id: 700, driver_id: 77, cluster_id: 1, status: 'EM_ANDAMENTO', version: 1 }],
      orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }],
      users: [{ id: 77, auth_user_id: '77' }]
    },
    {
      onStorageUpload: ({ objectPath }) => {
        if (!String(objectPath || '').includes('/')) {
          return pgError('new row violates row-level security policy for table "objects"');
        }
        return null;
      }
    }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-11d',
      mutationId: 'm-waypoint-update-photo-canonical-path',
      entityType: 'route_waypoint',
      entityId: 9981,
      op: 'UPDATE',
      baseVersion: 1,
      payload: {
        route_id: 998,
        status: 'CONCLUIDO',
        file_name: 'photo_sync.jpg',
        user_id: 77,
        object_path: 'photo_sync.jpg',
        image_base64: Buffer.from('sync-photo-bytes').toString('base64')
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    assert.equal(fakeSupabase.storageUploads.length, 1);
    assert.equal(fakeSupabase.storageUploads[0].objectPath, '998/9981/photo_sync.jpg');

    const photoRows = fakeSupabase.state.waypoint_delivery_photo || [];
    assert.equal(photoRows.length, 1);
    assert.equal(photoRows[0].object_path, '998/9981/photo_sync.jpg');
  });
});

test('sync API: route_waypoint CREATE falha quando status obrigatório está ausente', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      routes: [{ id: 993, import_id: 700, driver_id: 77, cluster_id: 1, status: 'CRIADA', version: 1 }],
      orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }],
      users: [{ id: 77, auth_user_id: '77' }]
    },
    { enforceConstraints: true }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-12',
      mutationId: 'm-waypoint-missing-status',
      entityType: 'route_waypoint',
      entityId: 9931,
      op: 'CREATE',
      baseVersion: 0,
      payload: {
        route_id: 993,
        seq_order: 1
      }
    });

    assert.equal(response.status, 500);
    assert.equal(response.payload.ok, false);
    assert.match(response.payload.error, /status/);
    assert.match(response.payload.error, /not-null constraint/);
  });
});

test('sync API: push ignora erros de RLS em mutations_applied/change_log sem quebrar aplicação', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      routes: [{ id: 994, import_id: 700, driver_id: 77, cluster_id: 1, status: 'CRIADA', version: 1 }],
      orders_import: [{ id: 700, user_id: 77, status: 'SEM_ROTA' }],
      users: [{ id: 77, auth_user_id: '77' }]
    },
    {
      rls: {
        select: ['mutations_applied'],
        insert: ['mutations_applied', 'change_log']
      }
    }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await pushOne(baseUrl, {
      deviceId: 'device-13',
      mutationId: 'm-route-rls-safe',
      entityType: 'route',
      entityId: 994,
      op: 'UPDATE',
      baseVersion: 1,
      payload: {
        status: 'EM_ANDAMENTO',
        ativa: true
      }
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.results[0].status, 'APPLIED');

    const route = fakeSupabase.state.routes.find((item) => item.id === 994);
    assert.ok(route);
    assert.equal(route.status, 'EM_ANDAMENTO');
    assert.equal(route.version, 2);
    assert.equal(fakeSupabase.state.change_log.length, 0);
    assert.equal(fakeSupabase.state.mutations_applied.length, 0);
  });
});

test('sync API: pull retorna vazio quando change_log está bloqueado por RLS', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      change_log: [
        {
          id: 1,
          entity_type: 'route',
          entity_id: 10,
          op: 'UPDATE',
          version: 2,
          created_at: '2026-03-08T21:00:00.000Z'
        }
      ]
    },
    {
      rls: {
        select: ['change_log']
      }
    }
  );

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await postJson(baseUrl, '/sync/pull', {
      sinceTs: '1970-01-01T00:00:00.000Z'
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.ok, true);
    assert.deepEqual(response.payload.changes, []);
  });
});

test('sync API: pull respeita sinceTs e ordenação por created_at asc', async () => {
  const fakeSupabase = createFakeSupabase({
    change_log: [
      {
        id: 3,
        entity_type: 'route',
        entity_id: 30,
        op: 'UPDATE',
        version: 3,
        created_at: '2026-03-08T10:30:00.000Z'
      },
      {
        id: 1,
        entity_type: 'route',
        entity_id: 10,
        op: 'CREATE',
        version: 1,
        created_at: '2026-03-08T09:00:00.000Z'
      },
      {
        id: 2,
        entity_type: 'route_waypoint',
        entity_id: 20,
        op: 'UPDATE',
        version: 2,
        created_at: '2026-03-08T10:00:00.000Z'
      }
    ]
  });

  const app = loadSyncAppWithSupabase(fakeSupabase);
  await withServer(app, async (baseUrl) => {
    const response = await postJson(baseUrl, '/sync/pull', {
      sinceTs: '2026-03-08T09:15:00.000Z'
    });

    assert.equal(response.status, 200);
    assert.equal(response.payload.ok, true);
    assert.deepEqual(
      response.payload.changes.map((item) => item.id),
      [2, 3]
    );
  });
});
