'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

function createFakeSupabase(state, options = {}) {
  const missingUrlPhotoColumn = Boolean(options.missingUrlPhotoColumn);
  const tables = {
    route_waypoints: [...(state.route_waypoints || [])],
    waypoint_delivery_photo: [...(state.waypoint_delivery_photo || [])]
  };

  class Query {
    constructor(tableName) {
      this.tableName = tableName;
      this.filters = [];
      this.selectedColumns = '*';
      this.orderBy = null;
      this.limitCount = null;
    }

    select(columns = '*') {
      this.selectedColumns = columns;
      return this;
    }

    eq(column, value) {
      this.filters.push({ type: 'eq', column, value });
      return this;
    }

    order(column, optionsArg = {}) {
      this.orderBy = {
        column,
        ascending: optionsArg.ascending !== false
      };
      return this;
    }

    limit(value) {
      const parsed = Number(value);
      this.limitCount = Number.isFinite(parsed) ? parsed : null;
      return this;
    }

    _applyFilters(rows) {
      return rows.filter((row) => this.filters.every((filter) => row[filter.column] === filter.value));
    }

    _project(row) {
      if (!row || this.selectedColumns === '*') {
        return row ? { ...row } : null;
      }
      const selected = String(this.selectedColumns)
        .split(',')
        .map((column) => column.trim())
        .filter(Boolean);
      const output = {};
      for (const column of selected) {
        if (Object.prototype.hasOwnProperty.call(row, column)) {
          output[column] = row[column];
        }
      }
      return output;
    }

    async maybeSingle() {
      if (
        this.tableName === 'route_waypoints' &&
        missingUrlPhotoColumn &&
        String(this.selectedColumns).includes('url_photo')
      ) {
        return {
          data: null,
          error: { message: 'column route_waypoints.url_photo does not exist' }
        };
      }

      let rows = this._applyFilters(tables[this.tableName] || []);
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
        data: this._project(rows[0] || null),
        error: null
      };
    }
  }

  return {
    from(tableName) {
      return new Query(tableName);
    },
    storage: {
      from(bucket) {
        return {
          async createSignedUrl(path, expiresIn) {
            return {
              data: { signedUrl: `https://signed.local/${bucket}/${path}?e=${expiresIn}` },
              error: null
            };
          }
        };
      }
    }
  };
}

function loadWaypointServiceWithSupabase(fakeSupabase) {
  const supabasePath = require.resolve('../src/lib/supabase');
  const waypointServicePath = require.resolve('../src/services/waypoint.service');

  delete require.cache[supabasePath];
  delete require.cache[waypointServicePath];

  require.cache[supabasePath] = {
    id: supabasePath,
    filename: supabasePath,
    loaded: true,
    exports: { supabaseAdmin: fakeSupabase }
  };

  return require('../src/services/waypoint.service');
}

test('waypoint photo service: fallback funciona quando route_waypoints.url_photo não existe', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      route_waypoints: [{ id: 5634, route_id: 846, status: 'CONCLUIDO' }],
      waypoint_delivery_photo: [
        {
          id: 6,
          waypoint_id: 5634,
          route_id: 846,
          user_id: 247,
          bucket: 'delivery-photos',
          object_path: 'photo_1773346638024.jpg',
          file_name: 'photo_1773346638024.jpg'
        }
      ]
    },
    { missingUrlPhotoColumn: true }
  );

  const waypointService = loadWaypointServiceWithSupabase(fakeSupabase);
  const result = await waypointService.getWaypointPhoto(5634);

  assert.equal(result.success, true);
  assert.match(
    result.signed_url,
    /https:\/\/signed\.local\/delivery-photos\/photo_1773346638024\.jpg/
  );
});

test('waypoint photo service: retorna 404 quando waypoint não possui foto', async () => {
  const fakeSupabase = createFakeSupabase(
    {
      route_waypoints: [{ id: 7001, route_id: 99, status: 'CONCLUIDO' }],
      waypoint_delivery_photo: []
    },
    { missingUrlPhotoColumn: true }
  );

  const waypointService = loadWaypointServiceWithSupabase(fakeSupabase);

  await assert.rejects(
    waypointService.getWaypointPhoto(7001),
    (error) => {
      assert.equal(error.status, 404);
      assert.match(error.message, /não possui foto|nao possui foto/i);
      return true;
    }
  );
});
