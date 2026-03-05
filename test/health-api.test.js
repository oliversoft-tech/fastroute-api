'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');

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

test('health endpoint expõe versão/build/commit', async () => {
  process.env.BUILD_ID = 'build-test-1';
  process.env.GIT_SHA = 'abc1234';

  const router = require('../src/routes/health.routes');
  const pkg = require('../package.json');

  const app = express();
  app.use('/health', router);

  await withServer(app, async (baseUrl) => {
    const response = await fetch(`${baseUrl}/health`);
    assert.equal(response.status, 200);

    const payload = await response.json();
    assert.equal(payload.ok, true);
    assert.equal(payload.service, 'FastRoute Backend');
    assert.equal(payload.version, pkg.version);
    assert.equal(payload.build, 'build-test-1');
    assert.equal(payload.commit, 'abc1234');
  });
});
