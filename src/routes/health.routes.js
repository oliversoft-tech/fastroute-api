'use strict';
const router = require('express').Router();
const pkg = require('../../package.json');

router.get('/', (req, res) =>
  res.json({
    ok: true,
    service: 'FastRoute Backend',
    version: pkg.version,
    environment: process.env.NODE_ENV || 'development',
    build: process.env.BUILD_ID || null,
    commit: process.env.GIT_SHA || null
  })
);
module.exports = router;
