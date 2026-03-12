'use strict';
const router = require('express').Router();
const syncService = require('../services/sync.service');

router.post('/push', async (req, res, next) => {
  try {
    const result = await syncService.push(req.body, {
      authorizationHeader: req.headers?.authorization
    });
    res.json(result);
  } catch (e) {
    next(e);
  }
});

router.post('/pull', async (req, res, next) => {
  try {
    const result = await syncService.pull(req.body.sinceTs);
    res.json(result);
  } catch (e) {
    next(e);
  }
});

module.exports = router;
