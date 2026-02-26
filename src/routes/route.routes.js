'use strict';

const router = require('express').Router();
const routeService = require('../services/route.service');
const { requireAuth } = require('../middleware/auth.middleware');
const { upload } = require('../middleware/upload.middleware');

router.get('/route', requireAuth, async (req, res, next) => {
  try {
    const routeId = req.query.route_id || req.query['route-id'];
    const result = await routeService.getRoute(req.auth.userId, routeId);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.post('/route/import', requireAuth, upload.single('file'), async (req, res, next) => {
  try {
    const epsParam = typeof req.query.eps === 'string' ? req.query.eps : req.body?.eps;
    const minPtsParam = typeof req.query.minPts === 'string' ? req.query.minPts : req.body?.minPts;

    const result = await routeService.importRoute(req.auth.userId, req.file, {
      eps: epsParam,
      minPts: minPtsParam
    });
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.patch('/route/start', requireAuth, async (req, res, next) => {
  try {
    const result = await routeService.startRoute(req.auth.userId, req.body);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.patch('/route/finish', requireAuth, async (req, res, next) => {
  try {
    const routeId = req.query.route_id || req.body?.route_id;
    const result = await routeService.finishRoute(req.auth.userId, routeId);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
