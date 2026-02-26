'use strict';

const router = require('express').Router();
const waypointService = require('../services/waypoint.service');
const { requireAuth } = require('../middleware/auth.middleware');
const { upload } = require('../middleware/upload.middleware');

router.patch('/waypoint/finish', requireAuth, upload.single('image_base64'), async (req, res, next) => {
  try {
    const payload = {
      ...req.body,
      waypoint_id: req.body?.waypoint_id || req.query?.waypoint_id,
      status: req.body?.status
    };

    const result = await waypointService.finishWaypoint(req.auth.userId, payload, req.file);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.get('/waypoint/photo', requireAuth, async (req, res, next) => {
  try {
    const waypointId = req.query.waypoint_id || req.query.id;
    const result = await waypointService.getWaypointPhoto(waypointId);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

router.patch('/waypoint/reorder', requireAuth, async (req, res, next) => {
  try {
    const result = await waypointService.reorderWaypoints(req.body);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
