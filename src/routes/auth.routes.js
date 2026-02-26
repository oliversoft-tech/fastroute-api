'use strict';

const router = require('express').Router();
const authService = require('../services/auth.service');

router.post('/login', async (req, res, next) => {
  try {
    const result = await authService.login(req.body);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
