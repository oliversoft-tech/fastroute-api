'use strict';
const router = require('express').Router();
router.get('/', (req, res) => res.json({ ok: true, service: 'FastRoute Backend' }));
module.exports = router;