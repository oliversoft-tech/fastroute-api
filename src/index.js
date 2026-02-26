'use strict';
require('dotenv').config();

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');

const authRoutes = require('./routes/auth.routes');
const routeRoutes = require('./routes/route.routes');
const waypointRoutes = require('./routes/waypoint.routes');
const syncRoutes = require('./routes/sync.routes');
const healthRoutes = require('./routes/health.routes');
const { errorHandler } = require('./middleware/errorHandler');

const app = express();

app.use(helmet());
app.use(cors());
app.use(express.json({ limit: process.env.JSON_LIMIT || '2mb' }));
app.use(morgan('dev'));

app.use(authRoutes);
app.use(routeRoutes);
app.use(waypointRoutes);
app.use('/health', healthRoutes);
app.use('/sync', syncRoutes);

app.use(errorHandler);

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`FastRoute backend running on http://localhost:${port}`);
});
