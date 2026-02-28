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

const PORT = process.env.PORT || 3002;

app.listen(PORT, "0.0.0.0", () => {
  console.log(`astRoute backend running on ${PORT}`);
});
