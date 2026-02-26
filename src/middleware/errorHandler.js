'use strict';
function errorHandler(err, req, res, next) {
  void next;
  console.error(err);
  const status = Number(err.status) || 500;

  const payload = {
    ok: false,
    error: err.message || 'Internal server error'
  };

  if (err.details) {
    payload.details = err.details;
  }

  res.status(status).json(payload);
}
module.exports = { errorHandler };
