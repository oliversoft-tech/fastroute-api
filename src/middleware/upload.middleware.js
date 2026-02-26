'use strict';

const multer = require('multer');

const maxUploadSizeBytes = Number(process.env.MAX_UPLOAD_SIZE_BYTES || 10 * 1024 * 1024);

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: maxUploadSizeBytes
  }
});

module.exports = { upload };
