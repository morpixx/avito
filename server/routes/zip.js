import { Router } from 'express';
import path from 'path';
import fs from 'fs';
import { createZipFromFolders } from '../services/zip.js';

const router = Router();

router.post('/zip/create', async (req, res) => {
  const { inputFolders, outputZipPath, rootFolderName, flatten = false, files = [] } = req.body || {};
  if (!Array.isArray(inputFolders) || !outputZipPath) {
    return res.status(400).json({ error: 'Invalid payload' });
  }
  try {
    for (const f of inputFolders) {
      if (!fs.existsSync(f)) return res.status(400).json({ error: `Input folder not found: ${f}` });
    }
    const result = await createZipFromFolders({ inputFolders, outputZipPath, rootFolderName, flatten, files });
    res.json({ ok: true, ...result });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'Zip failed' });
  }
});

export default router;
