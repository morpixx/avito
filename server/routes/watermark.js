import { Router } from 'express';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import { users, watermarks } from '../db/schema.js';
import { eq } from 'drizzle-orm';

const router = Router();

function getDb() {
  const dbFile = path.resolve('./data/db.sqlite');
  const raw = new Database(dbFile);
  return drizzle(raw);
}

router.get('/watermark/:userId', async (req, res) => {
  const db = getDb();
  const userId = req.params.userId;
  try {
    const wm = await db.select().from(watermarks).where(eq(watermarks.userId, userId));
    if (!wm || wm.length === 0) return res.json(null);
    res.json(wm[0]);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'Failed to get watermark' });
  }
});

router.post('/watermark', async (req, res) => {
  const db = getDb();
  const { userId, username, filePath, sha256, placement, opacity, margin } = req.body || {};
  if (!userId || !filePath || !sha256 || !placement || typeof opacity !== 'number' || typeof margin !== 'number') {
    return res.status(400).json({ error: 'Invalid payload' });
  }
  try {
    // ensure user exists
    const u = await db.select().from(users).where(eq(users.id, userId));
    if (!u || u.length === 0) {
      await db.insert(users).values({ id: userId, username: username || null, createdAt: Date.now() });
    }

    // ensure storage dir exists
    await fs.promises.mkdir(path.dirname(filePath), { recursive: true });

    const rec = {
      userId, filePath, sha256, placement, opacity, margin, updatedAt: Date.now()
    };

    // upsert by PK
    await db.insert(watermarks).values(rec).onConflictDoUpdate({
      target: watermarks.userId,
      set: rec
    });

    res.json({ ok: true });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'Failed to save watermark' });
  }
});

export default router;
