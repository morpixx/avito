// Грузим .env как из текущего каталога, так и из корня проекта
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import express from 'express';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import Database from 'better-sqlite3';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { users, watermarks } from './db/schema.js';
import watermarkRouter from './routes/watermark.js';
import textsRouter from './routes/texts.js';
import zipRouter from './routes/zip.js';

// Инициализация dotenv
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// Сперва пробуем корень проекта (../.env), затем fallback на текущий CWD
dotenv.config({ path: path.resolve(__dirname, '../.env') });
dotenv.config();

const PORT = process.env.PORT || 3000;

async function ensureDataDir() {
  await fs.promises.mkdir('./data', { recursive: true });
}

function openDb() {
  const dbFile = path.resolve('./data/db.sqlite');
  const raw = new Database(dbFile);
  // PRAGMA
  raw.pragma('journal_mode = WAL');
  raw.pragma('synchronous = NORMAL');
  raw.pragma('temp_store = MEMORY');
  raw.pragma('foreign_keys = ON');
  raw.pragma('cache_size = -2000');
  return raw;
}

async function runMigrations(raw) {
  // drizzle-kit migrate uses CLI, но на запуске убедимся, что таблицы существуют.
  const db = drizzle(raw);
  // create tables if not exist (idempotent simple bootstrap)
  raw.exec(`CREATE TABLE IF NOT EXISTS users (
    id text PRIMARY KEY,
    username text,
    createdAt integer NOT NULL
  );`);
  raw.exec(`CREATE TABLE IF NOT EXISTS watermarks (
    userId text PRIMARY KEY REFERENCES users(id),
    filePath text NOT NULL,
    sha256 text NOT NULL,
    placement text NOT NULL,
    opacity integer NOT NULL,
    margin integer NOT NULL,
    updatedAt integer NOT NULL
  );`);
}

async function main() {
  await ensureDataDir();
  const raw = openDb();
  await runMigrations(raw);

  const app = express();
  app.use(cors());
  app.use(express.json({ limit: '10mb' }));
  app.use('/storage', express.static(path.resolve('./storage')));

  app.use(watermarkRouter);
  app.use(textsRouter);
  app.use(zipRouter);

  app.get('/health', (req, res) => res.json({ ok: true }));

  app.listen(PORT, () => console.log(`Server listening on http://localhost:${PORT}`));
}

main().catch((e) => {
  console.error('Fatal', e);
  process.exit(1);
});
