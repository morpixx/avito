import { Router } from 'express';
import { z } from 'zod';
import { generateTexts, getLLMInfo, getLastLLMTrace, testLLM } from '../services/textGen.js';

const router = Router();

const Body = z.object({
  baseFacts: z.record(z.any()),
  baseDescription: z.string().min(1),
  n: z.number().min(1).max(100),
  styleHints: z.string().optional(),
  debug: z.boolean().optional()
});

router.post('/texts/generate', async (req, res) => {
  try {
    const parse = Body.safeParse(req.body);
    if (!parse.success) {
      return res.status(200).json({ ok: true, variants: [] });
    }
    const { baseFacts, baseDescription, n, styleHints, debug } = parse.data;
    let variants = await generateTexts({ baseFacts, baseDescription, n, styleHints, debug: Boolean(debug) });

    // Лёгкая дедупликация (страховка): нормализация + точное сравнение
    const norm = (s) => String(s || '')
      .toLowerCase()
      .replace(/[^\p{L}\p{N}\s]+/gu, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    const out = [];
    for (const t of Array.isArray(variants) ? variants : []) {
      if (!t || typeof t !== 'string') continue;
      const k = norm(t);
      if (!k) continue;
      if (!out.some((x) => norm(x) === k)) out.push(t.trim());
    }
    if (out.length > 0) variants = out.slice(0, n || out.length);
    return res.status(200).json({ ok: true, variants: Array.isArray(variants) ? variants : [] });
  } catch (e) {
    console.error('texts/generate fatal:', e);
    return res.status(200).json({ ok: true, variants: [] });
  }
});

router.get('/llm/debug', (req, res) => {
  try {
    res.json(getLLMInfo());
  } catch (e) {
    res.status(500).json({ error: 'LLM info error' });
  }
});

router.get('/llm/last', (req, res) => {
  try {
    res.json(getLastLLMTrace());
  } catch (e) {
    res.status(500).json({ error: 'LLM trace error' });
  }
});

router.get('/llm/test', async (req, res) => {
  try {
    const r = await testLLM();
    res.json(r);
  } catch (e) {
    res.status(500).json({ ok: false, error: 'LLM test error' });
  }
});

export default router;
