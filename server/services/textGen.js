import Groq from 'groq-sdk';

// --- Простая дедупликация ---
function normalize(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
function isUnique(arr, s, minDiff = 3) {
  const normS = normalize(s);
  for (const t of arr) {
    const normT = normalize(t);
    if (normT === normS) return false;
    // Словарная разница
    const a = new Set(normT.split(' '));
    const b = new Set(normS.split(' '));
    let diff = 0;
    for (const w of a) if (!b.has(w)) diff++;
    for (const w of b) if (!a.has(w)) diff++;
    if (diff < minDiff) return false;
  }
  return true;
}

// --- Основная функция генерации ---
export async function generateTexts({ baseFacts, baseDescription, n, styleHints }) {
  const apiKey = process.env.GROQ_API_KEY;
  const model = process.env.GROQ_MODEL || 'llama-3.3-70b-versatile';
  const groq = new Groq({ apiKey });

  console.log('GROQ_API_KEY:', !!apiKey, 'GROQ_MODEL:', model);

  // Формируем промпт для модели
  const norm = (s) => String(s || '').replace(/\s+/g, ' ').trim();

  const facts = baseFacts?.structured
    ? Object.entries(baseFacts.structured)
        .map(([k, v]) => {
          if (Array.isArray(v)) return `${k}: ${v.map(norm).join(', ')}`;
          if (v && typeof v === 'object') return `${k}: ${norm(JSON.stringify(v))}`;
          return `${k}: ${norm(v)}`;
        })
        .join('; ')
    : norm(baseFacts?.source);

  const prompt = [
    'ВАЖНО: Ответ — только валидный JSON-массив строк, без объектов, без ключей, без пояснений, без фигурных скобок {}. Пример: ["...", "..."]',
    'Не используй фигурные скобки {} вообще. Только квадратные [].',
    'Ответ начинается с [ и заканчивается на ].',
    `Сгенерируй ровно ${n} уникальных вариантов описания объекта недвижимости на русском языке.`,
    facts ? `Факты (единственный источник правды, ничего не выдумывай): ${facts}` : '',
    baseDescription ? `Референс дополнительных фактов, если базовых будет мало: ${norm(baseDescription).slice(0, 600)}` : '',
    'Требования к каждому варианту: 580-640 символов (с пробелами); один абзац без переносов строк (\\n и \\r запрещены); без эмодзи и CAPS; орфография — норма.',
    'Запрещено: добавлять несуществующие детали; менять числа, адреса, площади, цены, сроки; писать оценочные расстояния/виды/сроки, если их нет в фактах; нумеровать варианты.',
    'Разнообразие: меняй ракурс (планировка/свет/инфраструктура/сценарии/инвест-логика), синтаксис и лексику; не повторяй целые фразы между вариантами.',
    'Если какого-то факта нет — просто опусти его.',
    `Перед выводом проверь: длина каждого варианта 450–580; вариантов ровно ${n}; все соответствуют фактам; формулировки существенно различаются; внутри строк нет неэкранированных " или \\.`,
    'Верни сразу валидный JSON-массив строк без пояснений.'
  ].filter(Boolean).join('\n');


  let variants = [];
  let error = null;

  for (let attempt = 0; attempt < 2; ++attempt) {
    try {
      const resp = await groq.chat.completions.create({
        model,
        messages: [
          { role: 'user', content: prompt }
        ],
        temperature: 1.5,
        top_p: 1,
        max_completion_tokens: 10500,
        response_format: { type: "json_object" } // <-- это и есть требование JSON
      });
      let raw = resp.choices?.[0]?.message?.content || '';
      // Парсим JSON-массив из ответа
      let arr = [];
      try {
        arr = JSON.parse(raw);
        if (Array.isArray(arr)) {
          // Всё ок, это массив
        } else if (arr && typeof arr === 'object') {
          // Если это объект с ключами-строками (и значениями строками), превращаем в массив значений
          const values = Object.values(arr);
          if (values.every(v => typeof v === 'string')) {
            arr = values;
          } else if (Array.isArray(arr.variants)) {
            arr = arr.variants;
          } else {
            arr = [];
          }
        } else {
          arr = [];
        }
      } catch {
        arr = [];
      }
      // Дедупликация
      for (const t of arr) {
        if (typeof t === 'string' && isUnique(variants, t)) variants.push(t.trim());
        if (variants.length >= n) break;
      }
      if (variants.length >= n) break;
    } catch (e) {
      error = e;
      console.error('Groq error:', e); // <--- добавьте это!
      // Если ошибка — повторим ещё раз
    }
  }

  // Если не удалось — fallback
  while (variants.length < n) {
    variants.push(`${baseDescription}\n\n[Вариант ${variants.length + 1} • ${Math.random().toString(16).slice(2, 8)}]`);
  }
  return variants.slice(0, n);
}

// --- Для /llm/debug и тестов ---
export function getLLMInfo() {
  return {
    provider: 'groq',
    model: process.env.GROQ_MODEL || 'llama-3.3-70b-versatile',
    baseURL: process.env.GROQ_BASE_URL,
    hasKey: !!process.env.GROQ_API_KEY
  };
}



export function getLastLLMTrace() {
  return {};
}

