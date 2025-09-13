import os
from dotenv import load_dotenv
import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from aiogram import Bot, Dispatcher, Router, F
from aiogram.exceptions import TelegramNetworkError
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from PIL import Image
import requests

from utils.fileio import ensure_dir, normalize_exif, sha256_file, delete_tree, save_preview
from utils.phash import phash, hamming
from image_pipeline import seeded_rng, soft_augment, apply_watermark
from packer import pack_job

# загрузка .env из корня проекта
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

BOT_TOKEN = os.getenv('BOT_TOKEN')
SERVER_URL = os.getenv('SERVER_URL', 'http://localhost:3000')
MAX_L = int(os.getenv('MAX_PHOTOS', '50'))
MAX_N = int(os.getenv('MAX_N', '100'))
MAX_M = int(os.getenv('MAX_M', '20'))

# Таймауты HTTP для бота (без прокси)
HTTP_TIMEOUT = int(os.getenv('BOT_HTTP_TIMEOUT', '60'))  # секунды
session = AiohttpSession(timeout=HTTP_TIMEOUT)
bot = Bot(token=BOT_TOKEN, session=session)
router = Router()
dp = Dispatcher()
dp.include_router(router)


@dataclass
class JobData:
    user_id: int
    job_id: str
    base_description: str = ''
    photos: List[Dict] = field(default_factory=list)  # {path, sha256, phash}
    unique_photos: List[Dict] = field(default_factory=list)
    N: int = 10
    M: int = 5
    archive_name: str = ''
    watermark: Optional[Dict] = None  # {path, sha256, placement, opacity, margin}
    status: str = 'Idle'
    progress: int = 0
    structured_facts: Optional[Dict] = None

    def root(self):
        return f'./workspace/{self.user_id}/{self.job_id}'

    def save(self):
        ensure_dir(self.root())
        with open(f'{self.root()}/job.json', 'w', encoding='utf-8') as f:
            json.dump(self.__dict__, f, ensure_ascii=False, indent=2)


# ===== UI helpers: единый «панельный» месседж =====
async def _delete_prev_panel(state: FSMContext, chat_id: int):
    data = await state.get_data()
    panel_id = data.get('panel_msg_id')
    if panel_id:
        try:
            await bot.delete_message(chat_id, panel_id)
        except Exception:
            pass


async def send_panel_msg(ctx_msg: Message, state: FSMContext, *, text: Optional[str] = None, photo_path: Optional[str] = None, caption: Optional[str] = None, reply_markup: Optional[InlineKeyboardMarkup] = None) -> Message:
    chat_id = ctx_msg.chat.id
    await _delete_prev_panel(state, chat_id)
    if photo_path:
        sent = await ctx_msg.answer_photo(FSInputFile(photo_path), caption=caption, reply_markup=reply_markup)
    else:
        sent = await ctx_msg.answer(text or caption or '…', reply_markup=reply_markup)
    await state.update_data(panel_msg_id=sent.message_id)
    return sent


async def edit_panel_text(ctx_msg: Message, state: FSMContext, *, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    data = await state.get_data()
    chat_id = ctx_msg.chat.id
    panel_id = data.get('panel_msg_id')
    if not panel_id:
        # если нет панели — создадим новую
        await send_panel_msg(ctx_msg, state, text=text, reply_markup=reply_markup)
        return
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=panel_id, text=text, reply_markup=reply_markup)
    except Exception:
        # если редактирование не удалось (например, панель была медиа) — пересоздадим
        await send_panel_msg(ctx_msg, state, text=text, reply_markup=reply_markup)


class States(StatesGroup):
    Idle = State()
    AwaitDescription = State()
    Facts = State()
    CollectPhotos = State()
    TuneParams = State()
    Watermark = State()
    Confirm = State()
    Running = State()


# ===== HTTP helpers (не блокируют event loop) =====
async def _http_get(url: str, *, timeout: int = 120):
    return await asyncio.to_thread(lambda: requests.get(url, timeout=timeout))


async def _http_post(url: str, *, json_body: dict, timeout: int = 120):
    return await asyncio.to_thread(lambda: requests.post(url, json=json_body, timeout=timeout))


# ===== Telegram helpers =====
async def safe_cb_answer(cb: CallbackQuery, text: str = '', *, show_alert: bool = False):
    try:
        await cb.answer(text, show_alert=show_alert)
    except Exception:
        # Игнорируем ошибки "query is too old" и др.
        pass


def kb_simple(btns: List[List[tuple]]):
    b = InlineKeyboardBuilder()
    for row in btns:
        b.row(*[InlineKeyboardButton(text=txt, callback_data=data) for txt, data in row])
    return b.as_markup()


@router.message(Command('start', 'help'))
async def start(message: Message, state: FSMContext):
    await message.answer(
        'Привет! Я помогу собрать пакет объявлений. Нажмите «Создать новый пакет объявлений».',
        reply_markup=kb_simple([[('Создать новый пакет объявлений', 'start')]])
    )


@router.message(Command('new'))
async def cmd_new(message: Message, state: FSMContext):
    await state.clear()
    await start(message, state)


@router.message(Command('cancel'))
async def cmd_cancel(message: Message, state: FSMContext):
    data = await state.get_data()
    job_data = data.get('job')
    if job_data:
        try:
            delete_tree(JobData(**job_data).root())
        except Exception:
            pass
    await state.clear()
    await message.answer('Текущий мастер/задача отменены. /start')


@router.message(Command('status'))
async def cmd_status(message: Message, state: FSMContext):
    data = await state.get_data()
    job_data = data.get('job')
    if not job_data:
        await message.answer('Активной задачи нет.')
    else:
        job = JobData(**job_data)
        await message.answer(f"Статус: {job.status}, прогресс: {job.progress}%")


@router.message(Command('settings'))
async def cmd_settings(message: Message):
    await message.answer('Настройки по умолчанию: MAX_N, MAX_M, MAX_PHOTOS (env). Переключатель выдачи zip ссылкой/файлом — в будущем.')


@router.callback_query(F.data == 'start')
async def start_new(cb: CallbackQuery, state: FSMContext):
    await state.set_state(States.AwaitDescription)
    await send_panel_msg(
        cb.message,
        state,
    text='Шаг 1/6 — Описание квартиры. Пришлите текст с описанием. Когда готовы — нажмите «Дальше».',
        reply_markup=kb_simple([[('➡ Дальше', 'next')], [('✖ Отмена', 'cancel')]])
    )


@router.callback_query(States.AwaitDescription, F.data == 'next')
async def next_to_photos(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    base_description = data.get('base_description', '')
    if len(base_description) < 40:
        await cb.answer('Описание слишком короткое (минимум 40 символов).', show_alert=True)
        return
    # init job
    job_id = str(int(time.time()))
    jd = JobData(user_id=cb.from_user.id, job_id=job_id, base_description=base_description)
    jd.archive_name = time.strftime('ads_%Y%m%d_%H%M')
    jd.save()
    await state.update_data(job=jd.__dict__)

    # переход на шаг ввода фактов
    await state.set_state(States.Facts)
    tmpl = (
        'Шаг 2/6 — Форма фактов (опционально).\n'
        'Скопируйте и заполните шаблон, отправьте одним сообщением, или нажмите «Пропустить».\n\n'
        'Город: Москва\n'
        'Адрес: ул. Пример, 12\n'
        'Метро/Район: Арбат\n'
        'Комнаты: 2\n'
        'Площадь: 45.5\n'
        'Этаж: 5/9\n'
        'Цена: 7500000\n'
        'Валюта: RUB\n'
        'Комиссия: 0\n'
    )
    await cb.message.edit_text(
        tmpl,
        reply_markup=kb_simple([[('Пропустить', 'facts:skip')], [('✖ Отмена', 'cancel')]])
    )


@router.message(States.AwaitDescription)
async def capture_description(message: Message, state: FSMContext):
    await state.update_data(base_description=message.text or '')
    await message.answer('Принято. Нажмите «Дальше».')


# ===== Шаг «Факты» =====
def _parse_structured_facts(text: str) -> Dict:
    lines = [l.strip() for l in (text or '').splitlines() if l.strip()]
    out: Dict[str, object] = {}
    for ln in lines:
        if ':' not in ln:
            continue
        key, val = ln.split(':', 1)
        key = key.strip().lower()
        val = val.strip()
        if key in ('город', 'city'):
            out['city'] = val
        elif key in ('адрес', 'address'):
            out['address'] = val
        elif key in ('метро/район', 'метро', 'район', 'district', 'metro'):
            out['district'] = val
        elif key in ('комнаты', 'rooms'):
            try:
                out['rooms'] = int(val)
            except Exception:
                out['rooms'] = val
        elif key in ('площадь', 'area'):
            val2 = val.replace(',', '.').replace('м2', '').replace('м^2', '').replace('м²', '').strip()
            try:
                out['area'] = float(val2)
            except Exception:
                out['area'] = val
        elif key in ('этаж', 'floor'):
            out['floor'] = val
        elif key in ('цена', 'price'):
            digits = ''.join(ch for ch in val if ch.isdigit())
            try:
                out['price'] = int(digits) if digits else val
            except Exception:
                out['price'] = val
        elif key in ('валюта', 'currency'):
            out['currency'] = val.upper()
        elif key in ('комиссия', 'commission'):
            out['commission'] = val
    return out


@router.message(States.Facts)
async def facts_input(message: Message, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    facts = _parse_structured_facts(message.text or '')
    job.structured_facts = facts if facts else None
    job.save()
    await state.update_data(job=job.__dict__, structured_facts=facts)
    await state.set_state(States.CollectPhotos)
    await message.answer(
        'Шаг 3/6 — Приём фото. Пришлите 1…50 фото. Поддерживаются альбомы. Когда закончите — «Готово».',
        reply_markup=kb_simple([
            [('✅ Готово', 'done_photos')],
            [('➕ Ещё фото', 'noop')],
            [('🗑 Очистить все', 'clear_photos')],
            [('✖ Отмена', 'cancel')]
        ])
    )


@router.callback_query(States.Facts, F.data == 'facts:skip')
async def facts_skip(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    job.structured_facts = None
    job.save()
    await state.update_data(job=job.__dict__)
    await state.set_state(States.CollectPhotos)
    await cb.message.edit_text(
        'Шаг 3/6 — Приём фото. Пришлите 1…50 фото. Поддерживаются альбомы. Когда закончите — «Готово».',
        reply_markup=kb_simple([
            [('✅ Готово', 'done_photos')],
            [('➕ Ещё фото', 'noop')],
            [('🗑 Очистить все', 'clear_photos')],
            [('✖ Отмена', 'cancel')]
        ])
    )


async def _tg_file_download(message: Message, dest: str) -> Optional[str]:
    if message.photo:
        file = await bot.get_file(message.photo[-1].file_id)
    elif message.document and message.document.mime_type.startswith('image/'):
        file = await bot.get_file(message.document.file_id)
    else:
        return None
    url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
    ensure_dir(os.path.dirname(dest))
    r = await _http_get(url, timeout=120)
    r.raise_for_status()
    content = r.content
    await asyncio.to_thread(lambda: open(dest, 'wb').write(content))
    return dest


@router.message(States.CollectPhotos)
async def on_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    if message.photo is None and (message.document is None or not str(message.document.mime_type).startswith('image/')):
        await message.reply('Принимаю только фото. Видео/гиф отклоняются.')
        return
    if len(job.photos) >= MAX_L:
        await message.reply('Достигнут лимит фотографий. Нажмите «Готово».')
        return

    tmp_path = f"{job.root()}/source/_tmp_{int(time.time()*1000)}.bin"
    saved = await _tg_file_download(message, tmp_path)
    if not saved:
        await message.reply('Не удалось скачать файл. Повторите.')
        return
    norm_path = f"{job.root()}/source/{int(time.time()*1000)}.jpg"
    normalize_exif(saved, norm_path)
    sha = sha256_file(norm_path)
    with Image.open(norm_path) as im:
        p = phash(im)
    job.photos.append({ 'path': norm_path, 'sha256': sha, 'phash': int(p) })

    # dedup
    uniq = []
    hidden = 0
    for item in job.photos:
        if not any(hamming(item['phash'], u['phash']) <= 10 for u in uniq):
            uniq.append(item)
        else:
            hidden += 1
    job.unique_photos = uniq

    job.save()
    await state.update_data(job=job.__dict__)
    await message.reply(f"Получено: {len(job.photos)} (уникальных: {len(job.unique_photos)}). Скрыто дублей: {hidden}.")


@router.callback_query(States.CollectPhotos, F.data == 'clear_photos')
async def clear_photos(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    delete_tree(f"{job.root()}/source")
    job.photos = []
    job.unique_photos = []
    job.save()
    await state.update_data(job=job.__dict__)
    await cb.answer('Очищено.')


@router.callback_query(States.CollectPhotos, F.data == 'done_photos')
async def done_photos(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    K = len(job.unique_photos)
    if K < 1:
        await cb.answer('Нужно минимум 1 уникальное фото.', show_alert=True)
        return
    await state.set_state(States.TuneParams)
    await cb.message.edit_text(
    f'Шаг 4/6 — Параметры. Описание {len(job.base_description)} симв., уникальных фото: {K}.',
        reply_markup=kb_simple([
            [('N: 10', 'n:10'), ('20', 'n:20'), ('40', 'n:40'), ('60', 'n:60'), ('80', 'n:80'), ('100', 'n:100')],
            [('M: 5', 'm:5'), ('10', 'm:10'), ('12', 'm:12'), (f'Все ({K})', f'm:{K}'), ('Другое', 'noop')],
            [('➡ Водяная марка', 'wm'), ('◀ Назад', 'back'), ('✖ Отмена', 'cancel')]
        ])
    )


@router.callback_query(States.TuneParams, F.data.startswith('n:'))
async def choose_n(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    val = int(cb.data.split(':')[1])
    if 1 <= val <= MAX_N:
        job.N = val
        job.save()
        await state.update_data(job=job.__dict__)
        await cb.answer(f'N={val}')
    else:
        await cb.answer('Недопустимое N')


@router.callback_query(States.TuneParams, F.data.startswith('m:'))
async def choose_m(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    val = int(cb.data.split(':')[1])
    K = len(job.unique_photos)
    max_m = min(K, MAX_M)
    if 1 <= val <= max_m:
        job.M = val
        job.save()
        await state.update_data(job=job.__dict__)
        await cb.answer(f'M={val}')
    else:
        await cb.answer(f'Недопустимое M (1..{max_m})')


@router.callback_query(States.TuneParams, F.data == 'wm')
async def to_watermark(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    # fetch existing wm
    try:
        r = await _http_get(f"{SERVER_URL}/watermark/{job.user_id}", timeout=10)
        prev = r.json()
    except Exception:
        prev = None
    await state.set_state(States.Watermark)
    if prev:
        await send_panel_msg(cb.message, state, text=(
            f"Шаг 5/6 — Водяная марка. Найдена ваша марка {prev.get('filePath')} (позиция {prev.get('placement')}, opacity {prev.get('opacity')}%). Использовать?"
        ), reply_markup=kb_simple([[('Использовать прошлую', 'wm:use_prev'), ('Загрузить новую', 'wm:upload_new'), ('Пропустить', 'wm:off')], [('◀ Назад', 'back'), ('✖ Отмена', 'cancel')]]))
    else:
        await send_panel_msg(cb.message, state, text='Шаг 5/6 — Водяная марка. Загрузите логотип или «Пропустить».',
                             reply_markup=kb_simple([[('Загрузить новую', 'wm:upload_new'), ('Пропустить', 'wm:off')], [('◀ Назад', 'back'), ('✖ Отмена', 'cancel')]]))


@router.callback_query(States.Watermark, F.data == 'wm:use_prev')
async def wm_use_prev(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    r = await _http_get(f"{SERVER_URL}/watermark/{job.user_id}", timeout=10)
    prev = r.json()
    job.watermark = prev
    job.save()
    await state.update_data(job=job.__dict__)
    await cb.answer('Используем сохранённую марку')
    # Показать предпросмотр и управление для подтверждения/тонкой настройки
    try:
        preview = await render_wm_preview(job)
        await send_panel_msg(cb.message, state, photo_path=preview,
                             caption=f"Текущие параметры: pos={job.watermark['placement']}, opacity={job.watermark['opacity']}%, margin={job.watermark['margin']}.",
                             reply_markup=wm_controls_kb(job))
    except Exception:
        await send_panel_msg(cb.message, state, text='Сохранённая марка установлена. Подтвердите или отредактируйте параметры ниже.', reply_markup=wm_controls_kb(job))


@router.callback_query(States.Watermark, F.data == 'wm:off')
async def wm_off(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    job.watermark = None
    job.save()
    await state.update_data(job=job.__dict__)
    await cb.answer('Марка отключена')
    await state.set_state(States.Confirm)
    await send_panel_msg(cb.message, state,
                         text=f"Шаг 6/6 — Подтверждение. N={job.N}, M={job.M}, марка выкл, архив {job.archive_name}.",
                         reply_markup=kb_simple([[('🚀 Запустить', 'confirm')], [('◀ Назад', 'back'), ('✖ Отмена', 'cancel')]]))

@router.callback_query(States.Watermark, F.data == 'wm:upload_new')
async def wm_upload_new_prompt(cb: CallbackQuery, state: FSMContext):
    await cb.answer('Ожидаю логотип')
    await send_panel_msg(cb.message, state,
                         text='Пришлите логотип как файл PNG/JPG или фото. Затем настройте параметры и нажмите «✔ Ок».',
                         reply_markup=kb_simple([[('Пропустить', 'wm:off')], [('◀ Назад', 'back'), ('✖ Отмена', 'cancel')]]))


@router.callback_query(States.Watermark, F.data == 'back')
async def back_from_wm(cb: CallbackQuery, state: FSMContext):
    await state.set_state(States.TuneParams)
    await cb.message.answer('Возврат к параметрам. Выберите N и M.', reply_markup=kb_simple([[('➡ Водяная марка', 'wm'), ('✖ Отмена', 'cancel')]]))


def wm_controls_kb(job: JobData):
    cur_op = job.watermark.get('opacity', 70) if job.watermark else 70
    cur_mg = job.watermark.get('margin', 24) if job.watermark else 24
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text='↖TL', callback_data='wm_pos:tl'),
        InlineKeyboardButton(text='↗TR', callback_data='wm_pos:tr'),
        InlineKeyboardButton(text='↙BL', callback_data='wm_pos:bl'),
        InlineKeyboardButton(text='↘BR', callback_data='wm_pos:br'),
        InlineKeyboardButton(text='◎Center', callback_data='wm_pos:center')
    )
    # opacity adjust in steps of 10
    minus_op = max(10, cur_op - 10)
    plus_op = min(100, cur_op + 10)
    b.row(
        InlineKeyboardButton(text=f'Opacity -10 → {minus_op}%', callback_data=f'wm_opacity:{minus_op}'),
        InlineKeyboardButton(text=f'Opacity +10 → {plus_op}%', callback_data=f'wm_opacity:{plus_op}')
    )
    # margin adjust in steps of 4
    minus_m = max(0, cur_mg - 4)
    plus_m = min(64, cur_mg + 4)
    b.row(
        InlineKeyboardButton(text=f'Margin -4 → {minus_m}', callback_data=f'wm_margin:{minus_m}'),
        InlineKeyboardButton(text=f'Margin +4 → {plus_m}', callback_data=f'wm_margin:{plus_m}')
    )
    b.row(
        InlineKeyboardButton(text='🔍 Предпросмотр', callback_data='wm:preview'),
        InlineKeyboardButton(text='✔ Ок', callback_data='wm:ok'),
        InlineKeyboardButton(text='◀ Назад', callback_data='back'),
    )
    return b.as_markup()


async def render_wm_preview(job: JobData) -> str:
    # choose first unique photo for preview; fallback to white canvas
    preview_path = f"{job.root()}/preview/wm_preview.jpg"
    os.makedirs(os.path.dirname(preview_path), exist_ok=True)
    src_path = job.unique_photos[0]['path'] if job.unique_photos else None
    if src_path:
        with Image.open(src_path) as src, Image.open(job.watermark['filePath']) as wm:
            img = apply_watermark(src, wm, job.watermark.get('placement', 'br'), job.watermark.get('opacity', 70), job.watermark.get('margin', 24))
            img.save(preview_path, format='JPEG', quality=85, subsampling=1, optimize=True)
    else:
        with Image.open(job.watermark['filePath']) as wm:
            canvas = Image.new('RGB', (800, 600), 'white')
            img = apply_watermark(canvas, wm, job.watermark.get('placement', 'br'), job.watermark.get('opacity', 70), job.watermark.get('margin', 24))
            img.save(preview_path, format='JPEG', quality=85, subsampling=1, optimize=True)
    return preview_path


@router.message(States.Watermark)
async def wm_upload(message: Message, state: FSMContext):
    if not message.document and not message.photo:
        await message.reply('Пришлите логотип как файл или фото.')
        return
    data = await state.get_data()
    job = JobData(**data.get('job'))
    tmp_path = f"{job.root()}/preview/wm_tmp.bin"
    if message.photo:
        file = await bot.get_file(message.photo[-1].file_id)
    else:
        file = await bot.get_file(message.document.file_id)
    url = f"https://api.telegram.org/file/bot{os.getenv('BOT_TOKEN')}/{file.file_path}"
    r = await _http_get(url, timeout=60)
    r.raise_for_status()
    ensure_dir(os.path.dirname(tmp_path))
    await asyncio.to_thread(lambda: open(tmp_path, 'wb').write(r.content))

    with Image.open(tmp_path) as im:
        wm_preview = apply_watermark(Image.new('RGB', (800, 600), 'white'), im, placement='br', opacity=70, margin=24)
    preview_path = f"{job.root()}/preview/wm_preview.jpg"
    save_preview(wm_preview, preview_path)

    # persist on server as user watermark
    sha = sha256_file(tmp_path)
    storage_path = f"storage/watermarks/{job.user_id}/logo.png"
    os.makedirs(os.path.dirname(storage_path), exist_ok=True)
    Image.open(tmp_path).save(storage_path)
    payload = {
        'userId': str(job.user_id),
        'username': message.from_user.username,
        'filePath': storage_path,
        'sha256': sha,
        'placement': 'br',
        'opacity': 70,
        'margin': 24
    }
    # сохраняем на сервере неблокирующим образом
    try:
        await _http_post(f"{SERVER_URL}/watermark", json_body=payload, timeout=30)
    except Exception:
        pass

    job.watermark = payload
    job.save()
    await state.update_data(job=job.__dict__)
    try:
        preview = await render_wm_preview(job)
        await send_panel_msg(message, state, photo_path=preview,
                             caption=f"Текущие параметры: pos={job.watermark['placement']}, opacity={job.watermark['opacity']}%, margin={job.watermark['margin']}.",
                             reply_markup=wm_controls_kb(job))
    except Exception:
        await send_panel_msg(message, state, text='Логотип принят. Настройте параметры:', reply_markup=wm_controls_kb(job))


@router.callback_query(States.Watermark, F.data.startswith('wm_pos:'))
async def wm_set_pos(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    pos = cb.data.split(':')[1]
    if not job.watermark:
        await cb.answer('Сначала загрузите логотип')
        return
    job.watermark['placement'] = pos
    job.save()
    await state.update_data(job=job.__dict__)
    await cb.answer(f'Позиция: {pos}')


@router.callback_query(States.Watermark, F.data.startswith('wm_opacity:'))
async def wm_set_opacity(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    val = int(cb.data.split(':')[1])
    if not job.watermark:
        await cb.answer('Сначала загрузите логотип')
        return
    job.watermark['opacity'] = max(10, min(100, val))
    job.save()
    await state.update_data(job=job.__dict__)
    await cb.answer(f'Opacity: {job.watermark["opacity"]}%')


@router.callback_query(States.Watermark, F.data.startswith('wm_margin:'))
async def wm_set_margin(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    val = int(cb.data.split(':')[1])
    if not job.watermark:
        await cb.answer('Сначала загрузите логотип')
        return
    job.watermark['margin'] = max(0, min(64, val))
    job.save()
    await state.update_data(job=job.__dict__)
    await cb.answer(f'Margin: {job.watermark["margin"]}')


@router.callback_query(States.Watermark, F.data == 'wm:preview')
async def wm_preview(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    if not job.watermark:
        await cb.answer('Нет логотипа')
        return
    preview = await render_wm_preview(job)
    await send_panel_msg(
        cb.message,
        state,
        photo_path=preview,
        caption=f"Предпросмотр. pos={job.watermark['placement']}, opacity={job.watermark['opacity']}%, margin={job.watermark['margin']}",
        reply_markup=wm_controls_kb(job)
    )


@router.callback_query(States.Watermark, F.data == 'wm:ok')
async def wm_ok(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))
    await state.set_state(States.Confirm)
    wm_state = 'выкл' if not job.watermark else f"вкл ({job.watermark.get('placement')}, {job.watermark.get('opacity')}%, m{job.watermark.get('margin')})"
    await send_panel_msg(cb.message, state,
                         text=f"Шаг 6/6 — Подтверждение. N={job.N}, M={job.M}, марка {wm_state}, архив {job.archive_name}.",
                         reply_markup=kb_simple([[('🚀 Запустить', 'confirm')], [('◀ Назад', 'back'), ('✖ Отмена', 'cancel')]]))


@router.callback_query(States.Confirm, F.data == 'back')
async def confirm_back(cb: CallbackQuery, state: FSMContext):
    await state.set_state(States.Watermark)
    await send_panel_msg(cb.message, state,
                         text='Вернулись к водяной марке. Пришлите логотип или «Пропустить».',
                         reply_markup=kb_simple([[('Пропустить', 'wm:off')], [('◀ Назад', 'back'), ('✖ Отмена', 'cancel')]]))


def simple_text_difference(text1: str, text2: str) -> float:
    """Простая проверка различий между текстами по словам (возвращает долю различающихся слов)"""
    words1 = set(text1.lower().split())
    words2 = set(text2.lower().split())
    
    if not words1 and not words2:
        return 0.0
    if not words1 or not words2:
        return 1.0
        
    intersection = words1.intersection(words2)
    union = words1.union(words2)
    
    return 1.0 - (len(intersection) / len(union)) if union else 0.0


def ensure_unique_texts(texts: List[str], base_description: str, min_difference: float = 0.3) -> List[str]:
    """Обеспечивает уникальность каждого текста (минимум min_difference различий)"""
    if not texts:
        return [base_description]
    
    unique_texts = []
    used_texts = set()
    
    for i, text in enumerate(texts):
        # Проверяем, что текст достаточно отличается от уже использованных
        is_unique = True
        text_clean = text.strip()
        
        # Проверяем против всех уже добавленных текстов
        for existing in unique_texts:
            difference = simple_text_difference(text_clean, existing)
            if difference < min_difference:
                is_unique = False
                break
        
        if is_unique and text_clean and text_clean not in used_texts:
            unique_texts.append(text_clean)
            used_texts.add(text_clean)
        else:
            # Если текст недостаточно уникален, модифицируем его
            modified_text = f"{text_clean} [Объявление №{i+1}]"
            unique_texts.append(modified_text)
            used_texts.add(modified_text)
    
    return unique_texts


async def generate_texts(base_facts: dict, base_description: str, n: int, style_hints: str = 'нейтрально, без воды') -> List[str]:
    """Генерация уникальных текстов для каждого объявления"""
    body = {
        'baseFacts': base_facts,
        'baseDescription': base_description,
        'n': n,
        'styleHints': style_hints
    }
    try:
        r = await _http_post(f"{SERVER_URL}/texts/generate", json_body=body, timeout=180)
        data = r.json()
        variants: List[str] = []
        if isinstance(data, list):
            variants = [str(x) for x in data]
        elif isinstance(data, dict):
            # сервер теперь возвращает { ok: true, variants: [...] }
            raw = data.get('variants') or data.get('value') or data.get('texts') or data.get('data')
            if isinstance(raw, list):
                variants = [str(x) for x in raw]
        
        # Обеспечиваем уникальность полученных текстов
        if variants:
            unique_variants = ensure_unique_texts(variants, base_description, min_difference=0.3)
        else:
            unique_variants = []
        
        # ensure N и fallback с уникальными модификациями
        while len(unique_variants) < n:
            fallback_text = f"{base_description} [Вариант {len(unique_variants) + 1}]"
            unique_variants.append(fallback_text)
        
        # Обрезаем до нужного количества и финальная проверка уникальности
        final_texts = unique_variants[:n]
        final_unique = ensure_unique_texts(final_texts, base_description, min_difference=0.2)
        
        return final_unique[:n]
    except Exception as e:
        # Fallback: создаем уникальные вариации базового описания
        print(f"Ошибка генерации текстов: {e}")
        fallback_texts = []
        for i in range(n):
            fallback_texts.append(f"{base_description} [Вариант {i+1}]")
        return fallback_texts


def progress_bar(p: int) -> str:
    blocks = int(p / 10)
    return '▰' * blocks + '▱' * (10 - blocks) + f' {p}%'


@router.callback_query(States.Confirm, F.data == 'confirm')
async def run_job(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job = JobData(**data.get('job'))

    K = len(job.unique_photos)
    if job.N < 1 or job.N > MAX_N or job.M < 1 or job.M > min(K, MAX_M):
        await cb.answer('Проверьте N/M.', show_alert=True)
        return

    await state.set_state(States.Running)
    job.status = 'Running'
    job.progress = 0
    job.save()
    # Сброс старой панели и старт единого прогресс-сообщения
    await _delete_prev_panel(state, cb.message.chat.id)
    await send_panel_msg(cb.message, state, text='Старт задачи… ' + progress_bar(0), reply_markup=kb_simple([[('✖ Остановить', 'stop')]]))

    stop_flag_path = f"{job.root()}/.stop"

    try:
        # 1) Генерация текстов - дожидаемся готовности ВСЕХ текстов
        job.status = 'Генерация текстов'
        job.progress = 10
        job.save()
        await edit_panel_text(cb.message, state, text='Генерация текстов… ' + progress_bar(job.progress), reply_markup=kb_simple([[('✖ Остановить', 'stop')]]))

        # Используем введённое пользователем описание как исходные факты (source)
        base_facts = {'source': job.base_description}
        if getattr(job, 'structured_facts', None):
            base_facts['structured'] = job.structured_facts
        
        # ВАЖНО: дожидаемся готовности ВСЕХ текстов перед продолжением
        print(f"Запрос генерации {job.N} уникальных текстов...")
        texts = await generate_texts(base_facts, job.base_description, job.N)
        print(f"Получено {len(texts)} текстов, проверяем уникальность...")
        
        # Дополнительная проверка и обеспечение уникальности
        final_texts = ensure_unique_texts(texts, job.base_description, min_difference=0.25)
        while len(final_texts) < job.N:
            final_texts.append(f"{job.base_description} [Дополнительный вариант {len(final_texts) + 1}]")
        
        # Обрезаем до нужного количества
        texts = final_texts[:job.N]
        
        # Сохраним на диск для диагностики
        try:
            ensure_dir(job.root())
            with open(f"{job.root()}/generated_texts.json", 'w', encoding='utf-8') as f:
                json.dump(texts, f, ensure_ascii=False, indent=2)
            print(f"Тексты сохранены: {len(texts)} уникальных вариантов")
        except Exception as e:
            print(f"Ошибка сохранения текстов: {e}")

        if os.path.exists(stop_flag_path):
            raise RuntimeError('stopped')

        # 2) Аугментация изображений
        job.status = 'Аугментация изображений'
        job.progress = 20
        job.save()
        await edit_panel_text(cb.message, state, text='Аугментация изображений… ' + progress_bar(job.progress), reply_markup=kb_simple([[('✖ Остановить', 'stop')]]))

        # Используем абсолютные пути, чтобы серверный ZIP и локальный фолбэк всегда видели корректные директории
        out_root = os.path.abspath(f"{job.root()}/out")
        ensure_dir(out_root)
        total = job.N * job.M
        done = 0
        for v in range(job.N):
            if os.path.exists(stop_flag_path):
                raise RuntimeError('stopped')
            ad_folder = os.path.join(out_root, f"объявление {v+1:02d}")
            # Страхуем создание обоих уровней, чтобы запись описания не падала
            ensure_dir(ad_folder)
            photos_dir = os.path.join(ad_folder, "фото")
            ensure_dir(photos_dir)
            # УНИКАЛЬНЫЙ текст для каждого объявления
            ad_text = texts[v] if v < len(texts) else f"{job.base_description} [Объявление №{v+1}]"
            with open(os.path.join(ad_folder, "описание.txt"), 'w', encoding='utf-8') as f:
                f.write(ad_text)
            print(f"Объявление {v+1}: сохранен уникальный текст ({len(ad_text)} символов)")
            for m in range(job.M):
                if os.path.exists(stop_flag_path):
                    raise RuntimeError('stopped')
                src = job.unique_photos[(v * job.M + m) % len(job.unique_photos)]
                with Image.open(src['path']) as im:
                    rng = seeded_rng(job.job_id, v, m)
                    aug = soft_augment(im, rng)
                    if job.watermark:
                        with Image.open(job.watermark['filePath']) as wm:
                            aug = apply_watermark(aug, wm, job.watermark.get('placement', 'br'), job.watermark.get('opacity', 70), job.watermark.get('margin', 24))
                    aug.save(os.path.join(photos_dir, f"photo_{m+1:02d}.jpg"), format='JPEG', quality=92, subsampling=1, optimize=True)
                done += 1
                job.progress = 20 + int(70 * done / total)
                if done % max(1, total // 20) == 0:
                    job.save()
                    await edit_panel_text(cb.message, state, text=f"Аугментация изображений: {done}/{total} (вариант {v+1} из {job.N})… " + progress_bar(job.progress), reply_markup=kb_simple([[('✖ Остановить', 'stop')]]))
                # отдаём управление event loop, чтобы обрабатывались другие апдейты (например, Стоп)
                if done % 5 == 0:
                    await asyncio.sleep(0)

        # 3) Сборка архива
        if os.path.exists(stop_flag_path):
            raise RuntimeError('stopped')
        job.status = 'Сборка архива'
        job.progress = 95
        job.save()
        await edit_panel_text(cb.message, state, text='Сборка архива… ' + progress_bar(job.progress), reply_markup=kb_simple([[('✖ Остановить', 'stop')]]))
        # Строим manifest.json и README.txt в корне out_root
        import datetime
        manifest = {
            "jobId": job.job_id,
            "title": job.archive_name,
            "createdAt": datetime.datetime.now().astimezone().isoformat(),
            "variants": job.N,
            "photosPerVariant": job.M,
            # Кладём в манифест факты, введённые пользователем (как есть)
            "facts": {"source": job.base_description, **({"structured": job.structured_facts} if job.structured_facts else {})},
            "watermark": {
                "enabled": bool(job.watermark),
                "file": (os.path.basename(job.watermark['filePath']) if job.watermark else None),
                "placement": (job.watermark.get('placement') if job.watermark else None),
                "opacity": (job.watermark.get('opacity') if job.watermark else None),
                "margin": (job.watermark.get('margin') if job.watermark else None)
            }
        }
        with open(os.path.join(out_root, 'manifest.json'), 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        readme_path = os.path.join(out_root, 'README.txt')
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write('Пакет объявлений. Структура: объявление NN/фото/photo_XX.jpg и описание.txt\n')

        archive_path = os.path.abspath(f"{job.root()}/archive.zip")
        try:
            payload = {
                'inputFolders': [out_root],
                'outputZipPath': archive_path,
                'rootFolderName': job.archive_name,
                'flatten': True,
                'files': []
            }
            rr = await _http_post(f"{SERVER_URL}/zip/create", json_body=payload, timeout=600)
            if rr.status_code != 200:
                raise RuntimeError('zip via server failed')
        except Exception:
            # fallback to local zip (в отдельном потоке, чтобы не блокировать event loop)
            await asyncio.to_thread(lambda: pack_job(out_root, archive_path, root_name=job.archive_name))

        # завершение
        job.status = 'Готово'
        job.progress = 100
        job.save()
        await edit_panel_text(cb.message, state, text='Готово! ' + progress_bar(100), reply_markup=None)
        # Заменим панель финальным сообщением с архивом
        await _delete_prev_panel(state, cb.message.chat.id)
        doc_msg = await cb.message.answer_document(
            FSInputFile(archive_path),
            caption=f"Готово! Сгенерировано: {job.N} × {job.M} = {job.N*job.M} изображений. Архив: {os.path.basename(archive_path)}",
            reply_markup=kb_simple([[('🔁 Ещё один пакет', 'start')], [('🗑 Удалить временные файлы', 'cleanup')]])
        )
        await state.update_data(panel_msg_id=doc_msg.message_id)
        await state.set_state(States.Idle)

    except RuntimeError as e:
        if str(e) == 'stopped':
            job.status = 'Отменено'
            job.save()
            try:
                os.remove(stop_flag_path)
            except Exception:
                pass
            await state.set_state(States.Confirm)
            await send_panel_msg(cb.message, state, text='Задача остановлена пользователем.', reply_markup=kb_simple([[('🔁 Запустить заново', 'confirm')]]))
            return
        await state.set_state(States.Confirm)
        await send_panel_msg(cb.message, state, text='Ошибка при выполнении задачи. Попробуйте ещё раз.', reply_markup=kb_simple([[('🔁 Повторить', 'confirm')]]))
    except Exception:
        await state.set_state(States.Confirm)
        await send_panel_msg(cb.message, state, text='Ошибка при выполнении задачи. Попробуйте ещё раз.', reply_markup=kb_simple([[('🔁 Повторить', 'confirm')]]))


@router.callback_query(States.Running, F.data == 'stop')
async def stop_job(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    job_data = data.get('job')
    if job_data:
        job = JobData(**job_data)
        with open(f"{job.root()}/.stop", 'w') as f:
            f.write('1')
        await safe_cb_answer(cb, 'Остановка запрошена')
        # Пытаемся обновить панель, чтобы пользователь увидел уведомление
        try:
            await edit_panel_text(cb.message, state, text='Остановка запрошена. Завершение текущих шагов… ' + progress_bar(job.progress), reply_markup=None)
        except Exception:
            pass
    else:
        await safe_cb_answer(cb, 'Нет активной задачи')


async def main():
    assert BOT_TOKEN, 'BOT_TOKEN is required in env'
    # Быстрая проверка сети/токена, чтобы дать понятный месседж до старта long-polling
    try:
        await bot.get_me()
    except TelegramNetworkError as e:
        print('Не удаётся подключиться к api.telegram.org. Проверьте интернет/прокси/VPN. Можно задать BOT_PROXY_URL или HTTPS_PROXY. Ошибка:', e)
    except Exception:
        # Игнорируем прочие ошибки здесь — polling ниже всё равно попытается переподключиться
        pass
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
