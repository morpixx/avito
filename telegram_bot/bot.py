import os
import logging
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext

from telegram_bot.fsm_states import ListingStates
from job_manager.manager import start_job

# Load environment
load_dotenv()
API_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# /start command
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Привет! Отправьте описание квартиры для генерации объявлений.")
    await state.set_state(ListingStates.INPUT_DESC)

# Receive description
@dp.message(StateFilter(ListingStates.INPUT_DESC))
async def process_desc(message: types.Message, state: FSMContext):
    await state.update_data(description=message.text)
    await message.answer("Теперь загрузите фотографии (минимум 1, максимум 20). Отправьте все подряд.")
    await state.set_state(ListingStates.UPLOAD_PHOTOS)

# Collect photos
@dp.message(StateFilter(ListingStates.UPLOAD_PHOTOS), content_types=[types.ContentType.PHOTO])
async def process_photo(message: types.Message, state: FSMContext):
    data = await state.get_data()
    photos = data.get('photos', [])
    photos.append(message.photo[-1].file_id)
    await state.update_data(photos=photos)
    await message.answer(f"Принято фото {len(photos)}. Продолжайте или нажмите /done.")

@dp.message(Command("done"), StateFilter(ListingStates.UPLOAD_PHOTOS))
async def finish_photos(message: types.Message, state: FSMContext):
    data = await state.get_data()
    count = len(data.get('photos', []))
    if count == 0:
        return await message.answer("Нужно хотя бы одно фото.")
    # Ask number of listings
    kb = InlineKeyboardMarkup(row_width=2)
    for n in [20, 40, 60]:
        kb.insert(InlineKeyboardButton(f"{n}", callback_data=f"num_{n}"))
    kb.insert(InlineKeyboardButton("Custom", callback_data="num_custom"))
    await message.answer("Сколько уникальных объявлений создать?", reply_markup=kb)
    await state.set_state(ListingStates.SET_NUM_LISTINGS)

# Number selection
@dp.callback_query(StateFilter(ListingStates.SET_NUM_LISTINGS))
async def select_num(cb: types.CallbackQuery, state: FSMContext):
    data = cb.data
    if data.startswith("num_") and data != "num_custom":
        n = int(data.split("_")[1])
        await state.update_data(num_listings=n)
        await cb.message.edit_text(f"Создадим {n} объявлений.")
        # proceed to settings
        await send_settings(cb.message, state)
    else:
        await cb.message.answer("Введите желаемое число объявлений цифрой:")
        await state.set_state(ListingStates.SET_NUM_LISTINGS)

async def send_settings(message: types.Message, state: FSMContext):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.insert(InlineKeyboardButton("Watermark ON", callback_data="wm_on"))
    kb.insert(InlineKeyboardButton("Watermark OFF", callback_data="wm_off"))
    await message.answer("Настройки генерации:", reply_markup=kb)
    await state.set_state(ListingStates.SETTINGS)

# Settings selection
@dp.callback_query(StateFilter(ListingStates.SETTINGS))
async def process_settings(cb: types.CallbackQuery, state: FSMContext):
    opt = cb.data
    wm = True if opt == "wm_on" else False
    await state.update_data(watermark=wm)
    await cb.message.edit_text(f"Watermark={'ON' if wm else 'OFF'}")
    # confirm
    data = await state.get_data()
    summary = (
        f"Описание: {data['description'][:50]}...\n"
        f"Фото: {len(data['photos'])}\n"
        f"Объявлений: {data['num_listings']}\n"
        f"Watermark: {wm}"
    )
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton("Начать генерацию", callback_data="confirm"))
    await cb.message.answer(summary, reply_markup=kb)
    await state.set_state(ListingStates.CONFIRM)

# Confirm and process job
@dp.callback_query(StateFilter(ListingStates.CONFIRM))
async def confirm(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.edit_text("Запускаем генерацию...")
    data = await state.get_data()
    job_id = await start_job(
        description=data['description'],
        photo_ids=data['photos'],
        num_listings=data['num_listings'],
        watermark=data['watermark'],
    )
    await state.set_state(ListingStates.PROCESSING)
    # notify user when done
    # For simplicity assume start_job returns path to ZIP
    zip_path = job_id  # using job_id as path for now
    await bot.send_document(cb.from_user.id, open(zip_path, 'rb'))
    await state.clear()
    await bot.send_message(cb.from_user.id, "Готово! Вот ваши объявления.")

if __name__ == '__main__':
    import asyncio
    asyncio.run(dp.start_polling(bot))
