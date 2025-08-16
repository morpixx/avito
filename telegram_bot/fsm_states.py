from aiogram.fsm.state import StatesGroup, State


class ListingStates(StatesGroup):
    START = State()
    INPUT_DESC = State()
    UPLOAD_PHOTOS = State()
    SET_NUM_LISTINGS = State()
    SETTINGS = State()
    CONFIRM = State()
    PROCESSING = State()
    QA_REVIEW = State()
    DONE = State()
