from aiogram.dispatcher.filters.state import StatesGroup, State


class BasicStates(StatesGroup):
    recommendation = State()
    voice_practice = State()
    text_practice = State()
    random_practice = State()
