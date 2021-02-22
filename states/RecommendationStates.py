from aiogram.dispatcher.filters.state import StatesGroup, State


class RecommendationStates(StatesGroup):
    callback_title = State()
