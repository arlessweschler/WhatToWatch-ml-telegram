from aiogram import Dispatcher

from utils import strings
from .menu_commands_handler import *


def setup(dp: Dispatcher):
    # main menu
    dp.register_message_handler(menu_movie_commands, lambda message: message.text == strings.COMMANDS, state='*')
    dp.register_message_handler(menu_recommend, lambda message: message.text == strings.RECOMMEND, state='*')
    dp.register_message_handler(menu_voice_practice, lambda message: message.text == strings.VOICE_PRACTICE, state='*')
    dp.register_message_handler(menu_text_practice, lambda message: message.text == strings.TEXT_PRACTICE, state='*')
    dp.register_message_handler(menu_random_show, lambda message: message.text == strings.RANDOM_SHOW, state='*')
