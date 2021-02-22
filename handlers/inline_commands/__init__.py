from aiogram import Dispatcher

from handlers.inline_commands import commands_handler


def setup(dp: Dispatcher):
    dp.register_callback_query_handler(commands_handler.distribution,
                                       lambda callback: callback.data.split("::")[0] == "command",
                                       state='*')


