from aiogram.utils import executor

from config.misc import dp, poster_bot
from database.DBService import prepare_db


async def on_startup(*args, **kwargs):
    await prepare_db()
    import handlers
    handlers.setup(dp)

    # await preapare_db()
    print("started")


if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup)
