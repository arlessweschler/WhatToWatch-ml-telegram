from datetime import datetime
from enum import IntEnum
from database import DBService as db


class Client:
    pk: int
    current_state: int

    def __init__(self, pk: int, current_state: int, created_at: datetime):
        self.pk = pk
        self.current_state = current_state
        self.created_at = created_at

    async def save(self):
        await db.ClientDB.update(self)


class ClientStates(IntEnum):
    STARTED = 0
