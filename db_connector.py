from typing import Any, Union
import sqlite3


class DataBaseConnector:
    def __init__(self, path_to_db: str, type_of_db: str) -> None:
        self.name = path_to_db
        self.type_of_db = type_of_db
        self.conn = sqlite3.connect(path_to_db)
        self.cursor = self.conn.cursor()
        print(f"получено соединение к базе данных. Путь до неё: {self.name}")
    
    async def create_table(self, peer_id: Any) -> None:
        cursor = self.conn.cursor() # если не будет работать, перепиши на обычный курсор
        # а именно: используй self.cursor()
        self.cursor.execute(
            f"""
            CREATE TABLE {self.type_of_db}_{peer_id}
            (number integer,  message text)
            """
        )
        self.conn.commit()
        print(f"создана новая таблица {self.type_of_db}_{peer_id} в базе данных {self.name}")
    
    async def clean_table(self, peer_id: Any) -> None: # тест с асинком
        cursor = self.conn.cursor() # если не будет работать, перепиши на обычный курсор
        # а именно: используй self.cursor()
        self.cursor.execute(f"DELETE FROM {self.type_of_db}_{peer_id}")
        self.conn.commit()
        print(f"таблица {self.type_of_db}_{peer_id} в базе данных {self.name} очищена")
    
    async def get_last_value(self, peer_id: Any) -> int:
        values: list = []
        for elem in self.cursor.execute(f"SELECT * FROM {self.type_of_db}_{peer_id}"):
            values.append(elem)
        self.conn.commit()
        if values:
            values = [elem[0] for elem in values]
            return max(values)
        return 0
    
    async def get_all_values_as_dict(self, peer_id: Any) -> dict:
        values: dict = {}
        for elem in self.cursor.execute(f"SELECT * FROM {self.type_of_db}_{peer_id}"):
            if elem:
                key, value = elem[0], elem[1]
                values[key] = value
        self.conn.commit()
        answer: dict = {}
        for key in sorted(values.keys()):
            answer[key] = values[key]
        return answer
    
    async def write_new_message(self, peer_id: Any, text: Union[str, None]) -> None:
        last_value: int = await self.get_last_value(peer_id)
        self.cursor.execute(
            f"""
            INSERT INTO {self.type_of_db}_{peer_id}
            VALUES ({last_value + 1}, "{text}")
            """
        )
        self.conn.commit()
        print(f"в базу данных {self.type_of_db}_{peer_id} добавлено новое сообщение '{text}' под номером {last_value + 1}")
    
    async def close_connect(self, peer_id: Any) -> None:
        self.conn.close()
        print(f"соединение с базой данных {self.name} прервано")
