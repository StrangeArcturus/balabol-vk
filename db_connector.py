from typing import Any, Union
import sqlite3


class DataBaseConnector:
    def __init__(self, path_to_db: str) -> None:
        self.name = path_to_db
        self.conn = sqlite3.connect(path_to_db)
        self.cursor = conn.cursor()
        print(f"получено соединение к базе данных. Путь до неё: {self.path_to_db}")
    
    def create_table(self, peer_id: Any) -> None:
        self.cursor.execute(
            f"""
            CREATE TABLE vk_{peer_id}
            (number integer,  message text)
            """
        )
        self.conn.commit()
        print(f"создана новая таблица vk_{peer_id} в базе данных {self.path_to_db}")
    
    def clean_table(self, peer_id: Any) -> None:
        self.cursor.execute(f"TRUNCATE TABLE vk_{peer_id}")
        self.conn.commit()
        print(f"таблица vk_{peer_id} в базе данных {self.path_to_db} очищена")
    
    def get_last_value(self, peer_id: Any) -> int:
        values: list = []
        for elem in self.cursor.execute(f"SELECT * FROM vk_{peer_id}"):
            values.append(elem)
        self.conn.commit()
        if values:
            values = [elem[0] for elem in values]
            return max(values)
        return 0
    
    def get_all_values_as_dict(self, peer_id: Any) -> dict:
        values: dict = {}
        for elem in self.cursor.execute(f"SELECT * FROM vk_{peer_id}"):
            if elem:
                key, value = elem[0], elem[1]
                values[key] = value
        self.conn.commit()
        answer: dict = {}
        for key in sorted(values.keys()):
            answer[key] = values[key]
        return answer
    
    def write_new_message(self, peer_id: Any, text: Union[text, None]) -> None:
        last_value: int = self.get_last_value(peer_id)
        self.cursor.execute(
            f"""
            INSERT INTO vk_{peer_id}
            VALUES ({last_value + 1}, "{text}")
            """
        )
        self.conn.commit()
        print(f"в базу данных vk_{peer_id} добавлено новое сообщение '{text}' под номером {last_value + 1}")
    
    def close_connect(self, peer_id: Any) -> None:
        self.conn.close()
        print(f"соединение с базой данных {self.path_to_db} прервано")
