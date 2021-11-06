# -*- coding: utf-8 -*-
from typing import Any, Union, Optional
from os import mkdir
import sqlite3


class AsyncDataBaseConnector:
    def __init__(self, path_to_db: str, type_of_db: str) -> None:
        self.name = path_to_db
        self.type_of_db = type_of_db
        for path in self.name.split("/"):
            try: mkdir(path)
            except: pass
        self.conn = sqlite3.connect(path_to_db)
        cursor = self.conn.cursor()
        print(f"получено соединение к базе данных. Путь до неё: {self.name}")
        try:
            cursor.execute("""
            CREATE TABLE chats_status
            (chat text, opens_status integer)
            """)
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("""
            CREATE TABLE our_chat
            (number integer,  message text)
            """)
        except sqlite3.OperationalError:
            pass
        self.conn.commit()
    
    async def create_table(self, peer_id: Union[Any, int, str], is_ls_flag: bool) -> None:
        if is_ls_flag:
            return
        cursor = self.conn.cursor() # если не будет работать, перепиши на обычный курсор
        # а именно: используй self.cursor()
        table_name: str = f'{self.type_of_db}_{peer_id}'
        try:
            cursor.execute(
                f"""
                CREATE TABLE {table_name}
                (number integer,  message text)
                """
            )
            cursor.execute(f"""
            UPDATE chats_status
            SET '{table_name}' = 1
            """)
            self.conn.commit()
            print(f"создана новая таблица {table_name} в базе данных {self.name}")
        except sqlite3.OperationalError:
            print(f"таблица {table_name} в базе данных {self.name} уже существует")
    
    async def clean_table(self, peer_id: Union[Any, int, str], is_ls_flag: bool) -> None: # тест с асинком
        if is_ls_flag:
            return
        table_name: str = f'{self.type_of_db}_{peer_id}'
        cursor = self.conn.cursor() # если не будет работать, перепиши на обычный курсор
        # а именно: используй self.cursor()
        cursor.execute(f"DELETE FROM {table_name}")
        self.conn.commit()
        print(f"таблица {table_name} в базе данных {self.name} очищена")
    
    async def get_last_value(self, peer_id: Union[Any, int, str, None], is_ls_flag: bool) -> int:
        table_name: str = f'{self.type_of_db}_{peer_id}' if not is_ls_flag else 'our_chat'
        cursor = self.conn.cursor()
        values: list = []
        for elem in cursor.execute(f'SELECT * FROM "{table_name}"'):
            values.append(elem)
        self.conn.commit()
        if values:
            values = [elem[0] for elem in values]
            return max(values)
        return 0
    
    async def get_all_values_as_dict(self, peer_id: Union[Any, int, str, None], is_ls_flag: bool) -> dict:
        table_name: str = f'{self.type_of_db}_{peer_id}' if not is_ls_flag else 'our_chat'
        cursor = self.conn.cursor()
        values: dict = {}
        for elem in cursor.execute(f"SELECT * FROM {table_name}"):
            if elem:
                key, value = elem[0], elem[1]
                values[key] = value
        self.conn.commit()
        answer: dict = {}
        for key in sorted(values.keys()):
            answer[key] = values[key]
        return answer
    
    async def write_new_message(self, peer_id: Union[Any, int, str, None], text: Optional[str], is_ls_flag: bool) -> None:
        table_name: str = f'{self.type_of_db}_{peer_id}'
        if is_ls_flag:
            table_name = 'our_chat'
        cursor = self.conn.cursor()
        last_value: int = await self.get_last_value(peer_id, is_ls_flag)
        cursor.execute(
            f"""
            INSERT INTO {table_name}
            VALUES ({last_value + 1}, "{text}")
            """
        )
        if not is_ls_flag:
            data = cursor.execute(f"""SELECT opens_status FROM chats_status WHERE chat = '{table_name}'""").fetchone()
            chat_status_is_open: bool = bool(data)
            if chat_status_is_open:
                cursor.execute(
                    f"""
                    INSERT INTO our_chat
                    VALUES ({last_value + 1}, "{text}")
                    """
                )
        self.conn.commit()
        print(f"в базу данных {table_name} добавлено новое сообщение '{text}' под номером {last_value + 1}")
    

    async def change_status_table(self, peer_id: Union[Any, int, str]) -> bool:
        table_name: str = f'{self.type_of_db}_{peer_id}'
        cursor = self.conn.cursor()
        data = cursor.execute(f"""SELECT {table_name} FROM chat_status WHERE chat = '{table_name}'""").fetchone()
        chat_status_is_open: bool = bool(data[0])
        cursor.execute(f"""
        UPDATE chat_status
        SET {table_name} = {int(not chat_status_is_open)}
        """)
        self.conn.commit()
        return chat_status_is_open
    
    async def close_connect(self, peer_id: Union[Any, int, str]) -> None:
        self.conn.close()
        print(f"соединение с базой данных {self.name} прервано")
