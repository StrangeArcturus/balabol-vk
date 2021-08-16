# -*- coding: utf-8 -*-
import re
from asyncio import sleep
from os import mkdir, remove, listdir
from random import choice, random

# from aiofiles import open
# import aiofiles

import sqlite3 # with database, not txt file

from markovify import NewlineText
from vkbottle.bot import Bot, Message
from vkbottle.dispatch.rules.bot import ChatActionRule, FromUserRule

from db_connector import DataBaseConnector
from config import *  # BOT_TOKEN, RESPONSE_CHANCE, RESPONSE_DELAY

connector = DataBaseConnector("db/balabol.db", "vk")
bot = Bot(BOT_TOKEN)
print("бот начал слушать сообщения")


@bot.on.chat_message(ChatActionRule("chat_invite_user"))
async def invited(message: Message) -> None:
    """Приветствие при приглашении бота в беседу."""
    if message.group_id == -message.action.member_id:
        await connector.create_table(message.peer_id)
        print("пригласили в новый чат")
        await message.answer(
            """
Всем привет! Я Балабол, я учусь человеческой речи, и для каждого чата
"обучен" я буду в зависимости от ваших сообщений)
А для работы мне нужно выдать доступ к переписке или права администратора.
Для сброса базы данных этого чата используйте команды /сброс или /reset"""
        )


@bot.on.chat_message(text=["/сброс", "/reset"])
async def reset(message: Message) -> None:
    """Сброс базы данных администратором беседы."""
    peer_id = message.peer_id
    print(f"послана команда сбросить базу данных для чата {peer_id}")
    try:
        members = await message.ctx_api.messages.get_conversation_members(
            peer_id=peer_id
        )
    except Exception:
        await message.answer(
            "Не удалось проверить, являетесь ли вы администратором, "
            + "потому что я не администратор."
        )
        return
    admins = [member.member_id for member in members.items if member.is_admin]
    from_id = message.from_id
    if from_id in admins:

        # Удаление базы данных беседы
        try:
            # remove(f"db/{peer_id}.db") # not txt
            await connector.clean_table(peer_id)
        except FileNotFoundError:
            pass

        await message.answer(f"@id{from_id}, база данных успешно сброшена.")
        print("база данных успешно сброшена")
    else:
        await message.answer("Сбрасывать базу данных могут только администраторы.")
        print("отклонено. Сбрасывать базу данных могут только администраторы.")


@bot.on.chat_message(FromUserRule())
async def talk(message: Message) -> None:
    peer_id = message.peer_id
    text = message.text.lower()
    print(f"заметил сообщение в беседе:\n'{text}'")

    if text:
        # Удаление пустых строк из полученного сообщения
        while "\n\n" in text:
            text = text.replace("\n\n", "\n")

        # Преобразование [id1|@durov] в @id1
        user_ids = tuple(set(pattern.findall(text)))
        for user_id in user_ids:
            text = re.sub(rf"\[id{user_id}\|.*?]", f"@id{user_id}", text)

        # Создание папки db, если не создана
        try:
            mkdir("db")
        except FileExistsError:
            pass
        
        # запись нового сообщения в базу данных чата
        await connector.write_new_message(peer_id, text)
    if random() > RESPONSE_CHANCE:
        return

    # Чтение истории беседы
    messages_dict: dict = await connector.get_all_values_as_dict(peer_id)
    messages_list: list = [messages_dict[key] for key in messages_dict.keys()]
    db: str = "\n".join(messages_list)
    db = db.strip().lower()

    # Задержка перед ответом
    await sleep(RESPONSE_DELAY)

    # Генерация сообщения
    text_model = NewlineText(input_text=db, well_formed=False, state_size=4)
    sentence = text_model.make_sentence(tries=1000) or choice(db.splitlines())
    print(f"отвечаю на сообщениею Мой ответ: {sentence}")

    await message.answer(sentence)


if __name__ == "__main__":
    pattern = re.compile(r"\[id(\d*?)\|.*?]")
    bot.run_forever()
