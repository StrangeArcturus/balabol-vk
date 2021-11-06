# -*- coding: utf-8 -*-
import re
from os import mkdir
from asyncio import sleep, run
from random import choice, random


from markovify import NewlineText
from vkbottle import DocMessagesUploader
from vkbottle.api.api import API
from vkbottle.bot import Bot, Message
from vkbottle.dispatch.rules.bot import ChatActionRule, FromUserRule
from sqlite3 import OperationalError

from db_connector import AsyncDataBaseConnector
from config import *  # BOT_TOKEN, RESPONSE_CHANCE, RESPONSE_DELAY


connector = AsyncDataBaseConnector("db/balabol.db", "vk")
bot = Bot(BOT_TOKEN)
pattern = re.compile(r"\[id(\d*?)\|.*?]")
print("бот начал слушать сообщения")


@bot.on.chat_message(ChatActionRule("chat_invite_user"))
async def invited(message: Message) -> None:
    is_ls_flag: bool = False
    """Приветствие при приглашении бота в беседу."""
    if message.group_id == -message.action.member_id:
        await connector.create_table(message.peer_id, is_ls_flag)
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
    is_ls_flag: bool = False
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
            await connector.clean_table(peer_id, is_ls_flag)
        except FileNotFoundError:
            pass

        await message.answer(f"@id{from_id}, база данных успешно сброшена.")
        print("база данных успешно сброшена")
    else:
        await message.answer("Сбрасывать базу данных могут только администраторы.")
        print("отклонено. Сбрасывать базу данных могут только администраторы.")


@bot.on.message(text=['/ping', '/пинг', '/PING', '/ПИНГ'])
async def ping(message: Message) -> None:
    """
    Проверка бота на работоспособность, пинг-понг
    """
    text: str = message.text.lower()
    ping_pong: dict = {
        '/ping': 'pong',
        '/пинг': 'понг'
    }
    await message.answer(ping_pong[text])


@bot.on.chat_message(text=['/change_status'])
async def on_off_private(message: Message) -> None:
    peer_id = message.peer_id
    status = await connector.change_status_table(peer_id=peer_id)
    if status:
        await message.answer('История чата была доступна боту для обслуживания лс, но теперь его история приватная')
    else:
        await message.answer('Чат был приватным и не использовался для обсуживания лс, но теперь его история публичная')


@bot.on.message(text=['/get_db'])
async def get_db_file(message: Message) -> None:
    """
    Отправка ботом сообщение юзеру с файлом базы данных
    (мало ли что)
    """
    peer_id = message.peer_id
    text = 'Появилась нужда в отправке базы данных'
    await db_dump(peer_id=peer_id, text=text)
    await message.answer(
        text=text,
        attachment='db/balabol.db'
    )


@bot.on.chat_message()
async def talk_chats(message: Message) -> None:
    is_ls_flag = False
    peer_id = message.peer_id
    text = message.text.lower()
    print(f"заметил сообщение в беседе:\n'{text}'")

    if text:
        if re.fullmatch('(https?://[\w.-]+)', text):
            return
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
        try:
            await connector.write_new_message(peer_id, text, is_ls_flag)
        except OperationalError:
            await connector.create_table(message.peer_id, is_ls_flag)
            await connector.write_new_message(peer_id, text, is_ls_flag)
    if random() > RESPONSE_CHANCE:
        print(f'отвечать в чат не буду(')
        return

    # Чтение истории беседы
    messages_dict: dict = await connector.get_all_values_as_dict(peer_id, is_ls_flag)
    messages_list: list = [messages_dict[key] for key in messages_dict.keys()]
    db: str = "\n".join(messages_list)
    db = db.strip().lower()
    public_messages_dict: dict = await connector.get_all_values_as_dict(peer_id, is_ls_flag)
    public_messages_list: list = [public_messages_dict[key] for key in messages_dict.keys()]
    db += "\n".join(public_messages_list).strip().lower()

    # Задержка перед ответом
    await sleep(RESPONSE_DELAY)

    # Генерация сообщения
    text_model = NewlineText(input_text=db, well_formed=False, state_size=1)
    sentence = text_model.make_sentence(tries=1000) or choice(db.splitlines())
    sentence = pretty_message(sentence)
    print(f"отвечаю на сообщениею Мой ответ: {sentence}")

    await message.answer(sentence)


@bot.on.private_message()
async def talk_private(message: Message) -> None:
    is_ls_flag = True
    peer_id = message.peer_id
    text = message.text.lower()
    print(f"заметил сообщение в беседе:\n'{text}'")

    if text:
        if re.fullmatch('(https?://[\w.-]+)', text):
            return
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
        try:
            await connector.write_new_message(peer_id, text, is_ls_flag)
        except OperationalError:
            await connector.create_table(message.peer_id, is_ls_flag)
            await connector.write_new_message(peer_id, text, is_ls_flag)
    if random() > RESPONSE_CHANCE:
        print(f'отвечать в чат не буду(')
        return

    # Чтение истории беседы
    messages_dict: dict = await connector.get_all_values_as_dict(peer_id, True)
    messages_list: list = [messages_dict[key] for key in messages_dict.keys()]
    db: str = "\n".join(messages_list)
    db = db.strip().lower()

    # Задержка перед ответом
    await sleep(RESPONSE_DELAY)

    # Генерация сообщения
    text_model = NewlineText(input_text=db, well_formed=False, state_size=1)
    sentence = text_model.make_sentence(tries=1000) or choice(db.splitlines())
    sentence = pretty_message(sentence)
    print(f"отвечаю на сообщениею Мой ответ: {sentence}")

    await message.answer(sentence)


async def db_dump(text='Появилась нужда в отправке базы данных', peer_id=OWNER_ID) -> None:
    """
    экстренный сброс базы данных владельцу
    """
    api = bot.api
    uploader = DocMessagesUploader(api=api, generate_attachment_strings=True)
    data = await uploader.upload(title='balabol.db', file_source="db/balabol.db", peer_id=peer_id)
    sender_api = API(token=BOT_TOKEN)
    await sender_api.messages.send(
        user_id=peer_id, attachment=data,
        message=text, random_id=0
    )


def pretty_message(text: str) -> str:
    left = ' —!"#$%&\')*+,-./:;=>?@[\\]^_`{|}~'
    right = ' —#$%&\'*+,-/<=>@[\\]^_`{|'
    for char in left:
        text = text.lstrip(char)
    for char in right:
        text = text.rstrip(char)
    return text


if __name__ == "__main__":
    try:
        bot.run_forever()
    except Exception as e:
        excps: tuple = (
            SystemExit, KeyboardInterrupt, OSError,
            InterruptedError, SystemError
        )
        if isinstance(e, excps):
            run(db_dump())
