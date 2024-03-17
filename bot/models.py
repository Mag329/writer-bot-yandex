import aiohttp
import ast
import sqlite3
import asyncio
from datetime import datetime, timedelta
import asyncssh
import re
import yaml
import telebot

from bot.init import data, messages, logging, bot, config_path
        
    
async def debug_send(message, text):
    try:
        conn, cur = await db_conn()
        cur.execute(f"SELECT debug FROM users WHERE user_id = {message.from_user.id}")
        debug_modes = cur.fetchall()
        logging.info(f'Запрос к базе данных:\n{text}')
        if debug_modes is not None and any('enable' in mode for mode in debug_modes):
            await bot.send_message(message.chat.id, text)
            logging.info(f'{message.from_user.id} - DEBUG - сообщение отправлено')
    except sqlite3.Error as e: 
        print(e)
    finally:
        conn.close()


# Соединение с базой данных
async def db_conn():
    try:
        conn = sqlite3.connect(data['main']['db_path'])
        cur = conn.cursor()
        # cur.execute = debug_execute(cur.execute)
    except sqlite3.Error as e:
        logging.error(f'Ошибка подключения к базе данных\nError: {e}')

        await owner_send(data['secret']['owner_id'], messages['models']['db_conn']['owner'].format(e))
        
        raise ConnectionError(messages['models']['db_conn']['error']) from e
    
    return conn, cur



async def db_tables_check():
    try:
        conn, cur = await db_conn()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cur.fetchall()
        new_tables = []

        for table in tables:
            table = table[0]
            new_tables.append(table)
        tables = new_tables
        if not ('users' in tables):
            cur.execute("""CREATE TABLE `users` (
                        `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                        `user_id` text NOT NULL,
                        `history` text NOT NULL,
                        `style` text NOT NULL,
                        `character` text NOT NULL,
                        `setting` text NOT NULL,
                        `tokens` integer NOT NULL,
                        `status` text NOT NULL,
                        `debug` text NOT NULL)
            """)
            
        if not ('statistics' in tables):
            cur.execute("""CREATE TABLE `statistics` (
                        `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                        `style` TEXT NOT NULL,
                        `character` TEXT NOT NULL,
                        `setting` TEXT NOT NULL,
                        `text` TEXT NOT NULL,
                        `timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
                        """)
            
            
        conn.commit()
        return 
        
    except ConnectionError as e:
        logging.critical(f'Ошибка первоначального подключения к базе данных\n{e}')
        
        await owner_send(messages['models']['db_check']['owner1'].format(e))
        
        raise ConnectionError(messages['models']['db_check']['error1']) from e
    
    except Exception as e:
        logging.critical(f'Ошибка при проверке таблиц базы данных\n{e}')

        await owner_send(messages['models']['db_check']['owner2'].format(e))
        
    finally:
        conn.close()
        

async def new_session(message):   
    try:
        conn, cur = await db_conn()
        
        query = f"""
                SELECT * FROM users WHERE user_id = {message.from_user.id}
                """
        cur.execute(query)
        check_exist = cur.fetchone()
        await debug_send(message, query)
            
        # Проверяем по лимитам пользователей и сессий
        query = f"""
                SELECT count(*) FROM users WHERE user_id = {message.from_user.id}
                """
        cur.execute(query)
        sessions = cur.fetchone()[0]
        await debug_send(message, query)
        
        query = f"""
                SELECT count(*) FROM users WHERE status = 'active'
                """
        cur.execute(query)
        users = cur.fetchone()[0]
        await debug_send(message, query)
        
        markup = telebot.async_telebot.types.ReplyKeyboardRemove()
        
        if users >= data['main']['max_users']:
            return messages['models']['new_session']['users_limit'], markup
        else:
            if sessions >= data['main']['max_sessions']:
                return messages['models']['new_session']['sessions_limit'], markup
        
        # Изменяем статус на неактивный, если есть активная сессия
        if not (check_exist is None):
            query = f"""
                    UPDATE users SET status = 'inactive' WHERE user_id = {message.from_user.id} and status = 'active'
                    """
            cur.execute(query)
            await debug_send(message, query)
            
        # Создаем новую сессию
        query = f"""
                INSERT INTO users (user_id, history, style, character, setting, tokens, status, debug) VALUES ({message.from_user.id}, '{[]}', 'None', 'None', 'None', 0, 'active', 'disable')
                """
        cur.execute(query)
        conn.commit()
        await debug_send(message, query)
        
        logging.info(f'{message.from_user.id} - Новая сессия создана')
        
    except ConnectionError as e:
        print(e)
        
    except Exception as e:
        logging.critical(f'{message.from_user.id} - Ошибка при создание сессии\n{e}')
        await owner_send(messages['models']['new_session']['owner'].format(e))
        print(e)
        raise Warning(messages['models']['new_session']['error']) from e
    
    finally:
        conn.close()
    
    markup = await generate_markup(messages['bot']['new']['buttons'])
    
    return messages['bot']['new']['text'], markup

        
# Сохранение жанра в базу данных
async def save_style(message, style):
    try:
        conn, cur = await db_conn()
        
        query = f"""
                UPDATE users SET style = '{style}' WHERE user_id = {message.from_user.id} and status = 'active'
                """
        cur.execute(query)
        conn.commit()
        await debug_send(message, query)
        
        logging.info(f'{message.from_user.id} - Стиль сохранен: {style}')
        
    except ConnectionError as e:
        print(e)
        
    except Exception as e:
        logging.critical(f'{message.from_user.id} - Ошибка при сохранении жанра\n{e}')
        await owner_send(messages['models']['save_style']['owner'].format(e))
        print(e)
        raise Warning(messages['models']['save_style']['error']) from e
    
    finally:
        conn.close()


# Сохранение героя в базу данных

async def save_character(message, character):
    try:
        conn, cur = await db_conn()
        
        query = f"""
                SELECT * FROM users WHERE user_id = {message.from_user.id}
                """
        check_exist = cur.execute(query)
        await debug_send(message, query)
        if check_exist.fetchone() is None:
            raise  ValueError(messages['models']['save_character']['user_not_found']) 
        else:
            query = f"""
                    UPDATE users SET character = '{character}' WHERE user_id = {message.from_user.id} and status = 'active'
                    """
            cur.execute(query)
            await debug_send(message, query)
        
        conn.commit()
        
        logging.info(f'{message.from_user.id} - Герой сохранен: {character}')
        
    except ConnectionError as e:
        print(e)
    
    except Exception as e:
        logging.critical(f'{message.from_user.id} - Ошибка при сохранении героя\n{e}')
        await owner_send(messages['models']['save_character']['owner'].format(e))
        print(e)
        raise Warning(messages['models']['save_character']['error']) from e
    
    finally:
        conn.close()
        

# Сохранение сеттинга в базу данных

async def save_setting(message, setting):
    try:
        conn, cur = await db_conn()
        
        query = f"""
                SELECT * FROM users WHERE user_id = {message.from_user.id}
                """
        check_exist = cur.execute(query)
        await debug_send(message, query)
        if check_exist.fetchone() is None:
            raise  ValueError(messages['models']['save_setting']['user_not_found']) 
        else:
            query = f"""
                    UPDATE users SET setting = '{setting}' WHERE user_id = {message.from_user.id} and status = 'active'
                    """
            cur.execute(query)
            await debug_send(message, query)
        
        conn.commit()
        
        logging.info(f'{message.from_user.id} - Сеттинг сохранен: {setting}')
        
    except ConnectionError as e:
        print(e)
        
    except Exception as e:
        logging.critical(f'{message.from_user.id} - Ошибка при сохранении героя\n{e}')
        await owner_send(messages['models']['save_setting']['owner'].format(e))
        print(e)
        raise Warning(messages['models']['save_setting']['error']) from e
        
    finally:
        conn.close()


# Сохранение истории в базу данных

async def save_history(message, history):
    
    try:
        conn, cur = await db_conn()
        
        query = f"""
                SELECT * FROM users WHERE user_id = {message.from_user.id}
                """
        check_exist = cur.execute(query)
        await debug_send(message, query)
        if check_exist.fetchone() is None:
            raise  ValueError(messages['models']['save_history']['user_not_found']) 
        else:
            query = """
                    UPDATE users SET history = ? WHERE user_id = ? AND status = 'active'
                    """
            cur.execute(query, (str(history).replace('"', '\\"'), message.from_user.id))
            await debug_send(message, query)
        
        conn.commit()
        
        logging.info(f'{message.from_user.id} - История сохранена')
        
    except ConnectionError as e:
        print(e)
        
    except Exception as e:
        logging.critical(f'Ошибка при сохранении истории\n{e}')
        await owner_send(messages['models']['save_history']['owner'].format(e))
        print(e)
        
    finally:
        conn.close()


# Загрузка истории из базы данных

async def load_history(message):
    try:
        conn, cur = await db_conn()
        
        query = f"""
                SELECT history FROM users WHERE user_id = {message.from_user.id} and status = 'active'
                """
        history = cur.execute(query).fetchone()
        await debug_send(message, query)
        
        if history is None:
            raise  ValueError(messages['models']['load_history']['user_not_found'])
        else:
            history = history[0]
            
        if history != 'None': 
            return ast.literal_eval(history)
        else:
            return []
        
    except ConnectionError as e:
        print(e)

    finally:
        conn.close()


async def get_statistics(message):
    try:
        conn, cur = await db_conn()
        
        # Самый популярный жанр за 7 дней
        query = """
            SELECT style, COUNT(*) AS count 
            FROM statistics
            WHERE timestamp >= DATE('now', '-7 days')
            GROUP BY style
            ORDER BY count DESC
            LIMIT 1
        """
        popular_style = cur.execute(query).fetchone()
        await debug_send(message, query)
        
        if popular_style:
            popular_style_name = popular_style[0]
            popular_style_count = popular_style[1]
        else:
            popular_style_name = "Нет данных"
            popular_style_count = 0
        
        
        # Самый популярный герой за 7 дней
        query = """
            SELECT character, COUNT(*) AS count 
            FROM statistics
            WHERE timestamp >= DATE('now', '-7 days')
            GROUP BY character
            ORDER BY count DESC
            LIMIT 1
        """
        popular_character = cur.execute(query).fetchone()
        await debug_send(message, query)
        
        if popular_character:
            popular_character_name = popular_character[0]
            popular_character_count = popular_character[1]
        else:
            popular_character_name = "Нет данных"
            popular_character_count = 0
            
            
        # Самый популярный герой за 7 дней
        query = """
            SELECT setting, COUNT(*) AS count 
            FROM statistics
            WHERE timestamp >= DATE('now', '-7 days')
            GROUP BY setting
            ORDER BY count DESC
            LIMIT 1
        """
        popular_setting = cur.execute(query).fetchone()
        await debug_send(message, query)
        
        if popular_setting:
            popular_setting_name = popular_setting[0]
            popular_setting_count = popular_setting[1]
        else:
            popular_setting_name = "Нет данных"
            popular_setting_count = 0
        
        
        # Последние 3 запроса
        query = """
            SELECT text FROM statistics
            ORDER BY id DESC
            LIMIT 3
        """
        last_queries = cur.execute(query).fetchall()
        await debug_send(message, query)
    except ConnectionError as e:
        print(e)
        
    finally:
        conn.close()
    
    response = messages['models']['statistics']['text'].format(popular_style_name, popular_style_count, popular_character_name, popular_character_count, popular_setting_name, popular_setting_count)
    for query in last_queries:
        response += messages['models']['statistics']['last_messages'].format(query[0])
        
    return response        


# Подсчет токенов в тексте
async def count_tokens(message, text):
    async with aiohttp.ClientSession() as session:
        try:    
            async with session.post(
                'https://llm.api.cloud.yandex.net/foundationModels/v1/tokenize',
                headers = {
                    'Authorization': f"Bearer {data['secret']['iam_token']}",
                    'Content-Type': 'application/json'
                },
                json={
                    "modelUri": f"gpt://{data['secret']['folder_id']}/yandexgpt-lite/latest",
                    "text": text
                }
            ) as resp:
                if resp.status == 200:
                    return len((await resp.json())['tokens'])
                
        except aiohttp.ClientConnectionError as e:
            logging.error(f'{message.from_user.id} - Ошибка подключения: {e}')
            await owner_send(messages['models']['count_tokens']['owner_bad_connection'].format(e))
            raise Warning(messages['models']['count_tokens']['bad_connection']) from e
            # return messages['models']['count_tokens']['bad_connection']
        
        except Exception as e:
            logging.error(f'{message.from_user.id} - Непредвиденная ошибка: {e}')
            await owner_send(messages['models']['count_tokens']['owner_unexpected_error'].format(e))
            raise Warning(messages['models']['count_tokens']['unexpected_error']) from e
            # return messages['models']['count_tokens']['unexpected_error']

# Получить количество токенов

async def get_tokens(message):
    try:
        conn, cur = await db_conn()

        query = f"""
                SELECT tokens FROM users WHERE user_id = {message.from_user.id} AND status = 'active'
                """
        cur.execute(query)
        await debug_send(message, query)
        tokens = cur.fetchone()[0]

    except ConnectionError as e:
        print(e)

    finally:
        conn.close()

    logging.info(f'{message.from_user.id} - Токенов: {tokens}')
    return tokens


# Обновить количество токенов

async def update_tokens(message, tokens, set=False):
    try:
        conn, cur = await db_conn()

        if not set:
            old_tokens = await get_tokens(message)
            tokens = int(tokens) + int(old_tokens)
        
        query = f"""
                UPDATE users SET tokens = {tokens} WHERE user_id = {message.from_user.id} and status = 'active'
                """
        cur.execute(query)
        await debug_send(message, query)

        conn.commit()

    except ConnectionError as e:
        print(e)

    finally:
        conn.close()

    logging.info(f'{message.from_user.id} - Токены обновлены')


# запрос к GPT

async def gpt_request(message):
    try:
        history = await load_history(message)
    except ValueError as e:
        return e
    
    # Сколько токенов было до запроса
    tokens_before = await get_tokens(message)
    
    try:
        conn, cur = await db_conn()
        
        query = f"""
                SELECT style FROM users WHERE user_id = {message.from_user.id} and status = 'active'
                """
        style = cur.execute(query).fetchone()
        await debug_send(message, query)
        
        if style is None:
            return messages['models']['gpt_request']['not_style']
        else:
            style  = style[0]
        
        query = f"""
                SELECT character FROM users WHERE user_id = {message.from_user.id} and status = 'active'
                """
        character = cur.execute(query).fetchone()
        await debug_send(message, query)
        
        if character is None:
            return messages['models']['gpt_request']['not_character']
        else:
            character = character[0]
            
        query = f"""
                SELECT setting FROM users WHERE user_id = {message.from_user.id} and status = 'active'
                """
        setting = cur.execute(query).fetchone()
        await debug_send(message, query)
        
        if setting is None:
            return messages['models']['gpt_request']['not_setting']
        else:
            setting = setting[0]

        
        query = f"""
                INSERT INTO statistics (style, character, setting, text) VALUES ('{style}', '{character}', '{setting}', '{message.text}')
                """
        cur.execute(query)
        conn.commit()
        await debug_send(message, query)
        
    except ConnectionError as e:
        print(e)
        
    finally:
        conn.close()
        
    # Если история пустая, то добавляем системный текст и токены к истории
    if history == []:
        system_content = data['main']['system_content'].format(style, character, setting)
        history.append({"role": "system", "text": system_content})
        try:
            system_tokens = await count_tokens(message, system_content)
        except Warning as e:
            return e
        
        await update_tokens(message, system_tokens)


    content = message.text

    old_tokens = await get_tokens(message)
    try:
        new_tokens = await count_tokens(message, content)
    except Warning as e:
            return e
    
    
    if new_tokens > data['main']['max_tokens_request']:
        return messages['models']['gpt_request']['too_long']
    else:
        await update_tokens(message, new_tokens)
        
    if int(old_tokens) + int(new_tokens) > data['main']['token_limit']:
        return messages['models']['gpt_request']['token_limit']
    
    history.append({"role": "user", "text": content})
    
    # history.append({"role": "assistant", "text": data['main']['assistant_content'] + answer})

    async def keep_typing(chat_id):
        while True:
            await bot.send_chat_action(chat_id=chat_id, action='typing')
            await asyncio.sleep(5)  # Задержка между отправками действия "typing"

    typing_task = asyncio.create_task(keep_typing(message.chat.id))
    
    
    history_messages = []
    for row in history:
        history_messages.append({
            "role": row["role"],
            "text": row["text"]})
    
    async with aiohttp.ClientSession() as session:
        try:    
            async with session.post(
                'https://llm.api.cloud.yandex.net/foundationModels/v1/completion',
                headers = {
                    'Authorization': f"Bearer {data['secret']['iam_token']}",
                    'Content-Type': 'application/json'
                },
                json={
                    "modelUri": f"gpt://{data['secret']['folder_id']}/yandexgpt/latest",
                    "completionOptions": {
                        "stream": False, 
                        "temperature": data['main']['temperature'],
                        "maxTokens": str(data['main']['max_tokens_response'])
                    },
                    "messages": history_messages
                }
            ) as resp:
                logging_text = (f"""- Request: {
                                        'https://llm.api.cloud.yandex.net/foundationModels/v1/completion',
                                        {
                                            'Authorization': f"Bearer {data['secret']['iam_token']}",
                                            'Content-Type': 'application/json'
                                        },
                                        {
                                            "modelUri": f"gpt://{data['secret']['folder_id']}/yandexgpt-lite/latest",
                                            "completionOptions": {
                                                "stream": False, 
                                                "temperature": data['main']['temperature'],
                                                "maxTokens": str(data['main']['max_tokens_response'])
                                            },
                                            "messages": history_messages}}\n\n"""
                                f'- Response: {str(resp.status)}\n\n'
                                f'- Response Body: {resp.text}')
                logging.info(f'{message.from_user.id} - Запрос к нейросети:\n{logging_text}')
                await debug_send(message, logging_text)

                if resp.status == 200:
                    result = (await resp.json())['result']['alternatives'][0]['message']['text']
                    tokens_usage = (await resp.json())['result']['usage']['completionTokens']
                    await update_tokens(message, tokens_usage)
                    
                    history.append({"role": "assistant", "text": result})
                    await save_history(message, history)

                    typing_task.cancel()

                    tokens = await get_tokens(message)

                    return f"{result}\n{messages['models']['gpt_request']['text_end'].format(tokens, data['main']['token_limit'])}"
                else:
                    await update_tokens(message, tokens_before, True)
                    logging.error(f'{message.from_user.id} - Не удалось получить ответ от нейросети\nТекст ошибки: {await resp.json()}')
                    typing_task.cancel()
                    return messages['models']['gpt_request']['bad_response']
        except aiohttp.ClientConnectionError as e:
            logging.error(f'{message.from_user.id} - Ошибка подключения: {e}')
            await owner_send(messages['models']['gpt_request']['owner_bad_connection'].format(e))
            typing_task.cancel()
            return messages['models']['gpt_request']['bad_connection']
        
        except Exception as e:
            logging.error(f'{message.from_user.id} - Непредвиденная ошибка: {e}')
            await owner_send(messages['models']['gpt_request']['owner_unexpected_error'].format(e))
            typing_task.cancel()
            return messages['models']['gpt_request']['unexpected_error']
            
            
            
# Отправка сообщения владельцу бота
async def owner_send(message): 
    await bot.send_message(data['secret']['owner_id'], message)


async def generate_markup(buttons):
    markup = telebot.async_telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
    for button in buttons:
        markup.add(button)
        
    return markup



async def set_debug_mode(message):
    try:
        conn, cur = await db_conn()

        query = f"""
                SELECT debug FROM users WHERE user_id = {message.from_user.id}
                """
        cur.execute(query)
        await debug_send(message, query)   
        debug_mode = cur.fetchone()
        if debug_mode != None:
            debug_mode = debug_mode[0]
        else:
            return messages['models']['set_debug_mode']['user_not_found']
            
        if debug_mode == 'disable':
            query = f"""
                    UPDATE users SET debug = 'enable' WHERE user_id = {message.from_user.id}
                    """
            cur.execute(query)
            conn.commit()
            await debug_send(message, query)
            logging.info(f'{message.from_user.id} - Включен режим отладки')
            return messages['models']['set_debug_mode']['enable']
        elif debug_mode == 'enable':
            query = f"""
                    UPDATE users SET debug = 'disable' WHERE user_id = {message.from_user.id}
                    """
            cur.execute(query)
            conn.commit()
            await debug_send(message, query)
            logging.info(f'{message.from_user.id} - Выключен режим отладки')
            return messages['models']['set_debug_mode']['disable']
        
    except ConnectionError as e:
        print(e)

    finally:
        conn.close()
    

# Авто получение токена с сервера
async def run_command(host, username, key_file):
    async with asyncssh.connect(host, username=username, client_keys=[key_file]) as conn:
        result = await conn.run('curl -H Metadata-Flavor:Google 169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token')
        access_token = re.search(r'"access_token":"(.+?)"', result.stdout)
        if access_token:
            logging.info(f'Токен получен: {access_token.group(1)}')
            return access_token.group(1)
        else:
            return None


async def get_iam_token(host, username, key_file):
    access_token = await run_command(host, username, key_file)
    if access_token:
        # Сохраняем токен и текущее время в конфиг
        data['secret']['iam_token'] = access_token
        data['secret']['token_created_at'] = datetime.now().isoformat()
        try:
            with open(config_path, 'w') as f:
                yaml.dump(data, f, allow_unicode=True)
        except yaml.YAMLError as e:
            print(f"Ошибка при сохранении токена в config.yaml: \n{e}")
            logging.critical(messages['models']['get_token']['error1'].format(e))
            owner_send(messages['models']['get_token']['owner1'].format(e))
    else:
        print("Не удалось получить IAM-токен")
        logging.critical(messages['models']['get_token']['error2'])
        owner_send(messages['models']['get_token']['owner2'])


async def connect():
    host = data['secret']['ssh']['host']
    username = data['secret']['ssh']['username']
    key_file = data['secret']['ssh']['key_file']

    while True:
        # Проверяем срок действия токена
        token_created_at = data['secret'].get('token_created_at')
        if token_created_at:
            token_created_at = datetime.fromisoformat(token_created_at)
            token_expiration = token_created_at + timedelta(hours=12)
            if datetime.now() >= token_expiration:
                await get_iam_token(host, username, key_file)
        else:
            await get_iam_token(host, username, key_file)

        # Задержка перед следующей проверкой (1 час)
        await asyncio.sleep(3600)