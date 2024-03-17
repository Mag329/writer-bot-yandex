import asyncio

from bot.models import *


@bot.message_handler(commands=['start'])
async def start(message):
    logging.info(f"{message.from_user.id} - Отправка приветственного сообщения")
    markup = await generate_markup(messages['bot']['start']['buttons'])
    await bot.send_message(message.chat.id, messages['bot']['start']['text'], reply_markup=markup)
 

@bot.message_handler(func=lambda message: message.text in messages['bot']['start']['buttons'])
async def new(message):
    response, markup = await new_session(message)
    
    logging.info(f'{message.from_user.id} - Создание нового диалога')
    
    await bot.send_message(message.chat.id, response, reply_markup=markup)


@bot.message_handler(func=lambda message: message.text in messages['bot']['new']['buttons'])
async def style(message):
    try:
        try:
            await save_style(message, message.text)
            logging.info(f'{message.from_user.id} - Сохранение стиля')
        except Warning as e:
            await bot.send_message(message.chat.id, e)
            return
        
        markup = await generate_markup(messages['bot']['style']['buttons'])
        await bot.send_message(message.chat.id, messages['bot']['style']['text'], reply_markup=markup)
    except Warning as e:
        await bot.send_message(message.chat.id, e)


@bot.message_handler(func=lambda message: message.text in messages['bot']['style']['buttons'])
async def character(message):
    try:      
        await save_character(message, message.text)
        logging.info(f'{message.from_user.id} - Сохранение героя')
        
        markup = await generate_markup(messages['bot']['character']['buttons'])
        await bot.send_message(message.chat.id, messages['bot']['character']['text'], reply_markup=markup)
        
    except ValueError as e:
        await bot.send_message(message.chat.id, e)    


@bot.message_handler(func=lambda message: message.text in messages['bot']['character']['buttons'])
async def setting(message):
    try:
        try:
            await save_setting(message, message.text)
            logging.info(f'{message.from_user.id} - Сохранение сеттинга')
        except Warning as e:
            await bot.send_message(message.chat.id, e)
            return
        
        # markup = telebot.async_telebot.types.ReplyKeyboardRemove()
        markup = await generate_markup(messages['bot']['setting']['buttons'])

        await bot.send_message(message.chat.id, messages['bot']['setting']['text'], reply_markup=markup)
    except Warning as e:
        await bot.send_message(message.chat.id, e)
        

@bot.message_handler(commands=['debug'])
async def debug(message):
    logging.warning(f'{message.from_user.id} - Попытка использования /debug')
    text = message.text.replace('/debug ', '')
    if text == str(data['secret']['debug_code']):
        logging.warning(f'{message.from_user.id} - Верный пароль для /debug')
        with open("logs/latest.log", "rb") as f:
            await bot.send_document(message.chat.id, f)
    else:
        logging.warning(f'Неверный пароль для /debug')
        await bot.send_message(message.chat.id, messages['bot']['debug']['error'])


@bot.message_handler(commands=['debug_mode'])
async def debug_mode(message):
    logging.warning(f'{message.from_user.id} - Попытка использования /debug_mode')
    text = message.text.replace('/debug_mode ', '')
    if text == str(data['secret']['debug_code']):
        logging.warning(f'{message.from_user.id} - Верный пароль для /debug_mode')
        response = await set_debug_mode(message)
        await bot.send_message(message.chat.id, response)
    else:
        logging.warning(f'Неверный пароль для /debug_mode')
        await bot.send_message(message.chat.id, messages['models']['set_debug_mode']['error_code'])
        

@bot.message_handler(func=lambda message: message.text in messages['models']['statistics']['triger'])
async def show_statistics(message):
    response = await get_statistics(message)
    logging.info(f'{message.from_user.id} - Отправка статистики')
    await bot.send_message(message.chat.id, response)


@bot.message_handler(func=lambda message: message.text in messages['models']['tokens']['triger'])
async def show_tokens(message):
    tokens = await get_tokens(message)
    response = messages['models']['tokens']['text'].format(tokens, data['main']['token_limit']) # пришлось перенести в bot.py из-за нарушения глубины рекурсии
    
    logging.info(f'{message.from_user.id} - Отправка токенов')
    await bot.send_message(message.chat.id, response)


# Этот код рабочий (он для отправки всей истории), но закоментировал его потому что оставить его в bot.py не очень хорошая идея, а из models.py он не работает из-за глубины рекурсии 

# @bot.message_handler(func=lambda message: message.text in messages['bot']['all']['triger'])
# async def all(message):
#     try:
#         conn, cur = await db_conn()

#         cur.execute(f"""
#                      SELECT history FROM users WHERE user_id = {message.from_user.id} AND status = 'active'
#                      """)
#         history = cur.fetchone()
#         if history != None:
#             history = ast.literal_eval(history[0])
#             all_history = ''
#             for i in history:
#                 all_history += f" {i['text']}"
                
#             await bot.send_message(message.chat.id, all_history)
#         else:
#             await bot.send_message(message.chat.id, messages['bot']['all']['user_not_found'])

#     except ConnectionError as e:
#         print(e)

#     finally:
#         conn.close()


@bot.message_handler()
async def handle_message(message):
    # if messages['bot']['setting']['buttons'][0] in message.text:
    #     text = message.text.replace(messages['bot']['setting']['buttons'][0], '')
    
    # if messages['bot']['master']['buttons'][0] in message.text:
    #     text = text.replace(messages['bot']['master']['buttons'], '')
    
    markup = await generate_markup(messages['bot']['master']['buttons'])
        
    logging.debug(f"{message.from_user.id} - Запрос к GPT\nЗапрос: {message.text}")
    response = await gpt_request(message)
    
    response_parts = [response[i:i+4096] for i in range(0, len(response), 4096)]
    
    for part in response_parts:
        await bot.send_message(message.chat.id, part, reply_markup=markup) 




async def start():
    try:
        await db_tables_check()
    except ConnectionError:
        exit()
    try:
        connect_task = asyncio.create_task(connect())
        await asyncio.gather(connect_task, bot.polling())
    except ConnectionError:
        print("Ошибка подключения")
        connect_task.cancel()
    except Exception as e:
        print(f"Произошла ошибка: {e}")
    

if __name__ == '__main__':
    asyncio.run(start())