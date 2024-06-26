from telebot.async_telebot import AsyncTeleBot

import os
import logging
from datetime import datetime
import gzip
import yaml


current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

current_log_path = os.path.join(os.getcwd(), "logs/latest.log")

new_log_filename = f"log_{current_datetime}.log"
new_log_path = os.path.join(os.getcwd(), f'logs/{new_log_filename}')

log_archive_path = os.path.join(os.getcwd(), f"logs/log_{current_datetime}.gz")

# Проверяем, существует ли текущий файл логов
if os.path.exists(current_log_path):
    # Переименовываем текущий файл логов

    os.rename(current_log_path, new_log_path)
    
    with open(new_log_path, 'rb') as log:
        with gzip.open(log_archive_path, 'wb') as f:
            f.writelines(log)

    # Удаляем файл логов после архивации
    os.remove(new_log_path)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s\n\n",
    filename="logs/latest.log",
    filemode="w",
)


# Загрузка конфиг файла
config_path = os.path.join(os.getcwd(), 'config.yaml')
try:
    with open(config_path, encoding='utf-8') as f:
        data = yaml.safe_load(f)
except yaml.YAMLError as e:
    print(f"Ошибка при загрузке файла config.yaml: \n{e}")
    logging.critical(f"Ошибка при загрузке файла config.yaml: \n{e}")
    exit()


# Загрузка файла сообщений
messages_path = os.path.join(os.getcwd(), 'messages.yaml')
try:
    with open(messages_path, encoding='utf-8') as f:
        messages = yaml.safe_load(f)
except yaml.YAMLError as e:
    print(f"Ошибка при загрузке файла messages.yaml: \n{e}")
    logging.critical(f"Ошибка при загрузке файла messages.yaml: \n{e}")
    exit()


# Инициализация бота
bot = AsyncTeleBot(data['secret']['telegram_token'])
logging.info('Бот запущен')
print(messages['models']['start']['text'])


