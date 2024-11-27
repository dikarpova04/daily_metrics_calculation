import datetime
import telebot
import os
from metrics_calculation import calculate_all_metrics 

today = datetime.datetime.now()
today_str = today.strftime('%Y-%m-%d')


token = os.getenv('TELEGRAM_TOKEN')
pg_user = os.getenv('PG_USER')
pg_password = os.getenv('PG_PASSWORD')
pg_host = os.getenv('PG_HOST')

if not all([token, pg_user, pg_password, pg_host]):
    raise ValueError("Отсутствуют необходимые переменные окружения!")


final_message = calculate_all_metrics(today, pg_host, pg_user, pg_password)

bot = telebot.TeleBot(token)
bot.send_message('346443137', f'Актуальная информация на {today_str} от Online Cinema:\n\n{final_message}')
