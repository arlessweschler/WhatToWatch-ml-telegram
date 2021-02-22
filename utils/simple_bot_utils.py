API_TOKEN = ""

import telebot

bot = telebot.TeleBot(API_TOKEN)


def send(msg: str):
    bot.send_message(0, msg,parse_mode='HTML')

