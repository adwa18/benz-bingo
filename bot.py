"""
Benz Bingo Telegram Bot
Inline keyboard that opens the WebApp.

pip install python-telegram-bot
BOT_TOKEN=xxx WEBAPP_URL=https://your-server.com python bot.py
"""
import os
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN  = os.environ['BOT_TOKEN']
WEBAPP_URL = os.environ['WEBAPP_URL']   # e.g. https://your-server.com

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "🎱 Play Benz Bingo!",
            web_app=WebAppInfo(url=WEBAPP_URL)
        )
    ]])
    await update.message.reply_text(
        "🎱 *Welcome to Benz Bingo!*\n\n"
        "Pick your numbers, fill your card, shout BINGO!\n"
        "Bet 10, 50, 100 or 200 ETB and win big.\n\n"
        "_Tap the button below to launch the game:_",
        parse_mode='Markdown',
        reply_markup=keyboard
    )

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "ℹ️ *Benz Bingo Help*\n\n"
        "/start – Open the game\n"
        "/help  – Show this message\n\n"
        "How to play:\n"
        "1. Pick a bet amount (10/50/100/200 ETB)\n"
        "2. Choose a lucky number 1-100\n"
        "3. Get your random BINGO card\n"
        "4. Wait for the countdown & mark called numbers\n"
        "5. Complete a row/column/diagonal → claim BINGO!\n\n"
        "Winners receive 90% of the total pot. Good luck! 🍀",
        parse_mode='Markdown'
    )

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('help',  help_cmd))
    print('[Bot] Starting...')
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
