import sqlite3
import os
import re
import logging
import asyncio
import yt_dlp
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = '8635943612:AAH3EY_zCYvz3SR2r6T_ZvjHz7jh7DtNxXc'

conn = sqlite3.connect('downloads.db', check_same_thread=False)
cur = conn.cursor()

cur.execute('''
    CREATE TABLE IF NOT EXISTS downloads
    (artist TEXT, song TEXT, count INTEGER, PRIMARY KEY (artist, song))
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS user_downloads
    (user_id INTEGER, artist TEXT, song TEXT, count INTEGER,
     PRIMARY KEY (user_id, artist, song))
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS cached_tracks
    (artist TEXT, song TEXT, filename TEXT UNIQUE,
     PRIMARY KEY (artist, song))
''')
conn.commit()

def log_download(user_id: int, artist: str, song: str):
    cur.execute("SELECT count FROM downloads WHERE artist=? AND song=?", (artist, song))
    row = cur.fetchone()
    if row:
        cur.execute("UPDATE downloads SET count = count + 1 WHERE artist=? AND song=?", (artist, song))
    else:
        cur.execute("INSERT INTO downloads VALUES (?, ?, 1)", (artist, song))

    cur.execute("SELECT count FROM user_downloads WHERE user_id=? AND artist=? AND song=?", (user_id, artist, song))
    row_user = cur.fetchone()
    if row_user:
        cur.execute("UPDATE user_downloads SET count = count + 1 WHERE user_id=? AND artist=? AND song=?", (user_id, artist, song))
    else:
        cur.execute("INSERT INTO user_downloads VALUES (?, ?, ?, 1)", (user_id, artist, song))

    conn.commit()

def get_cached_filename(artist: str, song: str) -> str | None:
    cur.execute("SELECT filename FROM cached_tracks WHERE artist=? AND song=?", (artist, song))
    row = cur.fetchone()
    return row[0] if row else None

def cache_track(artist: str, song: str, filename: str):
    cur.execute("INSERT OR REPLACE INTO cached_tracks VALUES (?, ?, ?)", (artist, song, filename))
    conn.commit()

main_menu = ReplyKeyboardMarkup(
    [
        [KeyboardButton("Скачать трек")],
        [KeyboardButton("Топ песен"), KeyboardButton("Топ исполнителей")],
        [KeyboardButton("Мой профиль")]
    ],
    resize_keyboard=True
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Кидай ссылку SoundCloud / VK или просто напиши название трека для поиска 👇",
        reply_markup=main_menu
    )

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cur.execute("SELECT SUM(count) FROM user_downloads WHERE user_id = ?", (user_id,))
    total = cur.fetchone()[0] or 0

    cur.execute("SELECT COUNT(DISTINCT artist) FROM user_downloads WHERE user_id = ?", (user_id,))
    artists = cur.fetchone()[0] or 0

    cur.execute("SELECT artist, song, count FROM user_downloads WHERE user_id = ? ORDER BY count DESC LIMIT 1", (user_id,))
    fav = cur.fetchone()

    text = f"📊 Твоя стата:\n\nТреков: {total}\nАртистов: {artists}\n"
    if fav:
        text += f"Фаворит: {fav[0]} — {fav[1]} ({fav[2]} раз)"
    else:
        text += "Пока пусто — пришли ссылку!"
    await update.message.reply_text(text, reply_markup=main_menu)

async def top_songs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur.execute("SELECT artist, song, count FROM downloads ORDER BY count DESC LIMIT 10")
    rows = cur.fetchall()
    text = "🏆 Топ-10 треков:\n\n" if rows else "Пока пусто"
    for i, (a, s, c) in enumerate(rows, 1):
        text += f"{i}. {a} — {s} ({c})\n"
    await update.message.reply_text(text, reply_markup=main_menu)

async def top_artists(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur.execute("SELECT artist, SUM(count) as total FROM downloads GROUP BY artist ORDER BY total DESC LIMIT 10")
    rows = cur.fetchall()
    text = "🎤 Топ-10 артистов:\n\n" if rows else "Пока пусто"
    for i, (a, t) in enumerate(rows, 1):
        text += f"{i}. {a} ({t})\n"
    await update.message.reply_text(text, reply_markup=main_menu)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text:
        return

    txt_lower = text.lower()
    if txt_lower in ["скачать трек", "скачать", "скачай"]:
        await update.message.reply_text("Кидай ссылку на трек или просто название для поиска")
        return
    if txt_lower in ["мой профиль", "профиль", "стата", "статистика"]:
        await profile(update, context)
        return
    if txt_lower in ["топ песен", "топ треков"]:
        await top_songs(update, context)
        return
    if txt_lower in ["топ исполнителей", "топ артистов"]:
        await top_artists(update, context)
        return

    if re.match(r'^https?://', text):
        url = text
        if any(d in url.lower() for d in ['soundcloud.com', 'on.soundcloud.com', 'snd.sc', 'vk.com', 'vk.ru', 'vk.audio']):
            await download_track(update, url)
        else:
            await update.message.reply_text("Только SoundCloud и VK пока что")
        return

    # Поиск по названию
    await search_tracks(update, text)

async def search_tracks(update: Update, query: str):
    msg = await update.message.reply_text(f"Ищу «{query}»...")

    try:
        ydl_opts_search = {
            'extract_flat': True,
            'quiet': True,
            'playlistend': 6,
            'cookiefile': 'cookies.txt',
        }

        results = []
        with yt_dlp.YoutubeDL(ydl_opts_search) as ydl:
            try:
                info = ydl.extract_info(f"scsearch:{query}", download=False)
                results = info.get('entries', [])[:6]
            except:
                pass

            if not results:
                info = ydl.extract_info(f"ytsearch:{query}", download=False)
                results = info.get('entries', [])[:6]

        if not results:
            await msg.edit_text("Ничего не нашёл 😔\nПопробуй изменить запрос или пришли ссылку.")
            return

        text = f"Результаты по «{query}» ({len(results)}):\n\n"
        keyboard = []

        for entry in results:
            title = entry.get('title', 'Без названия')
            uploader = entry.get('uploader') or entry.get('artist') or "Неизвестно"
            url = entry.get('url') or entry.get('webpage_url') or entry.get('id')

            if not url.startswith('http'):
                if 'soundcloud' in str(entry):
                    url = f"https://soundcloud.com/{url}"
                else:
                    url = f"https://youtube.com/watch?v={url}"

            text += f"• {uploader} — {title}\n"
            keyboard.append([InlineKeyboardButton(
                f"Скачать {title[:25]}…",
                callback_data=f"dl_{url}"
            )])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await msg.edit_text(text, reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Ошибка поиска: {e}")
        await msg.edit_text("Ошибка при поиске. Попробуй позже.")

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data.startswith("dl_"):
        url = query.data.replace("dl_", "")
        await query.message.reply_text("Загружаю трек...")
        await download_track(query.message, url)

async def download_track(update: Update, url: str):
    user_id = update.effective_user.id
    msg = await update.message.reply_text("Загрузка…")

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': 'downloads/%(uploader|Unknown)s - %(title)s.%(ext)s',
        'writethumbnail': True,
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '128',
        }],
        'postprocessor_args': {
            'FFmpegExtractAudio': ['-threads', '0', '-preset', 'ultrafast', '-q:a', '0']
        },
        'noplaylist': True,
        'continuedl': True,
        'retries': 10,
        'fragment_retries': 5,
        'sleep_interval': 1,
        'max_sleep_interval': 5,
        'cookiefile': 'cookies.txt' if os.path.exists('cookies.txt') else None,  # куки опциональны
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'referer': 'https://vk.com/' if 'vk' in url.lower() else 'https://soundcloud.com/',
        'no_warnings': True,
        'quiet': True,
        # Обход для VK без куки (для публичных треков)
        'extractor_args': {'vk': {'skip': ['auth_check']}} if 'vk' in url.lower() else {},
    }

    try:
        os.makedirs("downloads", exist_ok=True)

        artist_temp = "Unknown"
        title_temp = "Track"
        try:
            with yt_dlp.YoutubeDL({'quiet': True}) as ydl_temp:
                info_temp = ydl_temp.extract_info(url, download=False)
                artist_temp = info_temp.get('uploader') or info_temp.get('artist') or "Unknown"
                title_temp = info_temp.get('title') or "Track"
        except:
            pass

        cached_file = get_cached_filename(artist_temp, title_temp)

        if cached_file and os.path.exists(cached_file):
            with open(cached_file, 'rb') as audio_file:
                await update.message.reply_audio(
                    audio=audio_file,
                    title=title_temp,
                    performer=artist_temp
                )
            log_download(user_id, artist_temp, title_temp)
            await msg.delete()
            return

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            real_url = url
            if 'on.soundcloud.com' in url or 'snd.sc' in url:
                try:
                    temp = ydl.extract_info(url, download=False, process=False)
                    real_url = temp.get('webpage_url') or temp.get('url') or url
                except:
                    pass

            info = ydl.extract_info(real_url, download=True)

            artist = info.get('uploader') or info.get('artist') or info.get('channel') or "Unknown"
            title = info.get('title') or "Track"
            filename = ydl.prepare_filename(info)
            if not filename.lower().endswith('.mp3'):
                filename = os.path.splitext(filename)[0] + '.mp3'

            thumbnail_path = os.path.splitext(filename)[0] + '.jpg'
            thumbnail = open(thumbnail_path, 'rb') if os.path.exists(thumbnail_path) else None

        if os.path.exists(filename):
            with open(filename, 'rb') as audio_file:
                await update.message.reply_audio(
                    audio=audio_file,
                    title=title.strip(),
                    performer=artist.strip(),
                    thumbnail=thumbnail
                )
            log_download(user_id, artist, title)
            cache_track(artist, title, filename)
            await msg.delete()

            if thumbnail:
                thumbnail.close()
                try:
                    os.remove(thumbnail_path)
                except:
                    pass
        else:
            await msg.edit_text("Не удалось скачать файл")

    except Exception as e:
        logger.error(f"Ошибка: {str(e)}", exc_info=True)
        await msg.edit_text("Ошибка при загрузке. Попробуй другую ссылку или обнови cookies.txt")

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("profile", profile))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(callback_handler))
    print("Бот запущен...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data.startswith("dl_"):
        url = query.data.replace("dl_", "")
        await query.message.reply_text("Загружаю трек...")
        await download_track(query.message, url)

if __name__ == "__main__":
    main()
