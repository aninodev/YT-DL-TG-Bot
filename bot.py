#!/usr/bin/env python
# pylint: disable=unused-argument

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import yt_dlp as youtube_dl
import logging
import io
import requests
import os
import atexit
import asyncio
import sqlite3
import argparse
import pathlib

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


parser = argparse.ArgumentParser("YT-DL-TG-Bot")
parser.add_argument(
    "--config", default="config.toml", type=str, help="Config file to use (TOML-format)"
)
parser.add_argument(
    "--token",
    help="Log out from official Telegram servers to enable reliable local Telegram Bot API server usage.",
)
parser.add_argument(
    "--db-type",
    help="DB type to use to store records of which videos were sent to which chats already to prevent reposting.",
    choices=["sqlite3"],
    default=None,
)
parser.add_argument(
    "--db-path",
    help="Path of DB to use to store records of which videos were sent to which chats already to prevent reposting.",
    type=str,
    default=None,
)
parser.add_argument(
    "--output-template",
    help="Output filepath template for the temporary files downloaded before sending to the chat.",
    type=str,
    default=None,
)
parser.add_argument(
    "--admin-ids",
    help="Admin IDs to use for managing the bot via Telegram itself.",
    type=int,
    nargs="+",
    default=None,
)

args = parser.parse_args()

arg_bindings = {
    "token": {
        "type": str,
        "tree": ["TelegramBot", "token"],
    },
    "config": {
        "type": pathlib.Path,
        "tree": None,
    },
    "db_type": {
        "type": str,
        "tree": ["Database", "type"],
    },
    "db_path": {
        "type": str,
        "tree": ["Database", "path"],
    },
    "output_template": {
        "type": str,
        "tree": ["YoutubeDL", "output_template"],
    },
    "admin_ids": {
        "type": list[str | int],
        "tree": ["Admin", "admin_ids"],
    },
}

with open(args.config, "rb") as fp:
    config = tomllib.load(fp)

def get_nested(data: dict, args: list[str]):
    """Get nested values from dict by nesting "path". Returns None if element does not exist."""
    if args and data:
        element = args[0]
        if element:
            value = data.get(element)
            return value if len(args) == 1 else get_nested(value, args[1:])


def get_settings():
    """Load all settings from arg_bindings from the CLI args or config file as a fallback."""
    settings = {}
    for arg_name, binding in arg_bindings.items():
        value = getattr(args, arg_name, None)
        if value is None:
            value = get_nested(config, binding["tree"])
        elif not value:
            if binding["any_true"]:
                value = get_nested(config, binding["tree"])
        else:
            try:
                value = binding["type"](value)
            except TypeError:
                value = get_nested(config, binding["tree"])
            else:
                settings[arg_name] = value
                continue
        if value is None:
            try:
                if binding["optional"]:
                    continue
            except KeyError:
                pass
            try:
                # "any_true" must have either true or false, so if the value is not Truthy at this point, consider it False
                if binding["any_true"]:
                    settings[arg_name] = False
                    continue
            except KeyError:
                pass
            raise TypeError("Param {} is missing.".format(arg_name))
        try:
            value = binding["type"](value)
        except TypeError:
            if binding["optional"]:
                continue
            else:
                raise TypeError("Param {} is of incorrect type.".format(arg_name))
        settings[arg_name] = value
    return settings


settings = get_settings()


def get_setting(param: str):
    """Get a single setting parameter from args/config."""
    try:
        setting = settings[param]
    except KeyError:
        raise KeyError("Parameter with CLI name {} not found.".format(param))
    return setting


# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
# Set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

db_type = get_setting("db_type")
if db_type.startswith("sqlite"):
    db_path = get_setting("db_path")

    def get_con():
        return sqlite3.connect(db_path, timeout=5)

    logger.info("Using SQLITE3 as DB engine")
    USE_DB = True
else:
    logger.error("DB type '{}' is invalid".format(db_type))
    USE_DB = False
if USE_DB:
    with get_con() as db_con:
        # Create table and unique index if not already present in the DB
        db_con.execute(
            "CREATE TABLE IF NOT EXISTS uploads (chat_platform TEXT, chat_id TEXT, video_platform TEXT, video_id TEXT);"
        )
        db_con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS unique_uploads ON uploads (chat_platform, chat_id, video_platform, video_id);"
        )


def remove_file(file_path: str):
    """Remove a file from the filesystem. Required for atexit to delete the files automatically at application exit."""
    try:
        os.remove(os.path.normpath(file_path))
    except KeyboardInterrupt:
        raise
    except FileNotFoundError:
        pass


# Define a few command handlers. These usually take the two arguments update and context.


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    await update.message.reply_html(
        rf"Hi {user.mention_html()}! Type /help for available commands.",
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    await update.message.reply_markdown_v2(
        "Available commands:\n"
        "/start \- Display hello message\n"
        "/help \- Display this help message\n"
        "/songs PLAYLIST \- Download songs from playlist and upload them into the current chat"
    )


async def playlist_songs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send songs in chat from playlist URL."""
    message = update.effective_message
    try:
        playlist_url = message.text.split()[1].strip()
    except IndexError:
        await message.reply_text("Please give a playlist URL")
        return
    ytdl_output_template = get_setting("output_template")
    ytdl_options = {
        "format": "bestaudio",
        # Directly download Opus audio streams from YouTube
        "audioformat": "opus",
        # Remove the conversion postprocessor
        "postprocessors": [],
        # Output Path Template from config file
        "outtmpl": ytdl_output_template,
        # Continue processing even if a video is unavailable or we run into another error
        "ignoreerrors": True,
        "extract_flat": True,
        "skip_download": True,
        # Log yt-dlp output to the same logger as PlaylistBot
        "logger": logger,
    }
    ytdl_download_options = {
        **ytdl_options,
        "extract_flat": False,
        "skip_download": False,
    }
    logger.info(
        "Fetching playlist {} for user {} in chat with {}".format(
            playlist_url, message.from_user, message.chat
        )
    )
    bot_message = await message.reply_text("Fetching playlist...")
    try:
        with youtube_dl.YoutubeDL(ytdl_options) as ytdl:
            # Retrieve playlist information without downloading
            info = ytdl.extract_info(playlist_url, download=False)
            # playlist_title = info["title"]
            # extractor = info['extractor'] # Use this if using the actual extractor name, which we are not currently using because of possible extractor names that are invalid for use in paths
            extractor = "youtube"
            extractor_key = info["extractor_key"]
            retry_message = None
            try:
                entries = info["entries"]
            except KeyError:
                with youtube_dl.YoutubeDL(ytdl_download_options) as ytdl_download:
                    logger.info(
                        "URL {} had no entries data. Refetching with Downloading enabled...".format(
                            info["url"]
                        )
                    )
                    retry_message = await message.reply_text(
                        "Ran into an issue while fetching playlist entries. Retrying using our alternate method. This may take a while..."
                    )
                    playlist_dl = ytdl_download.extract_info(info["url"])
                    logger.info(
                        "Extracted Downloaded Playlist info: {}".format(playlist_dl)
                    )
                    try:
                        entries = playlist_dl["entries"]
                    except KeyError:
                        logger.info(
                            "Second fetch extract method (download-enabled) failed as well. There was no entries data in the result. Aborting playlist at URL {} / {}.".format(
                                info["url"], playlist_url
                            )
                        )
                        retry_message = await retry_message.edit_text(
                            "Both methods to fetch playlist entries failed. Please try a different playlist URL instead."
                        )
                    else:
                        retry_message = await retry_message.edit_text(
                            "Initial fetching took longer because we had to use our alternate method. This may be because of a URL issue. Next time, please try to send just the playlist's page URL, rather than the URL of a video playing in the playlist. Now we'll download and send the songs, please wait..."
                        )
            await bot_message.edit_text(
                "Playlist fetched. Found {} videos. Downloading videos...".format(
                    len(entries)
                )
            )
            for entry in entries:
                if entry:
                    try:
                        if not entry["original_url"]:
                            logger.info(
                                "Entry {} has a blank original_url param. Skipping...".format(
                                    entry
                                )
                            )
                            continue
                    except KeyError:
                        try:
                            if not entry["url"]:
                                logger.info(
                                    "Entry {} does not have a original_url param and url param is blank. Skipping...".format(
                                        entry
                                    )
                                )
                                continue
                        except KeyError:
                            logger.info(
                                "Entry {} does not have an original_url or url param. Skipping...".format(
                                    entry
                                )
                            )
                            continue
                        else:
                            vid_url = entry["url"]
                    else:
                        vid_url = entry["original_url"]
                    vid_id = entry["id"]
                    if USE_DB:
                        db_params = (
                            "telegram",
                            str(message.chat_id),
                            extractor.strip(),
                            str(vid_id).strip(),
                        )
                        with get_con() as db_con:
                            if (
                                len(
                                    db_con.execute(
                                        "SELECT * from uploads where chat_platform=? AND chat_id=? AND video_platform=? AND video_id=?;",
                                        db_params,
                                    ).fetchall()
                                )
                                > 0
                            ):
                                logger.info(
                                    "YouTube video with ID {} already exists in the Telegram chat with ID {}. Skipping...".format(
                                        vid_id, message.chat_id
                                    )
                                )
                                await message.reply_text(
                                    "YouTube video with ID {} was already uploaded to this chat.".format(
                                        vid_id
                                    )
                                )
                                continue
                            else:
                                logger.info(
                                    "YouTube video with ID {} does not yet exist in the Telegram chat with ID {} (DB Params: {}). We will download it.".format(
                                        vid_id, message.chat_id, db_params
                                    )
                                )
                    try:
                        # channel_title = entry["channel"]
                        video_title = entry["title"]
                    except KeyError as e:
                        logger.warn("Entry {} missing a param: {}".format(entry, e))
                        continue
                    try:
                        # Insecure but it's much easier, shorter, and less prone to failure from keys not being present if using custom paths versus doing them manually without dict expansion.
                        temp_path = ytdl_output_template % {
                            **entry,
                            "extractor": extractor,
                            "extractor_key": extractor_key,
                        }
                        temp_path = os.path.abspath(os.path.normpath(temp_path))
                    except KeyError as e:
                        logger.warning(
                            "Did not get temp filepath for entry {} because of a missing param: {}".format(
                                entry, e
                            )
                        )
                        continue
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        logger.warning(
                            "Ran into an exception while trying to get local temp filepath for video with URL {}: {}".format(
                                vid_url, e
                            )
                        )
                        continue
                    else:
                        atexit.register(remove_file, temp_path)
                    with youtube_dl.YoutubeDL(ytdl_download_options) as ytdl_download:
                        logger.info('Downloading Opus audio stream from "%s"', vid_url)
                        entry_dl = ytdl_download.extract_info(vid_url)
                        dl_res = ytdl_download.download(
                            [
                                vid_url,
                            ]
                        )
                        logger.info(
                            "Got result {} from downloading from video URL.".format(
                                dl_res
                            )
                        )
                    try:
                        thumbnail_url = entry_dl["thumbnail"]
                        if thumbnail_url:
                            thumbnail_file = io.BytesIO(
                                requests.get(thumbnail_url).content
                            )
                            logger.info(
                                "Successfully fetched thumbnail for YouTube video with ID {}".format(
                                    vid_id
                                )
                            )
                        else:
                            thumbnail_file = None
                    except KeyError as e:
                        logger.warning(
                            "Thumbnail for video with URL {} could not be fetched because of an Exception: {}".format(
                                vid_url, e
                            )
                        )
                        thumbnail_file = None
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        logger.warning(
                            "Thumbnail for video with URL {} could not be fetched because of an Exception: {}".format(
                                vid_url, e
                            )
                        )
                        thumbnail_file = None
                    logger.info("Posting audio stream to Telegram")
                    filename = bytes(
                        os.path.basename(temp_path), encoding="latin1", errors="ignore"
                    ).decode("latin1", "ignore")
                    logger.info(
                        "Sending audio with audio path {}, title {}, and filename {}.".format(
                            temp_path, video_title, filename
                        )
                    )
                    try:
                        with open(temp_path, "rb") as audio_file:
                            audio_message = await context.bot.send_audio(
                                chat_id=message.chat_id,
                                audio=audio_file,
                                duration=int(entry["duration"]),
                                title=video_title,
                                thumbnail=thumbnail_file,
                                disable_notification=True,
                                caption=vid_url[:1000],
                            )
                        if audio_message:
                            logger.info("AUDIO_MESSAGE TRUE")
                    except FileNotFoundError:
                        logger.warning(
                            "YouTube video with ID {} could not be found on disk. It likely did not download successfully.".format(
                                vid_id
                            )
                        )
                    else:
                        if USE_DB:
                            with get_con() as db_con:
                                db_con.execute(
                                    "INSERT OR IGNORE INTO uploads (chat_platform, chat_id, video_platform, video_id) VALUES (?, ?, ?, ?);",
                                    db_params,
                                )
                                logger.info(
                                    "Added YouTube video with ID {} in Telegram chat ID {} (DB Params: {}) to DB".format(
                                        vid_id, message.chat_id, db_params
                                    )
                                )
                    try:
                        os.remove(temp_path)
                    except KeyboardInterrupt:
                        raise
                    except FileNotFoundError:
                        pass
                else:
                    # Value of entry evaluated Falsy. Skip it
                    continue
    except youtube_dl.utils.DownloadError:
        await bot_message.edit_text("Failed to download playlist.")
    except KeyboardInterrupt:
        raise
    except Exception as e:
        await message.reply_text("Ran into an error. Please try again later.")
        logger.exception(
            "Ran into an error while downloading/sending music file(s) for playlist URL {} with info {}: {}".format(
                playlist_url, info, e
            )
        )
    logger.info(
        "Finished Fetching playlist {} for user {} in chat {}".format(
            playlist_url, message.from_user, message.chat
        )
    )
    await message.reply_text("Finished.")
    await asyncio.sleep(5)
    await bot_message.delete()


async def dump_db(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    sender = update.effective_sender
    admin_ids = get_setting("admin_ids")
    if str(sender.id) not in admin_ids:
        message.reply_text("Sorry, you do not have permission to use this command.")
    else:
        with get_con() as db_con:
            db_cur = db_con.cursor()
            db_results = db_cur.execute("SELECT * FROM uploads;").fetchall()
        logger.info(
            "The following data is from the uploads DB and was requested by {}:\n{}".format(
                sender, db_results
            )
        )
        message.reply_text("Done. Look at the output file to see the data.")


def main() -> None:
    """Start the bot."""
    logger.debug("Settings: {}".format(settings))
    token = get_setting("token")
    # Create the Application and pass it your bot's token.
    application = (
        Application.builder()
        .token(token)
        .read_timeout(30)
        .write_timeout(30)
        .connect_timeout(30)
        .pool_timeout(5)
        .build()
    )

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("songs", playlist_songs))
    application.add_handler(CommandHandler("dump_db", dump_db))

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
