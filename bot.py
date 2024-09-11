#!/usr/bin/env python
# pylint: disable=unused-argument

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import yt_dlp as youtube_dl
import logging
import io
import requests
import os
import sys
import atexit
import asyncio
import sqlite3
import argparse
import pathlib
from urllib.parse import urlparse

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

CURRENTLY_SUPPORTED_DB_SCHEMA_VERSION = 1

parser = argparse.ArgumentParser("YT-DL-TG-Bot")
parser.add_argument(
    "--config", default="config.toml", type=str, help="Config file to use (TOML-format)"
)
parser.add_argument(
    "--token",
    help="Log out from official Telegram servers to enable reliable local Telegram Bot API server usage.",
)
parser.add_argument(
    "--logout",
    action="store_true",
    help="Log out from official Telegram servers to enable reliable local Telegram Bot API server usage.",
)
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Put the bot into local mode to use a local Telegram Bot API server.",
)
parser.add_argument("--base-url", help="Base URL of custom Telegram Bot API server.")
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
    "--cookies-file",
    help="Path of files containing netscape format cookies for yt-dlp.",
)
parser.add_argument(
    "--song-output-template",
    help="Output filepath template for the temporary song files downloaded before sending to the chat.",
    type=str,
    default=None,
)
parser.add_argument(
    "--video-output-template",
    help="Output filepath template for the temporary video files downloaded before sending to the chat.",
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
parser.add_argument(
    "--song-flag-repost-text",
    help="Repost Flag trigger arg text for /song command.",
    type=str,
    default=None,
)
parser.add_argument(
    "--songs-flag-repost-text",
    help="Repost Flag trigger arg text for /songs command.",
    type=str,
    default=None,
)
parser.add_argument(
    "--video-flag-repost-text",
    help="Repost Flag trigger arg text for /video command.",
    type=str,
    default=None,
)
parser.add_argument(
    "--videos-flag-repost-text",
    help="Repost Flag trigger arg text for /videos command.",
    type=str,
    default=None,
)

args = parser.parse_args()

arg_bindings = {
    "token": {
        "type": str,
        "tree": ["TelegramBot", "token"],
    },
    "logout": {
        "type": bool,
        "tree": ["TelegramBot", "logout"],
        "any_true": True,
    },
    "local_mode": {
        "type": bool,
        "tree": ["TelegramBot", "local_mode"],
        "any_true": True,
    },
    "base_url": {
        "type": str,
        "tree": ["TelegramBot", "base_url"],
        "optional": True,
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
    "cookies_file": {
        "type": str,
        "tree": ["YoutubeDL", "cookies_file_path"],
        "optional": True,
    },
    "song_output_template": {
        "type": str,
        "tree": ["YoutubeDL", "song_output_template"],
    },
    "video_output_template": {
        "type": str,
        "tree": ["YoutubeDL", "video_output_template"],
    },
    "admin_ids": {
        "type": list[str | int],
        "tree": ["Admin", "admin_ids"],
    },
    "song_flag_repost_text": {
        "type": str,
        "tree": ["Commands", "song", "flag", "repost", "text"],
    },
    "songs_flag_repost_text": {
        "type": str,
        "tree": ["Commands", "songs", "flag", "repost", "text"],
    },
    "video_flag_repost_text": {
        "type": str,
        "tree": ["Commands", "video", "flag", "repost", "text"],
    },
    "videos_flag_repost_text": {
        "type": str,
        "tree": ["Commands", "videos", "flag", "repost", "text"],
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
                    settings[arg_name] = None
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
                settings[arg_name] = None
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

    def does_db_table_exist(table_name: str):
        db_type = get_setting("db_type")
        # Handle SQLite3 table existance checking
        if db_type.startswith("sqlite"):
            with get_con() as db_con:
                table_exists = bool(
                    db_con.execute(
                        "SELECT EXISTS (SELECT * FROM sqlite_master WHERE type='table' AND name=?);",
                        (table_name,),
                    ).fetchone()[0]
                )
        logger.info(
            "DB table {} existance value is {}.".format(table_name, table_exists)
        )
        return table_exists

    uploads_tables_existed = does_db_table_exist("uploads")
    versions_table_existed = does_db_table_exist("versions")
    if not versions_table_existed:
        with get_con() as db_con:
            if not versions_table_existed:
                # Create table and unique index if not already present in the DB
                db_con.execute("CREATE TABLE versions (component TEXT, version INT);")
            db_con.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS unique_components ON versions (component);"
            )
            if not versions_table_existed:
                # Default to DB Schema 0 if the table/component does not exist so it is recreated from scratch
                db_con.execute(
                    "INSERT INTO versions (component, version) VALUES (?, ?);",
                    ("db_schema", 0),
                )
    with get_con() as db_con:
        # Get current DB Schema version
        db_schema = db_con.execute(
            "SELECT version FROM versions WHERE component=?;", ("db_schema",)
        ).fetchone()[0]
    if int(db_schema) == CURRENTLY_SUPPORTED_DB_SCHEMA_VERSION:
        clear_db = False
    elif int(db_schema) > CURRENTLY_SUPPORTED_DB_SCHEMA_VERSION:
        logger.critical(
            "This version of the program supports Database Schema {} at most, but your Database is currently using newer Schema of {}. Please upgrade this program to use your newer Database Schema".format(
                CURRENTLY_SUPPORTED_DB_SCHEMA_VERSION, db_schema
            )
        )
        sys.exit(
            "The database schema is too new for this version of the program. Please update this program or clear the database."
        )
    elif int(db_schema) == 0:
        if uploads_tables_existed:
            input_str = input(
                "Your Database Schema predates our schema versioning system. We cannot automatically update your tables. Would you like us to overwrite your old tables with ones using the current schema? Your existing uploads data will be lost! Your Choice (Y to clear DB): "
            )
            if input_str.lower().strip() in ("y", "yes", "t", "true", "1"):
                clear_db = True
            else:
                logger.warning(
                    "You chose to not overwrite your old database. Since we cannot use the existing database schema, we will be running without DB usage nor upload logging."
                )
                USE_DB = False

if USE_DB:
    if clear_db:
        db_con.execute("DROP TABLE uploads;")
    with get_con() as db_con:
        # Create table and unique index if not already present in the DB
        db_con.execute(
            "CREATE TABLE IF NOT EXISTS uploads (chat_platform TEXT, chat_id TEXT, message_id TEXT, video_platform TEXT, video_id TEXT, upload_type TEXT);"
        )
        db_con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS unique_uploads ON uploads (chat_platform, chat_id, message_id, video_platform, video_id, upload_type);"
        )
        db_con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS unique_messages ON uploads (chat_platform, chat_id, message_id);"
        )
        if clear_db:
            db_con.execute(
                "UPDATE versions SET version=? WHERE component=?;",
                (CURRENTLY_SUPPORTED_DB_SCHEMA_VERSION, "db_schema"),
            )


def uri_validator(x):
    try:
        result = urlparse(x)
        return all([result.scheme, result.netloc])
    except AttributeError:
        return False


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


async def help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /help is issued."""
    await update.message.reply_markdown_v2(
        "Available commands:\n"
        "/start \- Display hello message\n"
        "/help \- Display this help message\n"
        "/song VIDEO_URL \- Download song from video and upload it into the current chat\n"
        "/songs PLAYLIST_URL \- Download songs from video playlist and upload them into the current chat"
    )


async def song(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send song(s) in chat from video URL(s)."""
    message = update.effective_message
    user = update.effective_user
    repost = get_setting("song_flag_repost_text") in (
        a.strip().lower() for a in context.args
    )
    video_urls = set(
        a.strip()
        for a in context.args
        if a.strip().lower()
        not in [
            get_setting("song_flag_repost_text"),
        ]
    )
    valid_urls: list[str] = []
    invalid_urls: list[str] = []
    for video_url in video_urls:
        if uri_validator(video_url):
            valid_urls.append(video_url)
        else:
            invalid_urls.append(video_url)
    if valid_urls:
        if invalid_urls:
            logger.info(
                "Song request message {} by {} in {} contained valid URLS: {} and invalid URLs: {}.".format(
                    message, user, message.chat, valid_urls, invalid_urls
                )
            )
        else:
            logger.info(
                "Song request message {} by {} in {} contained valid URLS: {}.".format(
                    message, user, message.chat, valid_urls
                )
            )
    else:
        if invalid_urls:
            logger.info(
                "Song request message {} by {} in {} contained only invalid URLS: {}.".format(
                    message, user, message.chat, invalid_urls
                )
            )
            await message.reply_text(
                "The following are not valid URLs: {}. Please try again.".format(
                    invalid_urls
                ),
                disable_web_page_preview=True,
            )
            return
        else:
            logger.info(
                "Song request message {} by {} in {} contained no playlist URLS.".format(
                    message, user, message.chat
                )
            )
            await message.reply_text(
                "Please provide a YouTube video URL and try again."
            )
            return
    await send_songs_individual(update, context, valid_urls, repost)


async def send_songs_individual(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    video_urls: list[str],
    repost: bool,
) -> None:
    """Send songs in chat from playlist URL(s)."""
    await update.message.reply_text(
        "Fetching songs from {} videos: {}".format(len(video_urls), video_urls),
        disable_web_page_preview=True,
    )
    for video_url in video_urls:
        await send_song_individual(update, context, video_url, repost)


async def send_song_individual(
    update: Update, context: ContextTypes.DEFAULT_TYPE, video_url: str, repost: bool
) -> None:
    message = update.effective_message
    user = update.effective_user
    try:
        local_mode = get_setting("local_mode")
        ytdl_output_template = get_setting("song_output_template")
        ytdl_options = {
            "format": "bestaudio",
            # Directly download Opus audio streams from YouTube
            "audioformat": "opus",
            # Remove the conversion postprocessor
            "postprocessors": [],
            # Use custom cookies if specified
            "cookiefile": get_setting("cookies_file"),
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
            "Fetching video {} for user {} in chat with {}".format(
                video_url, user, message.chat
            )
        )
        bot_message = await message.reply_text("Fetching video...")
        try:
            with youtube_dl.YoutubeDL(ytdl_options) as ytdl:
                # Retrieve playlist information without downloading
                info = ytdl.extract_info(video_url, download=False)
                # playlist_title = info["title"]
                # extractor = info['extractor'] # Use this if using the actual extractor name, which we are not currently using because of possible extractor names that are invalid for use in paths
                extractor = "youtube"
                extractor_key = info["extractor_key"]
                successful_new_audio_uploads = 0
                successful_audio_reposts = 0
                skipped_audio_uploads = 0
                failed_downloads = 0

                entry = info
                skip_video = False
                reposting = False
                successful_audio_upload = False
                if entry:
                    try:
                        if not entry["original_url"]:
                            logger.info(
                                "Entry {} has a blank original_url param. Skipping...".format(
                                    entry
                                )
                            )
                            raise Exception(
                                "Skipping because of blank original_url param"
                            )
                    except KeyError:
                        try:
                            if not entry["url"]:
                                logger.info(
                                    "Entry {} does not have a original_url param and url param is blank. Skipping...".format(
                                        entry
                                    )
                                )
                                raise Exception(
                                    "Skipping because of no original_url param and url param is blank"
                                )
                        except KeyError:
                            logger.info(
                                "Entry {} does not have an original_url or url param. Skipping...".format(
                                    entry
                                )
                            )
                            raise Exception(
                                "Skipping because of no original_url or url params"
                            )
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
                            "video",
                        )
                        with get_con() as db_con:
                            if (
                                len(
                                    db_con.execute(
                                        "SELECT * from uploads where chat_platform=? AND chat_id=? AND video_platform=? AND video_id=? AND upload_type=?;",
                                        db_params,
                                    ).fetchall()
                                )
                                > 0
                            ):
                                if repost:
                                    logger.info(
                                        "YouTube video with ID {} already exists in the Telegram chat with ID {}, but {} was specified. Reposting audio...".format(
                                            vid_id,
                                            message.chat_id,
                                            get_setting("songs_flag_repost_text"),
                                        )
                                    )
                                    await message.reply_text(
                                        "YouTube video with ID {} was already uploaded to this chat, but {} was specified so we will repost it here.".format(
                                            vid_id,
                                            get_setting("songs_flag_repost_text"),
                                        )
                                    )
                                    reposting = True
                                else:
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
                                    skipped_audio_uploads += 1
                                    skip_video = True
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
                        raise Exception("Video is missing a required param")
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
                        raise Exception(
                            "Coult not get temp file path because of missing required param"
                        )
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        logger.warning(
                            "Ran into an exception while trying to get local temp filepath for video with URL {}: {}".format(
                                vid_url, e
                            )
                        )
                        raise Exception(
                            "Exception occured while trying to get local temp file path"
                        )
                    else:
                        if skip_video:
                            remove_file(temp_path)
                            return
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
                            ),
                        )
                        if dl_res != 0:
                            logger.warning(
                                "Download result is not zero; there was likely an error with the download; skipping..."
                            )
                            raise Exception(
                                "Download result not zero; download likely failed"
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
                        filesize = os.path.getsize(temp_path)
                    except FileNotFoundError:
                        failed_downloads += 1
                        logger.warning(
                            "Video with ID {} and file_path {} was not found on filesystem. Skipping upload.".format(
                                vid_id, temp_path
                            )
                        )
                        raise Exception(
                            "Skipping video because file was not found on filesystem"
                        )
                    logger.info(
                        "File {} is {:,} bytes large.".format(temp_path, filesize)
                    )
                    try:
                        if local_mode:
                            audio_message = await context.bot.send_audio(
                                chat_id=message.chat_id,
                                audio="file://{}".format(temp_path),
                                duration=int(entry["duration"]),
                                title=video_title,
                                thumbnail=thumbnail_file,
                                disable_notification=True,
                                caption=vid_url[:1000],
                            )
                        else:
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
                            successful_audio_upload = True
                            if reposting:
                                successful_audio_reposts += 1
                            else:
                                successful_new_audio_uploads += 1
                        remove_file(temp_path)
                    except FileNotFoundError:
                        logger.warning(
                            "YouTube video with ID {} could not be found on disk. It likely did not download successfully.".format(
                                vid_id
                            )
                        )
                    else:
                        if USE_DB:
                            if successful_audio_upload:
                                db_params = (
                                    "telegram",
                                    str(message.chat_id),
                                    str(audio_message.message_id),
                                    extractor.strip(),
                                    str(vid_id).strip(),
                                    "song",
                                )
                                with get_con() as db_con:
                                    db_con.execute(
                                        "INSERT OR IGNORE INTO uploads (chat_platform, chat_id, message_id, video_platform, video_id, upload_type) VALUES (?, ?, ?, ?, ?, ?);",
                                        db_params,
                                    )
                                    logger.info(
                                        "Added YouTube video with ID {} in Telegram chat ID {} (DB Params: {}) to DB".format(
                                            vid_id, message.chat_id, db_params
                                        )
                                    )
                else:
                    # Value of entry evaluated Falsy. Skip it
                    raise Exception("Video info evaluated to falsy")
        except youtube_dl.utils.DownloadError:
            await bot_message.edit_text(
                "Failed to download video with URL {}.".format(video_url),
                disable_web_page_preview=True,
            )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            # await message.reply_text("Ran into an error. Please try again later.")
            exc_msg = "Ran into an error while downloading/sending music file for URL {} with info {}: {}".format(
                video_url, len(info), e
            )
            logger.exception(exc_msg)
            raise Exception(exc_msg)
        if repost:
            logger.info(
                "Finished Fetching video {} for user {} in chat {}.".format(
                    video_url,
                    user,
                    message.chat,
                )
            )
            await message.reply_text(
                "Finished. Successfully re-uploaded previously sent video with ID {}.".format(
                    vid_id
                )
            )
        else:
            logger.info(
                "Finished Fetching video {} for user {} in chat {}.".format(
                    video_url,
                    user,
                    message.chat,
                )
            )
            await message.reply_text(
                "Finished. Successfully uploaded video with ID {}.".format(
                    vid_id,
                )
            )
        await asyncio.sleep(5)
        await bot_message.delete()
    except KeyboardInterrupt:
        raise
    except Exception:
        message.reply_text(
            "Failed to send song with URL {}.".format(video_url),
            disable_web_page_preview=True,
        )
        return False


async def songs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send songs in chat from playlist URL(s)."""
    message = update.effective_message
    user = update.effective_user
    repost = get_setting("songs_flag_repost_text") in (
        a.strip().lower() for a in context.args
    )
    playlist_urls = set(
        a.strip()
        for a in context.args
        if a.strip().lower()
        not in [
            get_setting("songs_flag_repost_text"),
        ]
    )
    valid_urls: list[str] = []
    invalid_urls: list[str] = []
    for playlist_url in playlist_urls:
        if uri_validator(playlist_url):
            valid_urls.append(playlist_url)
        else:
            invalid_urls.append(playlist_url)
    if valid_urls:
        if invalid_urls:
            logger.info(
                "Song Playlist request message {} by {} in {} contained valid URLS: {} and invalid URLs: {}.".format(
                    message, user, message.chat, valid_urls, invalid_urls
                )
            )
        else:
            logger.info(
                "Song Playlist request message {} by {} in {} contained valid URLS: {}.".format(
                    message, user, message.chat, valid_urls
                )
            )
    else:
        if invalid_urls:
            logger.info(
                "Song Playlist request message {} by {} in {} contained only invalid URLS: {}.".format(
                    message, user, message.chat, invalid_urls
                )
            )
            await message.reply_text(
                "The following are not valid URLs: {}. Please try again.".format(
                    invalid_urls
                ),
                disable_web_page_preview=True,
            )
            return
        else:
            logger.info(
                "Song Playlist request message {} by {} in {} contained no playlist URLS.".format(
                    message, user, message.chat
                )
            )
            await message.reply_text("Please provide a playlist URL and try again.")
            return
    await send_songs_playlists(update, context, valid_urls, repost)


async def send_songs_playlists(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    playlist_urls: list[str],
    repost: bool,
) -> None:
    """Send songs in chat from playlist URL(s)."""
    await update.message.reply_text(
        "Fetching songs from {} video playlists: {}".format(
            len(playlist_urls), playlist_urls
        ),
        disable_web_page_preview=True,
    )
    for playlist_url in playlist_urls:
        await send_songs_playlist(update, context, playlist_url, repost)


async def send_songs_playlist(
    update: Update, context: ContextTypes.DEFAULT_TYPE, playlist_url: str, repost: bool
) -> None:
    message = update.effective_message
    user = update.effective_user
    try:
        local_mode = get_setting("local_mode")
        ytdl_output_template = get_setting("song_output_template")
        ytdl_options = {
            "format": "bestaudio",
            # Directly download Opus audio streams from YouTube
            "audioformat": "opus",
            # Remove the conversion postprocessor
            "postprocessors": [],
            # Use custom cookies if specified
            "cookiefile": get_setting("cookies_file"),
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
                playlist_url, user, message.chat
            )
        )
        bot_message = await message.reply_text(
            "Fetching playlist with URL {}...".format(playlist_url),
            disable_web_page_preview=True,
        )
        try:
            with youtube_dl.YoutubeDL(ytdl_options) as ytdl:
                # Retrieve playlist information without downloading
                info = ytdl.extract_info(playlist_url, download=False)
                # playlist_title = info["title"]
                # extractor = info['extractor'] # Use this if using the actual extractor name, which we are not currently using because of possible extractor names that are invalid for use in paths
                extractor = "youtube"
                extractor_key = info["extractor_key"]
                retry_message = None
                successful_new_audio_uploads = 0
                successful_audio_reposts = 0
                skipped_audio_uploads = 0
                failed_downloads = 0
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
                    skip_video = False
                    reposting = False
                    successful_audio_upload = False
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
                                "song",
                            )
                            with get_con() as db_con:
                                if (
                                    len(
                                        db_con.execute(
                                            "SELECT * from uploads where chat_platform=? AND chat_id=? AND video_platform=? AND video_id=? AND upload_type=?;",
                                            db_params,
                                        ).fetchall()
                                    )
                                    > 0
                                ):
                                    if repost:
                                        logger.info(
                                            "YouTube video with ID {} already exists in the Telegram chat with ID {}, but {} was specified. Reposting audio...".format(
                                                vid_id,
                                                message.chat_id,
                                                get_setting("songs_flag_repost_text"),
                                            )
                                        )
                                        await message.reply_text(
                                            "YouTube video with ID {} was already uploaded to this chat, but {} was specified so we will repost it here.".format(
                                                vid_id,
                                                get_setting("songs_flag_repost_text"),
                                            )
                                        )
                                        reposting = True
                                    else:
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
                                        skipped_audio_uploads += 1
                                        skip_video = True
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
                            if skip_video:
                                remove_file(temp_path)
                                continue
                            atexit.register(remove_file, temp_path)
                        with youtube_dl.YoutubeDL(
                            ytdl_download_options
                        ) as ytdl_download:
                            logger.info(
                                'Downloading Opus audio stream from "%s"', vid_url
                            )
                            entry_dl = ytdl_download.extract_info(vid_url)
                            dl_res = ytdl_download.download(
                                [
                                    vid_url,
                                ]
                            )
                            logger.info(
                                "Got result {} from downloading from video URL.".format(
                                    dl_res
                                ),
                            )
                            if dl_res != 0:
                                logger.warning(
                                    "Download result is not zero; there was likely an error with the download; skipping..."
                                )
                                failed_downloads += 1
                                continue
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
                            os.path.basename(temp_path),
                            encoding="latin1",
                            errors="ignore",
                        ).decode("latin1", "ignore")
                        logger.info(
                            "Sending audio with audio path {}, title {}, and filename {}.".format(
                                temp_path, video_title, filename
                            )
                        )
                        try:
                            filesize = os.path.getsize(temp_path)
                        except FileNotFoundError:
                            failed_downloads += 1
                            logger.warning(
                                "Video with ID {} and file_path {} was not found on filesystem. Skipping upload.".format(
                                    vid_id, temp_path
                                )
                            )
                            continue
                        logger.info(
                            "File {} is {:,} bytes large.".format(temp_path, filesize)
                        )
                        try:
                            if local_mode:
                                audio_message = await context.bot.send_audio(
                                    chat_id=message.chat_id,
                                    audio="file://{}".format(temp_path),
                                    duration=int(entry["duration"]),
                                    title=video_title,
                                    thumbnail=thumbnail_file,
                                    disable_notification=True,
                                    caption=vid_url[:1000],
                                )
                            else:
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
                                successful_audio_upload = True
                                if reposting:
                                    successful_audio_reposts += 1
                                else:
                                    successful_new_audio_uploads += 1
                            remove_file(temp_path)
                        except FileNotFoundError:
                            logger.warning(
                                "YouTube video with ID {} could not be found on disk. It likely did not download successfully.".format(
                                    vid_id
                                )
                            )
                        else:
                            if USE_DB:
                                if successful_audio_upload:
                                    db_params = (
                                        "telegram",
                                        str(message.chat_id),
                                        str(audio_message.message_id),
                                        extractor.strip(),
                                        str(vid_id).strip(),
                                        "song",
                                    )
                                    with get_con() as db_con:
                                        db_con.execute(
                                            "INSERT OR IGNORE INTO uploads (chat_platform, chat_id, message_id, video_platform, video_id, upload_type) VALUES (?, ?, ?, ?, ?, ?);",
                                            db_params,
                                        )
                                        logger.info(
                                            "Added YouTube video with ID {} in Telegram chat ID {} (DB Params: {}) to DB".format(
                                                vid_id, message.chat_id, db_params
                                            )
                                        )
                    else:
                        # Value of entry evaluated Falsy. Skip it
                        continue
        except youtube_dl.utils.DownloadError:
            await bot_message.edit_text(
                "Failed to download playlist with URL {}.".format(playlist_url),
                disable_web_page_preview=True,
            )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            # await message.reply_text("Ran into an error. Please try again later.")
            exc_msg = "Ran into an error while downloading/sending music file(s) for playlist URL {} with info {}: {}".format(
                playlist_url, len(info), e
            )
            logger.exception(exc_msg)
            raise Exception(exc_msg)
        if repost:
            logger.info(
                "Finished Fetching playlist {} for user {} in chat {}. Successfully uploaded {} previously unsent videos of the {} videos in the playlist. Reposted {} videos because {} was specified. {} of the videos failed to download.".format(
                    playlist_url,
                    user,
                    message.chat,
                    successful_new_audio_uploads,
                    len(entries),
                    successful_audio_reposts,
                    get_setting("songs_flag_repost_text"),
                    failed_downloads,
                )
            )
            await message.reply_text(
                "Finished. Successfully uploaded {} previously unsent videos of the {} videos in the playlist. Reposted {} videos because {} was specified. {} of the videos failed to download due to issues with YouTube (possibly restrictions, private/unavailable videos, etc.).".format(
                    successful_new_audio_uploads,
                    len(entries),
                    successful_audio_reposts,
                    get_setting("songs_flag_repost_text"),
                    failed_downloads,
                )
            )
        else:
            logger.info(
                "Finished Fetching playlist {} for user {} in chat {}. Successfully uploaded {} of {} videos in the playlist. Skipped {} because they were already sent to the chat. {} of the videos failed to download.".format(
                    playlist_url,
                    user,
                    message.chat,
                    successful_new_audio_uploads,
                    len(entries),
                    skipped_audio_uploads,
                    failed_downloads,
                )
            )
            await message.reply_text(
                "Finished. Successfully uploaded {} of {} videos in the playlist. Skipped {} videos because they were already sent to this chat. {} of the videos failed to download due to issues with YouTube (possibly restrictions, private/unavailable videos, etc.).".format(
                    successful_new_audio_uploads,
                    len(entries),
                    skipped_audio_uploads,
                    failed_downloads,
                )
            )
        await asyncio.sleep(5)
        await bot_message.delete()
    except KeyboardInterrupt:
        raise
    except Exception:
        message.reply_text(
            "Failed to send song playlist with URL {}.".format(playlist_url),
            disable_web_page_preview=True,
        )
        return False


async def video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send video in chat from video URL."""
    message = update.effective_message
    user = update.effective_user
    repost = get_setting("video_flag_repost_text") in (
        a.strip().lower() for a in context.args
    )
    video_urls = set(
        a.strip()
        for a in context.args
        if a.strip().lower()
        not in [
            get_setting("video_flag_repost_text"),
        ]
    )
    valid_urls: list[str] = []
    invalid_urls: list[str] = []
    for video_url in video_urls:
        if uri_validator(video_url):
            valid_urls.append(video_url)
        else:
            invalid_urls.append(video_url)
    if valid_urls:
        if invalid_urls:
            logger.info(
                "Video request message {} by {} in {} contained valid URLS: {} and invalid URLs: {}.".format(
                    message, user, message.chat, valid_urls, invalid_urls
                )
            )
        else:
            logger.info(
                "Video request message {} by {} in {} contained valid URLS: {}.".format(
                    message, user, message.chat, valid_urls
                )
            )
    else:
        if invalid_urls:
            logger.info(
                "Video request message {} by {} in {} contained only invalid URLS: {}.".format(
                    message, user, message.chat, invalid_urls
                )
            )
            await message.reply_text(
                "The following are not valid URLs: {}. Please try again.".format(
                    invalid_urls
                ),
                disable_web_page_preview=True,
            )
            return
        else:
            logger.info(
                "Video request message {} by {} in {} contained no video URLS.".format(
                    message, user, message.chat
                )
            )
            await message.reply_text(
                "Please provide a YouTube video URL and try again."
            )
            return
    await send_videos_individual(update, context, valid_urls, repost)


async def send_videos_individual(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    video_urls: list[str],
    repost: bool,
) -> None:
    """Send videos in chat from video URL(s)."""
    await update.message.reply_text(
        "Fetching videos from {} URLs:\n{}".format(
            len(video_urls), "\n".join(video_urls)
        ),
        disable_web_page_preview=True,
    )
    for video_url in video_urls:
        await send_video_individual(update, context, video_url, repost)


async def send_video_individual(
    update: Update, context: ContextTypes.DEFAULT_TYPE, video_url: str, repost: bool
) -> None:
    message = update.effective_message
    user = update.effective_user
    try:
        local_mode = get_setting("local_mode")
        ytdl_output_template = get_setting("video_output_template")
        ytdl_options = {
            "format": "(bv*[vcodec~='^((he|a)vc|h26[45])']+ba[ext=m4a])/b[ext=mp4]",
            "merge_output_format": "mp4",
            "postprocessors": [],
            # Use custom cookies if specified
            "cookiefile": get_setting("cookies_file"),
            "outtmpl": ytdl_output_template,
            "ignoreerrors": True,
            "extract_flat": True,
            "skip_download": True,
            "logger": logger,
        }
        ytdl_download_options = {
            **ytdl_options,
            "extract_flat": False,
            "skip_download": False,
        }
        logger.info(
            "Fetching video {} for user {} in chat with {}".format(
                video_url, user, message.chat
            )
        )
        bot_message = await message.reply_text("Fetching video...")
        try:
            with youtube_dl.YoutubeDL(ytdl_options) as ytdl:
                info = ytdl.extract_info(video_url, download=False)
                extractor = "youtube"
                extractor_key = info["extractor_key"]
                successful_new_video_uploads = 0
                successful_video_reposts = 0
                skipped_video_uploads = 0
                failed_downloads = 0

                entry = info
                skip_video = False
                reposting = False
                successful_video_upload = False
                if entry:
                    try:
                        if not entry["original_url"]:
                            logger.info(
                                "Entry {} has a blank original_url param. Skipping...".format(
                                    entry
                                )
                            )
                            raise Exception(
                                "Skipping because of blank original_url param"
                            )
                    except KeyError:
                        try:
                            if not entry["url"]:
                                logger.info(
                                    "Entry {} does not have a original_url param and url param is blank. Skipping...".format(
                                        entry
                                    )
                                )
                                raise Exception(
                                    "Skipping because of no original_url param and url param is blank"
                                )
                        except KeyError:
                            logger.info(
                                "Entry {} does not have an original_url or url param. Skipping...".format(
                                    entry
                                )
                            )
                            raise Exception(
                                "Skipping because of no original_url or url params"
                            )
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
                            "video",
                        )
                        with get_con() as db_con:
                            if (
                                len(
                                    db_con.execute(
                                        "SELECT * from uploads where chat_platform=? AND chat_id=? AND video_platform=? AND video_id=? AND upload_type=?;",
                                        db_params,
                                    ).fetchall()
                                )
                                > 0
                            ):
                                if repost:
                                    logger.info(
                                        "YouTube video with ID {} already exists in the Telegram chat with ID {}, but {} was specified. Reposting video...".format(
                                            vid_id,
                                            message.chat_id,
                                            get_setting("video_flag_repost_text"),
                                        )
                                    )
                                    await message.reply_text(
                                        "YouTube video with ID {} was already uploaded to this chat, but {} was specified so we will repost it here.".format(
                                            vid_id,
                                            get_setting("video_flag_repost_text"),
                                        )
                                    )
                                    reposting = True
                                else:
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
                                    skipped_video_uploads += 1
                                    skip_video = True
                            else:
                                logger.info(
                                    "YouTube video with ID {} does not yet exist in the Telegram chat with ID {} (DB Params: {}). We will download it.".format(
                                        vid_id, message.chat_id, db_params
                                    )
                                )
                    try:
                        video_title = entry["title"]
                    except KeyError as e:
                        logger.warn("Entry {} missing a param: {}".format(entry, e))
                        raise Exception("Video is missing a required param")
                    try:
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
                        raise Exception(
                            "Could not get temp file path because of missing required param"
                        )
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        logger.warning(
                            "Ran into an exception while trying to get local temp filepath for video with URL {}: {}".format(
                                vid_url, e
                            )
                        )
                        raise Exception(
                            "Exception occurred while trying to get local temp file path"
                        )
                    else:
                        if skip_video:
                            remove_file(temp_path)
                            return
                        atexit.register(remove_file, temp_path)
                    with youtube_dl.YoutubeDL(ytdl_download_options) as ytdl_download:
                        logger.info('Downloading video from "%s"', vid_url)
                        entry_dl = ytdl_download.extract_info(vid_url)
                        dl_res = ytdl_download.download(
                            [
                                vid_url,
                            ]
                        )
                        logger.info(
                            "Got result {} from downloading from video URL.".format(
                                dl_res
                            ),
                        )
                        if dl_res != 0:
                            logger.warning(
                                "Download result is not zero; there was likely an error with the download; skipping..."
                            )
                            raise Exception(
                                "Download result not zero; download likely failed"
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
                    logger.info("Posting video to Telegram")
                    filename = bytes(
                        os.path.basename(temp_path), encoding="latin1", errors="ignore"
                    ).decode("latin1", "ignore")
                    logger.info(
                        "Sending video with video path {}, title {}, and filename {}.".format(
                            temp_path, video_title, filename
                        )
                    )
                    try:
                        filesize = os.path.getsize(temp_path)
                    except FileNotFoundError:
                        failed_downloads += 1
                        logger.warning(
                            "Video with ID {} and file_path {} was not found on filesystem. Skipping upload.".format(
                                vid_id, temp_path
                            )
                        )
                        raise Exception(
                            "Skipping video because file was not found on filesystem"
                        )
                    logger.info(
                        "File {} is {:,} bytes large.".format(temp_path, filesize)
                    )
                    caption = "{title}\n({url})".format(
                        title=video_title, url=vid_url[: 1020 - 3 - len(video_title)]
                    )
                    try:
                        if local_mode:
                            video_message = await context.bot.send_video(
                                chat_id=message.chat_id,
                                video="file://{}".format(temp_path),
                                duration=int(entry["duration"]),
                                thumbnail=thumbnail_file,
                                disable_notification=True,
                                caption=caption,
                            )
                        else:
                            with open(temp_path, "rb") as video_file:
                                video_message = await context.bot.send_video(
                                    chat_id=message.chat_id,
                                    video=video_file,
                                    duration=int(entry["duration"]),
                                    thumbnail=thumbnail_file,
                                    disable_notification=True,
                                    caption=caption,
                                )
                        if video_message:
                            logger.info("VIDEO_MESSAGE TRUE")
                            successful_video_upload = True
                            if reposting:
                                successful_video_reposts += 1
                            else:
                                successful_new_video_uploads += 1
                        remove_file(temp_path)
                    except FileNotFoundError:
                        logger.warning(
                            "YouTube video with ID {} could not be found on disk. It likely did not download successfully.".format(
                                vid_id
                            )
                        )
                    else:
                        if USE_DB:
                            if successful_video_upload:
                                db_params = (
                                    "telegram",
                                    str(message.chat_id),
                                    str(video_message.message_id),
                                    extractor.strip(),
                                    str(vid_id).strip(),
                                    "video",
                                )
                                with get_con() as db_con:
                                    db_con.execute(
                                        "INSERT OR IGNORE INTO uploads (chat_platform, chat_id, message_id, video_platform, video_id, upload_type) VALUES (?, ?, ?, ?, ?, ?);",
                                        db_params,
                                    )
                                    logger.info(
                                        "Added YouTube video with ID {} in Telegram chat ID {} (DB Params: {}) to DB".format(
                                            vid_id, message.chat_id, db_params
                                        )
                                    )
                else:
                    # Value of entry evaluated Falsy. Skip it
                    raise Exception("Video info evaluated to falsy")
        except youtube_dl.utils.DownloadError:
            await bot_message.edit_text(
                "Failed to download video with URL {}.".format(video_url)
            )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            # await message.reply_text("Ran into an error. Please try again later.")
            exc_msg = "Ran into an error while downloading/sending video file for video URL {} with info {}: {}".format(
                video_url, len(info), e
            )
            logger.exception(exc_msg)
            raise Exception(exc_msg)
        if repost:
            logger.info(
                "Finished Fetching video {} for user {} in chat {}.".format(
                    video_url,
                    user,
                    message.chat,
                )
            )
            await message.reply_text(
                "Finished. Successfully re-uploaded previously sent video with ID {}.".format(
                    vid_id
                )
            )
        else:
            logger.info(
                "Finished Fetching video {} for user {} in chat {}.".format(
                    video_url,
                    user,
                    message.chat,
                )
            )
            await message.reply_text(
                "Finished. Successfully uploaded video with ID {}.".format(
                    vid_id,
                )
            )
        await asyncio.sleep(5)
        await bot_message.delete()
    except KeyboardInterrupt:
        raise
    except Exception:
        message.reply_text(
            "Failed to send video with URL {}.".format(video_url),
            disable_web_page_preview=True,
        )
        return False


async def videos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send songs in chat from playlist URL(s)."""
    message = update.effective_message
    user = update.effective_user
    repost = get_setting("videos_flag_repost_text") in (
        a.strip().lower() for a in context.args
    )
    playlist_urls = set(
        a.strip()
        for a in context.args
        if a.strip().lower()
        not in [
            get_setting("videos_flag_repost_text"),
        ]
    )
    valid_urls: list[str] = []
    invalid_urls: list[str] = []
    for playlist_url in playlist_urls:
        if uri_validator(playlist_url):
            valid_urls.append(playlist_url)
        else:
            invalid_urls.append(playlist_url)
    if valid_urls:
        if invalid_urls:
            logger.info(
                "Video Playlist request message {} by {} in {} contained valid URLS: {} and invalid URLs: {}.".format(
                    message, user, message.chat, valid_urls, invalid_urls
                )
            )
        else:
            logger.info(
                "Video Playlist request message {} by {} in {} contained valid URLS: {}.".format(
                    message, user, message.chat, valid_urls
                )
            )
    else:
        if invalid_urls:
            logger.info(
                "Video Playlist request message {} by {} in {} contained only invalid URLS: {}.".format(
                    message, user, message.chat, invalid_urls
                )
            )
            await message.reply_text(
                "The following are not valid URLs: {}. Please try again.".format(
                    invalid_urls
                ),
                disable_web_page_preview=True,
            )
            return
        else:
            logger.info(
                "Video Playlist request message {} by {} in {} contained no playlist URLS.".format(
                    message, user, message.chat
                )
            )
            await message.reply_text(
                "Please provide a YouTube playlist URL and try again."
            )
            return
    await send_videos_playlists(update, context, valid_urls, repost)


async def send_videos_playlists(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    playlist_urls: list[str],
    repost: bool,
) -> None:
    """Send videos in chat from playlist URL(s)."""
    await update.message.reply_text(
        "Fetching videos from {} playlists: {}".format(
            len(playlist_urls), playlist_urls
        ),
        disable_web_page_preview=True,
    )
    for playlist_url in playlist_urls:
        await send_videos_playlist(update, context, playlist_url, repost)


async def send_videos_playlist(
    update: Update, context: ContextTypes.DEFAULT_TYPE, playlist_url: str, repost: bool
) -> None:
    message = update.effective_message
    user = update.effective_user
    try:
        local_mode = get_setting("local_mode")
        ytdl_output_template = get_setting("video_output_template")
        ytdl_options = {
            # "format_sort": "codec:h264",
            # "format": "bv[ext=mp4]+ba[ext=m4a]/b[ext=mp4]",
            # "format": "bv*[vcodec=h264]+ba[acodec=aac]",
            # "format": "(bv*[vcodec~='^((he|a)vc|h26[45])']+ba)",
            "format": "(bv*[vcodec~='^((he|a)vc|h26[45])']+ba[ext=m4a])/b[ext=mp4]",
            "merge_output_format": "mp4",
            # # Directly download Opus audio streams from YouTube
            # "audioformat": "opus",
            # Remove the conversion postprocessor
            "postprocessors": [],
            # Use custom cookies if specified
            "cookiefile": get_setting("cookies_file"),
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
            "Fetching video playlist {} for user {} in chat with {}".format(
                playlist_url, user, message.chat
            )
        )
        bot_message = await message.reply_text("Fetching video playlist...")
        try:
            with youtube_dl.YoutubeDL(ytdl_options) as ytdl:
                # Retrieve playlist information without downloading
                info = ytdl.extract_info(playlist_url, download=False)
                # playlist_title = info["title"]
                # extractor = info['extractor'] # Use this if using the actual extractor name, which we are not currently using because of possible extractor names that are invalid for use in paths
                extractor = "youtube"
                extractor_key = info["extractor_key"]
                retry_message = None
                successful_new_video_uploads = 0
                successful_video_reposts = 0
                skipped_video_uploads = 0
                failed_downloads = 0
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
                                "Second fetch extract method (download-enabled) failed as well. There was no entries data in the result. Aborting video playlist at URL {} / {}.".format(
                                    info["url"], playlist_url
                                )
                            )
                            retry_message = await retry_message.edit_text(
                                "Both methods to fetch video playlist entries failed. Please try a different YouTube playlist URL instead."
                            )
                        else:
                            retry_message = await retry_message.edit_text(
                                "Initial fetching took longer because we had to use our alternate method. This may be because of a URL issue. Next time, please try to send just the YouTube video playlist's page URL, rather than the URL of a YouTube video playing in the playlist. Now we'll download and send the videos, please wait..."
                            )
                await bot_message.edit_text(
                    "Playlist fetched. Found {} videos. Downloading videos...".format(
                        len(entries)
                    )
                )
                for entry in entries:
                    skip_video = False
                    reposting = False
                    successful_video_upload = False
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
                                "video",
                            )
                            with get_con() as db_con:
                                if (
                                    len(
                                        db_con.execute(
                                            "SELECT * from uploads where chat_platform=? AND chat_id=? AND video_platform=? AND video_id=? AND upload_type=?;",
                                            db_params,
                                        ).fetchall()
                                    )
                                    > 0
                                ):
                                    if repost:
                                        logger.info(
                                            "YouTube video with ID {} already exists in the Telegram chat with ID {}, but {} was specified. Reposting video...".format(
                                                vid_id,
                                                message.chat_id,
                                                get_setting("videos_flag_repost_text"),
                                            )
                                        )
                                        await message.reply_text(
                                            "YouTube video with ID {} was already uploaded to this chat, but {} was specified so we will repost it here.".format(
                                                vid_id,
                                                get_setting("videos_flag_repost_text"),
                                            )
                                        )
                                        reposting = True
                                    else:
                                        logger.info(
                                            "YouTube video with ID {} already exists in the Telegram chat with ID {}. Skipping...".format(
                                                vid_id, message.chat_id
                                            )
                                        )
                                        await message.reply_text(
                                            "YouTube video with ID {} was already uploaded to this chat as a video.".format(
                                                vid_id
                                            )
                                        )
                                        skipped_video_uploads += 1
                                        skip_video = True
                                else:
                                    logger.info(
                                        "YouTube video with ID {} does not yet exist in the Telegram chat with ID {} as a video (DB Params: {}). We will download it.".format(
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
                            if skip_video:
                                remove_file(temp_path)
                                continue
                            atexit.register(remove_file, temp_path)
                        with youtube_dl.YoutubeDL(
                            ytdl_download_options
                        ) as ytdl_download:
                            logger.info('Downloading video from "%s"', vid_url)
                            entry_dl = ytdl_download.extract_info(vid_url)
                            dl_res = ytdl_download.download(
                                [
                                    vid_url,
                                ]
                            )
                            logger.info(
                                "Got result {} from downloading from video URL.".format(
                                    dl_res
                                ),
                            )
                            if dl_res != 0:
                                logger.warning(
                                    "Download result is not zero; there was likely an error with the download; skipping..."
                                )
                                failed_downloads += 1
                                continue
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
                        logger.info("Posting video to Telegram")
                        filename = bytes(
                            os.path.basename(temp_path),
                            encoding="latin1",
                            errors="ignore",
                        ).decode("latin1", "ignore")
                        logger.info(
                            "Sending video with temp path {} and filename {}.".format(
                                temp_path, filename
                            )
                        )
                        try:
                            filesize = os.path.getsize(temp_path)
                        except FileNotFoundError:
                            failed_downloads += 1
                            logger.warning(
                                "Video with ID {} and file_path {} was not found on filesystem. Skipping upload.".format(
                                    vid_id, temp_path
                                )
                            )
                            continue
                        logger.info(
                            "File {} is {:,} bytes large.".format(temp_path, filesize)
                        )
                        caption = "{title}\n({url})".format(
                            title=video_title,
                            url=vid_url[: 1020 - 3 - len(video_title)],
                        )
                        try:
                            if local_mode:
                                video_message = await context.bot.send_video(
                                    chat_id=message.chat_id,
                                    video="file://{}".format(temp_path),
                                    duration=int(entry["duration"]),
                                    # title=video_title,
                                    thumbnail=thumbnail_file,
                                    disable_notification=True,
                                    caption=caption,
                                )
                            else:
                                with open(temp_path, "rb") as video_file:
                                    video_message = await context.bot.send_video(
                                        chat_id=message.chat_id,
                                        video=video_file,
                                        duration=int(entry["duration"]),
                                        # title=video_title,
                                        thumbnail=thumbnail_file,
                                        disable_notification=True,
                                        caption=caption,
                                    )
                            if video_message:
                                logger.info("VIDEO_MESSAGE TRUE")
                                successful_video_upload = True
                                if reposting:
                                    successful_video_reposts += 1
                                else:
                                    successful_new_video_uploads += 1
                            remove_file(temp_path)
                        except FileNotFoundError:
                            logger.warning(
                                "YouTube video with ID {} could not be found on disk. It likely did not download successfully.".format(
                                    vid_id
                                )
                            )
                        else:
                            if USE_DB:
                                if successful_video_upload:
                                    db_params = (
                                        "telegram",
                                        str(message.chat_id),
                                        str(video_message.message_id),
                                        extractor.strip(),
                                        str(vid_id).strip(),
                                        "video",
                                    )
                                    with get_con() as db_con:
                                        db_con.execute(
                                            "INSERT OR IGNORE INTO uploads (chat_platform, chat_id, message_id, video_platform, video_id, upload_type) VALUES (?, ?, ?, ?, ?, ?);",
                                            db_params,
                                        )
                                        logger.info(
                                            "Added YouTube video with ID {} in Telegram chat ID {} (DB Params: {}) to DB".format(
                                                vid_id, message.chat_id, db_params
                                            )
                                        )
                    else:
                        # Value of entry evaluated Falsy. Skip it
                        continue
        except youtube_dl.utils.DownloadError:
            await bot_message.edit_text(
                "Failed to download playlist with URL {}.".format(playlist_url)
            )
        except KeyboardInterrupt:
            raise
        except Exception as e:
            # await message.reply_text("Ran into an error. Please try again later.")
            exc_msg = "Ran into an error while downloading/sending music file(s) for playlist URL {} with info {}: {}".format(
                playlist_url, len(info), e
            )
            logger.exception(exc_msg)
            raise Exception(exc_msg)
        if repost:
            logger.info(
                "Finished Fetching playlist {} for user {} in chat {}. Successfully uploaded {} previously unsent videos of the {} videos in the playlist. Reposted {} videos because {} was specified. {} of the videos failed to download.".format(
                    playlist_url,
                    user,
                    message.chat,
                    successful_new_video_uploads,
                    len(entries),
                    successful_video_reposts,
                    get_setting("songs_flag_repost_text"),
                    failed_downloads,
                )
            )
            await message.reply_text(
                "Finished. Successfully uploaded {} previously unsent videos of the {} videos in the playlist. Reposted {} videos because {} was specified. {} of the videos failed to download due to issues with YouTube (possibly restrictions, private/unavailable videos, etc.).".format(
                    successful_new_video_uploads,
                    len(entries),
                    successful_video_reposts,
                    get_setting("songs_flag_repost_text"),
                    failed_downloads,
                )
            )
        else:
            logger.info(
                "Finished Fetching playlist {} for user {} in chat {}. Successfully uploaded {} of {} videos in the playlist. Skipped {} because they were already sent to the chat. {} of the videos failed to download.".format(
                    playlist_url,
                    user,
                    message.chat,
                    successful_new_video_uploads,
                    len(entries),
                    skipped_video_uploads,
                    failed_downloads,
                )
            )
            await message.reply_text(
                "Finished. Successfully uploaded {} of {} videos in the playlist. Skipped {} videos because they were already sent to this chat. {} of the videos failed to download due to issues with YouTube (possibly restrictions, private/unavailable videos, etc.).".format(
                    successful_new_video_uploads,
                    len(entries),
                    skipped_video_uploads,
                    failed_downloads,
                )
            )
        await asyncio.sleep(5)
        await bot_message.delete()
    except KeyboardInterrupt:
        raise
    except Exception:
        message.reply_text(
            "Failed to send video playlist with URL {}.".format(playlist_url),
            disable_web_page_preview=True,
        )
        return False


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
    local_mode = get_setting("local_mode")
    base_url = get_setting("base_url")
    logout: bool = get_setting("logout")

    # Create the Application and pass it your bot's token.
    app_builder = (
        Application.builder()
        .token(token)
        .read_timeout(30)
        .write_timeout(30)
        .connect_timeout(30)
        .pool_timeout(5)
    )

    if local_mode:
        logger.info("Starting in Local Mode.")
        app_builder = app_builder.local_mode(True).base_url(base_url)
    else:
        logger.info("Not running in Local Mode. Using official Telegram Bot API.")

    application = app_builder.build()

    if logout:
        logger.info(
            "Logging out of the Telegram Bot API to enable reliable connections via local Telegram Bot API server. Please remove the --logout flag from your next run and specify a manual Telegram Bot API Server address."
        )
        asyncio.run(application.bot.logOut())
        return 0

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help))
    application.add_handler(CommandHandler("song", song))
    application.add_handler(CommandHandler("songs", songs))
    application.add_handler(CommandHandler("video", video))
    application.add_handler(CommandHandler("videos", videos))
    application.add_handler(CommandHandler("dump_db", dump_db))

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
