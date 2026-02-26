import os
import sys
import re
import json
import time
import asyncio # type: ignore
import logging
import psutil
import requests
import traceback
import subprocess
# import paramiko
import threading
import base64
from datetime import datetime, timedelta, timezone
from pprint import pformat
from dotenv import load_dotenv
from urllib.parse import unquote, urljoin, parse_qs, urlparse, quote
from typing import Any
from uuid import uuid4
from io import BytesIO
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google import genai
from google.genai import types
from telegram.ext import (
    filters,
    MessageHandler,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    CallbackQueryHandler,
    MessageReactionHandler
)
from telegram import (
    InputTextMessageContent,
    Update,
    Bot,
    MessageEntity,
    InputMediaPhoto,
    InputMediaVideo,
    LinkPreviewOptions,
    InlineQueryResultArticle,
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup
)
from telegram.error import (
    NetworkError,
    BadRequest
)
from telegram.constants import (
    ParseMode,
    ChatAction,
    ChatMemberStatus,
)
from telegraph.aio import Telegraph # type: ignore
from PIL import Image
from pyzbar.pyzbar import decode # pyright: ignore[reportUnknownVariableType, reportMissingTypeStubs]

# Load environment variables from .env file
load_dotenv()

logging_file = os.path.join("log", f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log")

# Configure logging to only show messages from your script
logging.basicConfig(
    handlers=[
        logging.FileHandler(
            filename=logging_file,
            encoding="utf-8",
            mode="w+",
        ),
        logging.StreamHandler()
    ],
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s: %(message)s",
    datefmt="%F %A %T",
    level=logging.DEBUG
)

# Create your own logger for your bot messages
bot_logger = logging.getLogger("xhsfwbot")
bot_logger.setLevel(logging.DEBUG)

# Global variables for network monitoring
last_successful_request = time.time()
network_timeout_threshold = 120  # 2 minutes without successful requests triggers restart
is_network_healthy = True

# Concurrency control
max_concurrent_requests = 5  # Maximum number of concurrent note processing
processing_semaphore = asyncio.Semaphore(max_concurrent_requests)

# Whitelist functionality
whitelist_enabled = os.getenv('WHITELIST_ENABLED', 'false').lower() == 'true'
bot_logger.debug(f"Whitelist enabled: {whitelist_enabled}")
private_channel_id = int(os.getenv('CHANNEL_ID', '0'))
help_message_id = int(os.getenv('HELP_MESSAGE_ID', '0'))

# Auto restart configuration (in hours, default 24 hours, 0 to disable)
auto_restart_interval = float(os.getenv('AUTO_RESTART_HOURS', '8'))
bot_start_time = time.time()

async def is_user_whitelisted(user_id: int | None, bot: Bot | None = None) -> bool:
    """Check if a user is whitelisted"""
    if user_id is None:
        return False
    if not whitelist_enabled:
        return True
    if bot is not None:
        if private_channel_id:
            try:
                member = await bot.get_chat_member(private_channel_id, user_id)
                if member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                    return True
            except BadRequest as e:
                bot_logger.error(f"Error checking membership for user {user_id} in channel {private_channel_id}: {e}")
    return False

with open('redtoemoji.json', 'r', encoding='utf-8') as f:
    redtoemoji = json.load(f)
    f.close()

URL_REGEX = r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:\'\".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"""

FLASK_SERVER_NAME = os.getenv('FLASK_SERVER_NAME', '127.0.0.1')

def replace_redemoji_with_emoji(text: str) -> str:
    for red_emoji, emoji in redtoemoji.items():
        text = text.replace(red_emoji, emoji)
    return text

def check_network_connectivity() -> bool:
    """Check if network connectivity is available by testing multiple endpoints"""
    test_urls = [
        "https://api.telegram.org",
        "https://www.google.com", 
        "https://1.1.1.1"
    ]

    # Check for pool timeout errors in recent log content
    try:
        with open(logging_file, 'r', encoding='utf-8') as log_str:
            # Only read the last 50KB of log to avoid memory issues
            log_str.seek(0, 2)  # Go to end of file
            file_size = log_str.tell()
            read_size = min(file_size, 50000)  # Read last 50KB
            log_str.seek(max(0, file_size - read_size))
            log_content = log_str.read()
            
            # Check for pool timeout error pattern
            pool_timeout_patterns = [
                "Pool timeout: All connections in the connection pool are occupied",
                "Request was *not* sent to Telegram",
                "networkloop: Network Retry Loop (Polling Updates): Timed out"
            ]
            
            # Count occurrences in recent log
            pool_timeout_count = sum(1 for pattern in pool_timeout_patterns if pattern in log_content)
            if pool_timeout_count >= 2:
                bot_logger.warning(f"Detected {pool_timeout_count} pool timeout indicators in recent logs")
                return False
    except Exception as e:
        bot_logger.error(f"Error reading log file: {e}")

    for url in test_urls:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return True
        except:
            continue
    return False

def update_network_status(success: bool = True):
    """Update the global network status tracking"""
    global last_successful_request, is_network_healthy
    if success:
        last_successful_request = time.time()
        is_network_healthy = True
    else:
        current_time = time.time()
        if current_time - last_successful_request > network_timeout_threshold:
            is_network_healthy = False
            bark_notify("xhsfwbot network is unhealthy.")

def network_monitor():
    """Background network monitoring function"""
    global is_network_healthy
    while True:
        try:
            time.sleep(15)  # Check every 15 seconds
            current_time = time.time()
            if current_time - last_successful_request > network_timeout_threshold:
                bot_logger.warning(f"No successful network requests for {network_timeout_threshold} seconds")
                if not check_network_connectivity():
                    bot_logger.error("Network connectivity test failed - triggering restart")
                    is_network_healthy = False
                    restart_script()
                    break
        except Exception as e:
            bot_logger.error(f"Network monitor error: {e}")
            time.sleep(10)

def scheduled_restart_monitor():
    """Background function to trigger scheduled restarts for memory management"""
    if auto_restart_interval <= 0:
        bot_logger.info("Scheduled auto-restart is disabled")
        return
    
    restart_interval_seconds = auto_restart_interval * 3600
    bot_logger.info(f"Scheduled auto-restart enabled: will restart every {auto_restart_interval} hours")
    
    while True:
        try:
            time.sleep(60)  # Check every minute
            uptime = time.time() - bot_start_time
            if uptime >= restart_interval_seconds:
                bot_logger.info(f"Scheduled restart triggered after {uptime/3600:.2f} hours of uptime")
                bark_notify(f"xhsfwbot scheduled restart after {uptime/3600:.1f}h uptime")
                restart_script()
                break
        except Exception as e:
            bot_logger.error(f"Scheduled restart monitor error: {e}")
            time.sleep(60)

class Note:
    def __init__(
            self,
            note_data: dict[str, Any],
            comment_list_data: dict[str, Any],
            live: bool = False,
            telegraph: bool = False,
            with_xsec_token: bool = False,
            original_xsec_token: str = '',
            with_full_data: bool = False,
            telegraph_account: Telegraph | None = None,
            anchorCommentId: str = ''
    ) -> None:
        self.telegraph_account = telegraph_account
        self.telegraph = telegraph
        self.live = live
        if not note_data['data']:
            raise Exception("Note data not found!")
        self.user: dict[str, str | int] = {
            'id': note_data['data'][0]['user']['id'],
            'name': note_data['data'][0]['user']['name'],
            'red_id': note_data['data'][0]['user'].get('red_id', ''),
            'image': get_clean_url(note_data['data'][0]['user']['image']),
        }
        # self.text_language_code = note_data['data'][0]['note_list'][0]['text_language_code']

        self.title: str = note_data['data'][0]['note_list'][0]['title'] if note_data['data'][0]['note_list'][0]['title'] else f""
        self.type: str = note_data['data'][0]['note_list'][0]['type']

        self.raw_desc = replace_redemoji_with_emoji(note_data['data'][0]['note_list'][0]['desc'])
        bot_logger.debug(f"Note raw_desc\n\n {self.raw_desc}")
        self.desc = re.sub(
            r'(?P<tag>#\S+?)\[\S+\]#',
            r'\g<tag> ',
            self.raw_desc
        )
        self.time = note_data['data'][0]['note_list'][0]['time']
        self.ip_location = note_data['data'][0]['note_list'][0]['ip_location']\
            if 'ip_location' in note_data['data'][0]['note_list'][0] else '?'
        self.collected_count = note_data['data'][0]['note_list'][0]['collected_count']
        self.comments_count = note_data['data'][0]['note_list'][0]['comments_count']
        self.shared_count = note_data['data'][0]['note_list'][0]['shared_count']
        self.liked_count = note_data['data'][0]['note_list'][0]['liked_count']
        # self.last_update_time = note_data['data'][0]['note_list'][0]['last_update_time']
        self.comments_with_context: list[dict[str, Any]] = []
        if anchorCommentId:
            self.comments_with_context = extract_anchor_comment_id(comment_list_data['data'])
            bot_logger.debug(f"Comments with context extracted for anchorCommentId {anchorCommentId}:\n{pformat(self.comments_with_context)}")
        self.comments = extract_all_comments(comment_list_data['data'])
        self.length: int = len(self.desc + self.title)

        self.tags: list[str] = [tag['name'] for tag in note_data['data'][0]['note_list'][0]['hash_tag']]
        self.tag_string: str = ' '.join([f"#{tag}" for tag in self.tags])

        self.thumbnail = note_data['data'][0]['note_list'][0]['share_info']['image']
        self.images_list: list[dict[str, str]] = []
        if 'images_list' in note_data['data'][0]['note_list'][0]:
            for each in note_data['data'][0]['note_list'][0]['images_list']:
                if 'live_photo' in each and self.live:
                    bot_logger.debug(f'live photo found in {each}')
                    live_urls: list[str] = []
                    for s in each['live_photo']['media']['stream']:
                        if each['live_photo']['media']['stream'][s]:
                            for ss in each['live_photo']['media']['stream'][s]:
                                live_urls.append(ss['backup_urls'][0] if ss['backup_urls'] else ss['master_url'])
                    if len(live_urls) > 0:
                        self.images_list.append(
                            {'live': 'True', 'url': live_urls[0], 'thumbnail': remove_image_url_params(each['url'])}
                        )
                original_img_url = each['original']
                if re.findall(r'sns-na-i\d.xhscdn.com', original_img_url):
                    original_img_url = re.sub(r'sns-na-i\d.xhscdn.com', 'sns-na-i6.xhscdn.com', original_img_url).split('?imageView')[0] + '?imageView2/2/w/5000/h/5000/format/webp/q/56&redImage/frame/0'
                self.images_list.append(
                    {
                        'live': '',
                        'url': remove_image_url_params(original_img_url),
                        'thumbnail': remove_image_url_params(each['url_multi_level']['low'])
                    }
                )
        bot_logger.debug(f"Images found: {self.images_list}")
        self.url = get_clean_url(note_data['data'][0]['note_list'][0]['share_info']['link'])
        self.with_xsec_token = with_xsec_token
        if with_xsec_token:
            if not original_xsec_token:
                parsed_url = urlparse(str(note_data['data'][0]['note_list'][0]['share_info']['link']))
                self.xsec_token = parse_qs(parsed_url.query)['xsec_token'][0]
            else:
                self.xsec_token = original_xsec_token
            self.url += f"?xsec_token={self.xsec_token}"
        self.noteId = re.findall(r"[a-z0-9]{24}", self.url)[0]
        self.video_url = ''
        if 'video' in note_data['data'][0]['note_list'][0]:
            self.video_url = note_data['data'][0]['note_list'][0]['video']['url']
            if not re.findall(r'sign=[0-9a-z]+', self.video_url):
                self.video_url = re.sub(r'[0-9a-z\-]+\.xhscdn\.(com|net)', 'sns-bak-v1.xhscdn.com', self.video_url) #.split('?imageView')[0] + '?imageView2/2/w/5000/h/5000/format/webp/q/56&redImage/frame/0'
        if telegraph:
            self.to_html()
        tgmsg_result = self.to_telegram_message(preview=bool(self.length >= 666))
        bot_logger.debug(f"tgmsg_result: {tgmsg_result}\nlen: {self.length}, preview? = {bool(self.length >= 666)}")
        media_group_result = self.to_media_group()
        bot_logger.debug(f"media_group_result: {media_group_result}")

    async def initialize(self) -> None:
        if self.telegraph:
            await self.to_telegraph()
        self.short_preview = ''

    def to_dict(self) -> dict[str, str | int | Any]:
        return {
            'user': self.user,
            'title': self.title,
            'type': self.type,
            'desc': self.desc,
            'length': self.length,
            'time': self.time,
            'ip_location': self.ip_location,
            'collected_count': self.collected_count,
            'comments_count': self.comments_count,
            'shared_count': self.shared_count,
            'liked_count': self.liked_count,
            'images_list': getattr(self, 'images_list', []),
            'video_url': getattr(self, 'video_url', ''),
            'url': self.url,
        }
    
    def media_for_llm(self) -> list[dict[str, str]]:
        media_list: list[dict[str, str]] = []
        for img in self.images_list:
            if not img['live']:
                media_list.append({
                    'type': 'image',
                    'url': img['url']
                })
        # For video notes, use the thumbnail image instead of the full video
        if self.video_url and self.thumbnail:
            media_list.append({
                'type': 'image',
                'url': self.thumbnail
            })
        return media_list

    def to_html(self) -> str:
        html = ''
        html += f'<h3><a href="{self.url}">{self.title}</a></h3>' if self.title else ''
        for img in self.images_list:
            if not img['live']:
                html += f'<img src="{img["url"]}"></img>'
            else:
                html += f'<video src="{img["url"]}"></video>'
        if self.video_url:
            html += f'<video src="{self.video_url}"></video>'
        for lines in self.desc.split('\n'):
            line_html = tg_msg_escape_html(lines)
            html += f'<blockquote>{line_html}</blockquote>'
        html += f'<h4>👤 <a href="https://www.xiaohongshu.com/user/profile/{self.user["id"]}{f"?xsec_token={self.xsec_token}" if self.with_xsec_token else ""}"> @{self.user["name"]} ({self.user.get("red_id", "")})</a></h4>'
        html += f'<img src="{self.user["image"]}"></img>'
        html += f'<p>{get_time_emoji(self.time)} {convert_timestamp_to_timestr(self.time)}</p>'
        html += f'<p>❤️ {self.liked_count} ⭐ {self.collected_count} 💬 {self.comments_count} 🔗 {self.shared_count}</p>'
        if hasattr(self, 'ip_location'):
            ipaddr_html = tg_msg_escape_html(self.ip_location)
        else:
            ipaddr_html = '?'
        html += f'<p>📍 {ipaddr_html}</p>'
        html += f'<blockquote><a href="{self.url}">Source</a></blockquote>'
        if self.comments:
            html += '<hr>'
            for i, comment in enumerate(self.comments):
                html += f'<h4>💬 <a href="https://www.xiaohongshu.com/discovery/item/{self.noteId}?anchorCommentId={comment["id"]}{f"&xsec_token={self.xsec_token}" if self.with_xsec_token else ""}">Comment</a></h4>'
                if 'target_comment' in comment:
                    html += f'<p>↪️ <a href="https://www.xiaohongshu.com/user/profile/{comment["target_comment"]["user"]["userid"]}{f"?xsec_token={self.xsec_token}" if self.with_xsec_token else ""}"> {'@' + comment["target_comment"]["user"].get("nickname", "")} ({comment["target_comment"]["user"].get('red_id', '')})</a></p>'
                html += f'<p>{tg_msg_escape_html(replace_redemoji_with_emoji(comment["content"]))}</p>'
                for pic in comment['pictures']:
                    if 'mp4' in pic:
                        html += f'<video src="{pic}"></video>'
                    else:
                        html += f'<img src="{pic}"></img>'
                if comment.get('audio_url', ''):
                    html += f'<p><a href="{comment["audio_url"]}">🎤 Voice</a></p>'
                html += f'<p>❤️ {comment["like_count"]} 💬 {comment["sub_comment_count"]}<br>📍 {tg_msg_escape_html(comment["ip_location"])}<br>{get_time_emoji(comment["time"])} {convert_timestamp_to_timestr(comment["time"])}</p>'
                html += f'<p>👤 <a href="https://www.xiaohongshu.com/user/profile/{comment["user"]["userid"]}{f"?xsec_token={self.xsec_token}" if self.with_xsec_token else ""}"> {'@' + comment["user"].get("nickname", "")} ({comment["user"].get("red_id", "")})</a></p>'
                for sub_comment in comment.get('sub_comments', []):
                    html += '<blockquote><blockquote>'
                    html += f'<h4>💬 <a href="https://www.xiaohongshu.com/discovery/item/{self.noteId}?anchorCommentId={sub_comment["id"]}{f"&xsec_token={self.xsec_token}" if self.with_xsec_token else ""}">Comment</a></h4>'
                    if 'target_comment' in sub_comment:
                        html += f'<br><p>  ↪️  <a href="https://www.xiaohongshu.com/user/profile/{sub_comment["target_comment"]["user"]["userid"]}{f"?xsec_token={self.xsec_token}" if self.with_xsec_token else ""}"> {'@' + sub_comment["target_comment"]["user"].get("nickname", "")} ({sub_comment["target_comment"]["user"].get("red_id", "")})</a></p>'
                    html += f'<br><p>{tg_msg_escape_html(replace_redemoji_with_emoji(sub_comment["content"]))}</p>'
                    for pic in sub_comment['pictures']:
                        if 'mp4' in pic:
                            html += f'<br><video src="{pic}"></video>'
                        else:
                            html += f'<br><img src="{pic}"></img>'
                    if sub_comment.get('audio_url', ''):
                        html += f'<br><p><a href="{sub_comment["audio_url"]}">🎤 Voice</a></p>'
                    html += f'<br><p>❤️ {sub_comment["like_count"]} 💬 {sub_comment["sub_comment_count"]}<br>📍 {tg_msg_escape_html(sub_comment["ip_location"])}<br>{get_time_emoji(sub_comment["time"])} {convert_timestamp_to_timestr(sub_comment["time"])}</p>'
                    html += f'<br><p>👤 <a href="https://www.xiaohongshu.com/user/profile/{sub_comment["user"]["userid"]}{f"?xsec_token={self.xsec_token}" if self.with_xsec_token else ""}"> {'@' + sub_comment["user"].get("nickname", "")} ({sub_comment["user"].get("red_id", "")})</a></p>'
                    html += '</blockquote></blockquote>'
                if i != len(self.comments) - 1:
                    html += f'<hr>'
        self.html = html
        bot_logger.debug(f"HTML generated, \n\n{self.html}\n\n")
        return self.html

    def __str__(self) -> str:
        self.content = '笔记标题：' + self.title + '\n' + '笔记正文：' + self.desc
        for img in self.images_list:
            if not img['live']:
                img = requests.get(img["url"]).content # TODO image to base64?
        self.content += f'\n发布者：@{self.user["name"]} ({self.user.get('red_id', '')})\n'
        self.content += f'{get_time_emoji(self.time)} {convert_timestamp_to_timestr(self.time)}\n'
        self.content += f'点赞：{self.liked_count}收藏：{self.collected_count}评论：{self.comments_count}分享：{self.shared_count}\n'
        if hasattr(self, 'ip_location'):
            ipaddr_html = tg_msg_escape_html(self.ip_location) + '\n'
        else:
            ipaddr_html = '?\n'
        self.content += f'IP 地址：{ipaddr_html}\n\n评论区：\n\n'
        if self.comments:
            self.content += '\n'
            for i, comment in enumerate(self.comments):
                if comment["content"]:
                    self.content += f'💬 评论\n'
                    # if 'target_comment' in comment:
                    #     self.content += f'↪️ @{comment["target_comment"]["user"].get("nickname", "")} ({comment["target_comment"]["user"].get('red_id', '')})\n'
                    self.content += f'{tg_msg_escape_html(replace_redemoji_with_emoji(comment["content"]))}\n'
                    self.content += f'点赞：{comment["like_count"]}\nIP 地址：{tg_msg_escape_html(comment["ip_location"])}\n{get_time_emoji(comment["time"])} {convert_timestamp_to_timestr(comment["time"])}\n'
                    # self.content += f'发布者：@{comment["user"].get("nickname", "")} ({comment["user"].get('red_id', '')})\n'
                for sub_comment in comment.get('sub_comments', []):
                    if sub_comment["content"]:
                        self.content += f'💬 回复\n'
                        # if 'target_comment' in sub_comment:
                            # self.content += f'↪️ @{sub_comment["target_comment"]["user"].get("nickname", "")} ({sub_comment["target_comment"]["user"].get('red_id', '')})\n'
                        self.content += f'{tg_msg_escape_html(replace_redemoji_with_emoji(sub_comment["content"]))}\n'
                        self.content += f'点赞：{sub_comment["like_count"]}\nIP 地址：{tg_msg_escape_html(sub_comment["ip_location"])}\n{get_time_emoji(sub_comment["time"])} {convert_timestamp_to_timestr(sub_comment["time"])}\n'
                        # self.content += f'发布者：@{sub_comment["user"].get("nickname", "")} ({sub_comment["user"].get('red_id', '')})\n'
                if i != len(self.comments) - 1:
                    self.content += f'\n'
        bot_logger.debug(f"String generated, \n\n{self.content}\n\n")
        return self.content

    async def to_telegraph(self) -> str:
        if not hasattr(self, 'html'):
            self.to_html()
        if not self.telegraph_account:
            self.telegraph_account = Telegraph()
            await self.telegraph_account.create_account( # type: ignore
                short_name='@xhsfwbot',
            )
        response = await self.telegraph_account.create_page( # type: ignore
            title=f"{self.title} @{self.user['name']}",
            author_name=f'@{self.user["name"]} ({self.user.get('red_id', '')})',
            author_url=f"https://www.xiaohongshu.com/user/profile/{self.user['id']}",
            html_content=self.html,
        )
        self.telegraph_url = response['url']
        bot_logger.debug(f"Generated Telegraph URL: {self.telegraph_url}")
        return self.telegraph_url

    async def to_telegram_message(self, preview: bool = False) -> str:
        message = f'*[{tg_msg_escape_markdown_v2(self.title)}]({self.url})*\n' if self.title else ''
        if preview:
            message += f'{make_block_quotation(self.desc[:555] + '...')}\n' if self.desc else ''
            if hasattr(self, 'telegraph_url'):
                message += f'\n📝 [View more via Telegraph]({tg_msg_escape_markdown_v2(self.telegraph_url)})\n'
            else:
                message += f'\n📝 [View more via Telegraph]({tg_msg_escape_markdown_v2(await self.to_telegraph())})\n'
        else:
            message += f'{make_block_quotation(self.desc)}\n' if self.desc else ''
            if hasattr(self, 'telegraph_url'):
                message += f'\n📝 [Telegraph]({tg_msg_escape_markdown_v2(self.telegraph_url)})\n'
            elif self.telegraph:
                message += f'\n📝 [Telegraph]({tg_msg_escape_markdown_v2(await self.to_telegraph())})\n'
        message += f'\n[@{tg_msg_escape_markdown_v2(self.user["name"])} \\({tg_msg_escape_markdown_v2(self.user.get('red_id', ''))}\\)](https://www.xiaohongshu.com/user/profile/{self.user["id"]})\n'
        if type(self.liked_count) == str:
            like_html = tg_msg_escape_markdown_v2(self.liked_count)
        else:
            like_html = str(self.liked_count)
        if type(self.collected_count) == str:
            collected_html = tg_msg_escape_markdown_v2(self.collected_count)
        else:
            collected_html = self.collected_count
        if type(self.comments_count) == str:
            comments_html = tg_msg_escape_markdown_v2(self.comments_count)
        else:
            comments_html = self.comments_count
        if type(self.shared_count) == str:
            shared_html = tg_msg_escape_markdown_v2(self.shared_count)
        else:
            shared_html = self.shared_count
        message += f'>❤️ {like_html} ⭐ {collected_html} 💬 {comments_html} 🔗 {shared_html}'
        message += f'\n>{get_time_emoji(self.time)} {tg_msg_escape_markdown_v2(convert_timestamp_to_timestr(self.time))}\n'
        if hasattr(self, 'ip_location'):
            ip_html = tg_msg_escape_markdown_v2(self.ip_location)
        else:
            ip_html = '?'
        message += f'>📍 {ip_html}\n\n📕 [Note Source]({self.url})'
        self.message = message
        bot_logger.debug(f"Telegram message generated, \n\n{self.message}\n\n")
        return message

    async def to_media_group(self) -> list[list[InputMediaPhoto | InputMediaVideo]]:
        self.medien: list[InputMediaPhoto | InputMediaVideo] = []
        self.video_too_large = False  # Flag to track if video is too large
        for _, imgs in enumerate(self.images_list):
            if not imgs['live']:
                self.medien.append(
                    InputMediaPhoto(imgs['url'])
                )
            # else:
            #     self.medien.append(
            #         InputMediaVideo(
            #             requests.get(imgs['url']).content
            #         )
            #     )
        if self.video_url:
            # Check video size before downloading
            try:
                head_response = requests.head(self.video_url, timeout=10)
                content_length = head_response.headers.get('Content-Length', '0')
                video_size_mb = int(content_length) / (1024 * 1024)  # Convert to MB
                
                bot_logger.info(f"Video size: {video_size_mb:.2f}MB")
                
                if video_size_mb > 50:
                    bot_logger.warning(f"Video size {video_size_mb:.2f}MB exceeds 50MB limit, skipping video upload")
                    self.video_too_large = True
                    self.medien = []  # Clear medien to send text only
                else:
                    # Only download if size is acceptable
                    video_data = requests.get(self.video_url).content
                    self.medien = [InputMediaVideo(video_data)]
            except Exception as e:
                bot_logger.error(f"Failed to check video size: {e}, skipping video")
                self.video_too_large = True
                self.medien = []
        self.medien_parts = [self.medien[i:i + 10] for i in range(0, len(self.medien), 10)]
        return self.medien_parts

    async def send_as_telegram_message(self, bot: Bot, chat_id: int, reply_to_message_id: int = 0) -> None:
        sent_message = None
        if not hasattr(self, 'medien_parts'):
            self.medien_parts: list[list[InputMediaPhoto | InputMediaVideo]] = await self.to_media_group()
        
        # Prepare caption for media group
        caption_text = self.message if hasattr(self, 'message') else await self.to_telegram_message(preview=bool(self.length >= 666))
        
        # If video is too large, send only text message
        if hasattr(self, 'video_too_large') and self.video_too_large:
            sent_message = [await bot.send_message(
                chat_id=chat_id,
                text=caption_text,
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_to_message_id=reply_to_message_id,
                disable_notification=True,
                link_preview_options=LinkPreviewOptions(is_disabled=True)
            )]
        else:
            for i, part in enumerate(self.medien_parts):
                if self.video_url:
                    await bot.send_chat_action(
                        chat_id=chat_id,
                        action=ChatAction.UPLOAD_VIDEO,
                    )
                else:
                    await bot.send_chat_action(
                        chat_id=chat_id,
                        action=ChatAction.UPLOAD_PHOTO,
                    )
                try:
                    # Add caption to the first media item
                    if i == 0 and part:
                        part[0] = InputMediaPhoto(part[0].media, caption=caption_text, parse_mode=ParseMode.MARKDOWN_V2) if isinstance(part[0], InputMediaPhoto) else InputMediaVideo(part[0].media, caption=caption_text, parse_mode=ParseMode.MARKDOWN_V2)
                    
                    if self.video_url:
                        # Add caption to video
                        video_media = self.medien[-1:]
                        if video_media and i == 0:
                            video_media[0] = InputMediaVideo(video_media[0].media, caption=caption_text, parse_mode=ParseMode.MARKDOWN_V2)
                        sent_message = await bot.send_media_group(
                            chat_id=chat_id,
                            reply_to_message_id=reply_to_message_id,
                            media=video_media,
                            disable_notification=True
                        )
                    else:
                        sent_message = await bot.send_media_group(
                            chat_id=chat_id,
                            reply_to_message_id=reply_to_message_id,
                            media=part,
                            disable_notification=True
                        )
                except:
                    bot_logger.error(f"Failed to send media group:\n{traceback.format_exc()}")
                    media: list[InputMediaPhoto | InputMediaVideo] = []
                    for j, p in enumerate(part):
                        if type(p.media) == str and '.mp4' not in p.media:
                            media_content = requests.get(p.media).content
                            # Add caption to first media item in retry
                            if j == 0 and i == 0:
                                media.append(InputMediaPhoto(media_content, caption=caption_text, parse_mode=ParseMode.MARKDOWN_V2))
                            else:
                                media.append(InputMediaPhoto(media_content))
                        elif self.video_url and type(p.media) != str:
                            media.append(p)
                    if self.video_url:
                        video_media = self.medien[-1:]
                        if video_media and i == 0:
                            video_media[0] = InputMediaVideo(video_media[0].media, caption=caption_text, parse_mode=ParseMode.MARKDOWN_V2)
                        sent_message = await bot.send_media_group(
                            chat_id=chat_id,
                            reply_to_message_id=reply_to_message_id,
                            media=video_media,
                            disable_notification=True
                        )
                    else:
                        sent_message = await bot.send_media_group(
                            chat_id=chat_id,
                            reply_to_message_id=reply_to_message_id,
                            media=media,
                            disable_notification=True
                        )
        
        if not sent_message:
            bot_logger.error("No message was sent!")
            return
        
        # Store note content and media for AI summary on reaction
        try:
            msg_identifier = f"{chat_id}.{sent_message[0].message_id}"
            msg_data = {
                'content': str(self),
                'media': self.media_for_llm()
            }
            msg_file_path = os.path.join('data', f'{msg_identifier}.json')
            with open(msg_file_path, 'w', encoding='utf-8') as f:
                json.dump(msg_data, f, indent=4, ensure_ascii=False)
            bot_logger.debug(f"Saved message data to {msg_file_path} for AI summary")
        except Exception as e:
            bot_logger.error(f"Failed to save message data: {e}")
        
        reply_id = sent_message[0].message_id
        comment_id_to_message_id: dict[str, Any] = {}
        if self.comments_with_context:
            for _, comment in enumerate(self.comments_with_context):
                comment_text = ''
                comment_text += f'💬 [Comment](https://www.xiaohongshu.com/discovery/item/{self.noteId}?anchorCommentId={comment["id"]}{f"&xsec_token={self.xsec_token}" if self.with_xsec_token else ""})'
                if 'target_comment' in comment:
                    comment_text += f'\n↪️ [@{tg_msg_escape_markdown_v2(comment["target_comment"]["user"].get("nickname", ""))} \\({tg_msg_escape_markdown_v2(comment["target_comment"]["user"].get('red_id', ''))}\\)](https://www.xiaohongshu.com/user/profile/{comment["target_comment"]["user"]["userid"]}{f"?xsec_token={self.xsec_token}" if self.with_xsec_token else ""})\n'
                else:
                    comment_text += '\n'
                comment_text += f'{make_block_quotation(replace_redemoji_with_emoji(comment["content"]))}\n'
                comment_text += f'❤️ {comment["like_count"]} 💬 {comment["sub_comment_count"]} 📍 {tg_msg_escape_markdown_v2(comment["ip_location"])} {get_time_emoji(comment["time"])} {tg_msg_escape_markdown_v2(convert_timestamp_to_timestr(comment["time"]))}\n'
                comment_text += f'👤 [@{tg_msg_escape_markdown_v2(comment["user"].get("nickname", ""))} \\({tg_msg_escape_markdown_v2(comment["user"].get('red_id', ''))}\\)](https://www.xiaohongshu.com/user/profile/{comment["user"]["userid"]}{f"?xsec_token={self.xsec_token}" if self.with_xsec_token else ""})'
                bot_logger.debug(f"Sending comment:\n{comment_text}")
                if 'target_comment' in comment and _ > 0:
                    reply_id = comment_id_to_message_id[comment['target_comment']['id']].message_id
                if comment['pictures']:
                    await bot.send_chat_action(
                        chat_id=chat_id,
                        action=ChatAction.UPLOAD_PHOTO
                    )
                    # 1. Split pictures into chunks of 10
                    picture_chunks = [comment['pictures'][i:i + 10] for i in range(0, len(comment['pictures']), 10)]
                    for i, chunk in enumerate(picture_chunks):
                        media: list[InputMediaPhoto | InputMediaVideo] = []
                        for pic in chunk:
                            if 'mp4' not in pic:
                                media_data = requests.get(pic).content
                                media.append(InputMediaPhoto(media_data))
                        # 2. Check if this is the LAST chunk
                        if i == len(picture_chunks) - 1:
                            # Send the last chunk WITH the caption
                            sent_messages = await bot.send_media_group(
                                chat_id=chat_id,
                                reply_to_message_id=reply_id,
                                media=media,
                                caption=comment_text,
                                parse_mode=ParseMode.MARKDOWN_V2,
                                disable_notification=True
                            )
                            # 3. Store ONLY the first message object so .message_id works later
                            comment_id_to_message_id[comment['id']] = sent_messages[0]
                        else:
                            # Send intermediate chunks WITHOUT caption
                            await bot.send_media_group(
                                chat_id=chat_id,
                                reply_to_message_id=reply_id,
                                media=media,
                                disable_notification=True
                            )
                elif comment.get('audio_url', ''):
                    await bot.send_chat_action(
                        chat_id=chat_id,
                        action=ChatAction.RECORD_VOICE
                    )
                    # Download audio
                    r = requests.get(comment['audio_url'])
                    audio_bytes = r.content

                    # Convert to Ogg/Opus
                    ogg_bytes = convert_to_ogg_opus_pipe(audio_bytes)
                    comment_id_to_message_id[comment['id']] = await bot.send_voice(
                        chat_id=chat_id,
                        voice=ogg_bytes,
                        reply_to_message_id=reply_id,
                        caption=comment_text,
                        parse_mode=ParseMode.MARKDOWN_V2,
                        disable_notification=True
                    )
                else:
                    await bot.send_chat_action(
                        chat_id=chat_id,
                        action=ChatAction.TYPING
                    )
                    comment_id_to_message_id[comment['id']] = await bot.send_message(
                        chat_id=chat_id,
                        reply_to_message_id=reply_id,
                        text=comment_text,
                        parse_mode=ParseMode.MARKDOWN_V2,
                        disable_web_page_preview=True,
                        disable_notification=True
                    )

def get_redirected_url(url: str) -> str:
    return unquote(requests.get(url if 'http' in url else f'http://{url}').url.split("redirectPath=")[-1])

def get_clean_url(url: str) -> str:
    return urljoin(url, urlparse(url).path)

def get_time_emoji(timestamp: int) -> str:
    a = int(((timestamp + 8 * 3600) / 900 - 3) / 2 % 24)
    return f'{chr(128336 + a // 2 + a % 2 * 12)}'

def convert_timestamp_to_timestr(timestamp: int) -> str:
    utc_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    utc_plus_8 = utc_dt + timedelta(hours=8)
    return utc_plus_8.strftime('%Y-%m-%d %H:%M:%S')

def remove_image_url_params(url: str) -> str:
    for k, v in parse_qs(url).items():
        url = url.replace(f'&{k}={v[0]}', '')
    return url

def tg_msg_escape_html(t: str) -> str:
    return t.replace('<', '&lt;')\
        .replace('>','&gt;')\
        .replace('&', '&amp;')

def tg_msg_escape_markdown_v2(t: str | int) -> str:
    t = str(t)
    for i in ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
        t = t.replace(i, "\\" + i)
    return t

def make_block_quotation(text: str) -> str:
    lines = [f'>{tg_msg_escape_markdown_v2(line)}' for line in text.split('\n') if len(line) > 0 and bool(re.findall(r'\S+', line))]
    if len(lines) > 3:
        lines[0] = f'**{lines[0]}'
        lines[-1] = f'{lines[-1]}||'
    return '\n'.join(lines)

def open_note(noteId: str, anchorCommentId: str | None = None) -> dict[str, Any] | None:
    try:
        return requests.get(f'https://{FLASK_SERVER_NAME}/open_note/{noteId}' + (f"?anchorCommentId={anchorCommentId}" if anchorCommentId else '')).json()
    except:
        return None

def get_url_info(message_text: str) -> dict[str, str | bool]:
    xsec_token = ''
    urls = re.findall(URL_REGEX, message_text)
    bot_logger.info(f'URLs:\n{urls}')
    anchorCommentId = ''
    if len(urls) == 0:
        bot_logger.debug("NO URL FOUND!")
        return {'success': False, 'msg': 'No URL found in the message.', 'noteId': '', 'xsec_token': '', 'anchorCommentId': ''}
    elif re.findall(r"[a-z0-9]{24}", message_text) and not re.findall(r"user/profile/[a-z0-9]{24}", message_text):
        noteId = re.findall(r"[a-z0-9]{24}", message_text)[0]
        note_url = [u for u in urls if re.findall(r"[a-z0-9]{24}", u) and not re.findall(r"user/profile/[a-z0-9]{24}", u)][0]
        parsed_url = urlparse(str(note_url))
        if 'xsec_token' in parse_qs(parsed_url.query):
            xsec_token = parse_qs(parsed_url.query)['xsec_token'][0]
        if 'anchorCommentId' in parse_qs(parsed_url.query):
            anchorCommentId = parse_qs(parsed_url.query)['anchorCommentId'][0]
    elif 'xhslink.com' in message_text or 'xiaohongshu.com' in message_text:
        xhslink = [u for u in urls if 'xhslink.com' in u][0]
        bot_logger.debug(f"URL found: {xhslink}")
        redirectPath = get_redirected_url(xhslink)
        bot_logger.debug(f"Redirected URL: {redirectPath}")
        if re.findall(r"https?://(?:www.)?xhslink.com/[a-z]/[A-Za-z0-9]+", xhslink):
            clean_url = get_clean_url(redirectPath)
            if 'xiaohongshu.com/404' in redirectPath or 'xiaohongshu.com/login' in redirectPath:
                noteId = re.findall(r"noteId=([a-z0-9]+)", redirectPath)[0]
                if 'redirectPath=' in redirectPath:
                    redirectPath = unquote(redirectPath.replace('https://www.xiaohongshu.com/login?redirectPath=', '').replace('https://www.xiaohongshu.com/404?redirectPath=', '').replace('https://www.xiaohongshu.com/login?redirectPath=', ''))
            else:
                noteId = re.findall(r"https?:\/\/(?:www.)?xiaohongshu.com\/discovery\/item\/([a-z0-9]+)", clean_url)[0]
            parsed_url = urlparse(str(redirectPath))
            if 'xsec_token' in parse_qs(parsed_url.query):
                xsec_token = parse_qs(parsed_url.query)['xsec_token'][0]
            if 'anchorCommentId' in parse_qs(parsed_url.query):
                anchorCommentId = parse_qs(parsed_url.query)['anchorCommentId'][0]
        elif re.findall(r"https?:\/\/(?:www.)?xiaohongshu.com\/discovery\/item\/[0-9a-z]+", xhslink):
            noteId = re.findall(r"https?:\/\/(?:www.)?xiaohongshu.com\/discovery\/item\/([a-z0-9]+)", xhslink)[0]
            parsed_url = urlparse(str(xhslink))
            if 'xsec_token' in parse_qs(parsed_url.query):
                xsec_token = parse_qs(parsed_url.query)['xsec_token'][0]
            if 'anchorCommentId' in parse_qs(parsed_url.query):
                anchorCommentId = parse_qs(parsed_url.query)['anchorCommentId'][0]
        elif re.findall(r"https?://(?:www.)?xiaohongshu.com/explore/[a-z0-9]+", message_text):
            noteId = re.findall(r"https?:\/\/(?:www.)?xiaohongshu.com\/explore\/([a-z0-9]+)", xhslink)[0]
            parsed_url = urlparse(str(xhslink))
            if 'xsec_token' in parse_qs(parsed_url.query):
                xsec_token = parse_qs(parsed_url.query)['xsec_token'][0]
            if 'anchorCommentId' in parse_qs(parsed_url.query):
                anchorCommentId = parse_qs(parsed_url.query)['anchorCommentId'][0]
        else:
            return {'success': False, 'msg': 'Invalid URL or the note is no longer available.', 'noteId': '', 'xsec_token': ''}
    else:
        return {'success': False, 'msg': 'Invalid URL.', 'noteId': '', 'xsec_token': ''}
    return {'success': True, 'msg': 'Success.', 'noteId': noteId, 'xsec_token': xsec_token, 'anchorCommentId': anchorCommentId}

def parse_comment(comment_data: dict[str, Any]):
    target_comment = comment_data.get('target_comment', {})
    user = comment_data.get('user', {})
    content = comment_data.get('content', '')
    content = re.sub(
        r'(?P<tag>#\S+?)\[\S+\]#',
        r'\g<tag> ',
        content
    )
    pictures = comment_data.get('pictures', [])
    picture_urls: list[str] = []
    for p in pictures:
        original_url = p.get('origin_url', '')
        if 'video_info' in p:
            video_info = p.get('video_info', '')
            if video_info:
                video_data = json.loads(video_info)
                for stream in video_data['stream']:
                    if video_data['stream'][stream]:
                        if 'backup_urls' in video_data['stream'][stream][0]:
                            video_url = video_data['stream'][stream][0]['backup_urls'][0]
                            picture_urls.append(video_url)
        picture_urls.append(re.sub(r'sns-note-i\d.xhscdn.com', 'sns-na-i6.xhscdn.com', original_url).split('?imageView')[0] + '?imageView2/2/w/5000/h/5000/format/webp/q/56&redImage/frame/0')
    audio_info = comment_data.get('audio_info', '')
    audio_url = ''
    if audio_info:
        audio_data = audio_info.get('play_info', {})
        if audio_data:
            audio_url = audio_data.get('url', '')
    id = comment_data.get('id', '')
    time = comment_data.get('time', 0)
    like_count = comment_data.get('like_count', 0)
    sub_comment_count = comment_data.get('sub_comment_count', 0)
    ip_location = comment_data.get('ip_location', '?')
    data: dict[str, Any] = {
        'user': user,
        'content': content,
        'pictures': picture_urls,
        'id': id,
        'time': time,
        'like_count': like_count,
        'sub_comment_count': sub_comment_count,
        'ip_location': ip_location,
        'audio_url': audio_url,
    }
    if target_comment:
        data['target_comment'] = target_comment
    return data

def extract_anchor_comment_id(json_data: dict[str, Any]) -> list[dict[str, Any]]:
    comments = json_data.get('comments', [])
    if not comments:
        bot_logger.error("No comments found in the data.")
        bot_logger.error(f"JSON data: {pformat(json_data)}")
        return []
    comment = comments[0]
    sub_comments = comment.get('sub_comments', [])
    related_sub_comments: list[dict[str, Any]] = []
    if 'page_context' in json_data:
        page_context = json_data.get('page_context', '')
        if page_context:    
            page_context = json.loads(page_context)
            key_comments_id = page_context.get('top', [])
            for key in key_comments_id:
                for sub_comment in sub_comments:
                    if sub_comment.get('id', '') == key:
                        related_sub_comments.append(sub_comment)
    all_comments = [comment] + related_sub_comments

    data_parsed: list[dict[str, Any]] = []

    for c in all_comments:
        parsed_comment = parse_comment(c)
        data_parsed.append(parsed_comment)
    return data_parsed

def extract_all_comments(json_data: dict[str, Any]) -> list[dict[str, Any]]:
    comments = json_data.get('comments', [])
    if not comments:
        bot_logger.error("No comments found in the data.")
        bot_logger.error(f"JSON data: {pformat(json_data)}")
        return []

    data_parsed: list[dict[str, Any]] = []

    for comment in comments:
        parsed_comment = parse_comment(comment)
        sub_comments: list[dict[str, Any]] = []
        for sub_comment in comment.get('sub_comments', []):
            parsed_sub_comment = parse_comment(sub_comment)
            sub_comments.append(parsed_sub_comment)
        parsed_comment["sub_comments"] = sub_comments
        data_parsed.append(parsed_comment)
    return data_parsed

def convert_to_ogg_opus_pipe(input_bytes: bytes) -> bytes:
    process = subprocess.Popen(
        [
            "ffmpeg", "-i", "pipe:0",
            "-c:a", "libopus",
            "-f", "ogg",
            "pipe:1"
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    out, _ = process.communicate(input_bytes)
    return out

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    chat = update.effective_chat
    
    if not await is_user_whitelisted(user_id, context.bot):
        bot_logger.warning(f"Unauthorized access attempt from user {user_id}")
        if chat:
            try:
                await context.bot.send_message(
                    chat_id=chat.id,
                    text=f"Sorry, you are not authorized to use this bot. If you wish to gain access, please contact the bot administrator.",
                )
            except Exception as e:
                bot_logger.error(f"Failed to send unauthorized message: {e}")
        return
    
    if chat:
        try:
            await context.bot.send_message(chat_id=chat.id, text="I'm xhsfwbot, please send me a xhs link!\n/help for more info.")
            update_network_status(success=True)
        except Exception as e:
            bot_logger.error(f"Failed to send start message: {e}")
            update_network_status(success=False)

async def help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else None
    bot_logger.debug(f"Help requested by user {user_id}")
    chat = update.effective_chat
    
    if not await is_user_whitelisted(user_id, context.bot):
        bot_logger.warning(f"Unauthorized access attempt from user {user_id}")
        if chat:
            try:
                await context.bot.send_message(
                    chat_id=chat.id,
                    text=f"Sorry, you are not authorized to use this bot. If you wish to gain access, please contact the bot administrator.",
                )
            except Exception as e:
                bot_logger.error(f"Failed to send unauthorized message: {e}")
        return
    
    if chat:
        try:
            # Forward the specified help message from the private channel
            if private_channel_id and help_message_id:
                await context.bot.forward_message(
                    chat_id=chat.id,
                    from_chat_id=private_channel_id,
                    message_id=help_message_id,
                    disable_notification=True
                )
                update_network_status(success=True)
        except Exception as e:
            bot_logger.error(f"Failed to send help message: {e}")
            update_network_status(success=False)

async def handle_message_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle message reactions for AI summary trigger"""
    bot_logger.debug(f"Received reaction update: {update}")
    reaction = update.message_reaction
    if not reaction:
        bot_logger.debug("No message_reaction in update")
        return
    
    user_id = update.effective_user.id if update.effective_user else None
    bot_logger.debug(f"Reaction from user {user_id}: {reaction}")
    
    # Check whitelist
    if not await is_user_whitelisted(user_id, context.bot):
        bot_logger.warning(f"Unauthorized reaction from user {user_id}")
        return
    
    # Check if the reaction includes 🤔 (thinking emoji)
    new_reactions = reaction.new_reaction
    if not new_reactions:
        bot_logger.debug("No new reactions")
        return
    
    bot_logger.debug(f"New reactions: {new_reactions}")
    has_thinking_emoji = any(
        r.emoji == "🤔" for r in new_reactions if hasattr(r, 'emoji')
    )
    
    if not has_thinking_emoji:
        bot_logger.debug("No thinking emoji found in reactions")
        return
    
    bot_logger.info(f"User {user_id} reacted with 🤔 to message {reaction.message_id}")
    
    # Get message details
    chat_id = reaction.chat.id
    message_id = reaction.message_id
    msg_identifier = f"{chat_id}.{message_id}"
    
    # Check if we have data for this message (verifies it's a bot message we processed)
    msg_file_path = os.path.join('data', f'{msg_identifier}.json')
    if not os.path.exists(msg_file_path):
        bot_logger.debug(f"No data file found for message {msg_identifier}, ignoring reaction (not a bot-parsed message)")
        return
    
    # React with 👾 (Alien Monster emoji)
    try:
        await context.bot.set_message_reaction(
            chat_id=chat_id,
            message_id=message_id,
            reaction="👾"
        )
        bot_logger.info(f"Reacted with 👾 to message {message_id}")
    except Exception as e:
        bot_logger.error(f"Failed to set 👾 reaction: {e}")
    
    # Send typing action
    try:
        await context.bot.send_chat_action(
            chat_id=chat_id,
            action=ChatAction.TYPING
        )
    except Exception as e:
        bot_logger.debug(f"Failed to send typing action: {e}")
    
    # Send initial AI summary message
    try:
        ai_msg = await context.bot.send_message(
            chat_id=chat_id,
            reply_to_message_id=message_id,
            text=f"*{tg_msg_escape_markdown_v2('✨ AI Summary:')}*\n```\n{tg_msg_escape_markdown_v2('Loading...')}```",
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_notification=True
        )
    except Exception as e:
        bot_logger.error(f"Failed to send AI summary message: {e}")
        return
    
    # Read the stored message data (we already verified it exists above)
    try:
        with open(msg_file_path, 'r', encoding='utf-8') as f:
            msg_data = json.load(f)
            note_content = msg_data.get('content', '')
            media_data = msg_data.get('media', [])
    except Exception as e:
        bot_logger.error(f"Failed to read message data: {e}")
        await asyncio.sleep(0.5)
        await ai_msg.edit_text(
            text=f"*{tg_msg_escape_markdown_v2('✨ AI Summary:')}*\n```\n{tg_msg_escape_markdown_v2('Error: Failed to load note data.')}```",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    
    if not note_content:
        await asyncio.sleep(0.5)
        await ai_msg.edit_text(
            text=f"*{tg_msg_escape_markdown_v2('✨ AI Summary:')}*\n```\n{tg_msg_escape_markdown_v2('Error: Note content is empty.')}```",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    
    # Generate AI summary
    try:
        await asyncio.sleep(0.5)
        await ai_msg.edit_text(
            text=f"*{tg_msg_escape_markdown_v2('✨ AI Summary:')}*\n```\n{tg_msg_escape_markdown_v2('Gathering note data...')}```",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
        content_length = max(100, min(200, len(note_content)//2))
        llm_query = f'''以下是一篇小红书笔记的完整内容。请先判断笔记本体的性质，再据此确定总结的语气与取向。

【语气判断规则】
1. 若笔记或评论呈现明显槽点、反差、搞笑情节、离谱行为、过度矫情、自我矛盾或"废物行为"（包括但不限于巨婴操作、反智自信、嘴硬硬撑、生活不自理等），可适度使用克制的幽默与轻度吐槽，但仅针对行为本身。
2. 若槽点主体属于弱势群体（如老人、残障人士、认知障碍者等），即使行为可吐槽，也仅作客观、温和的事实描述。
3. 若内容正常、信息性强、无槽点，则保持中立、简洁的分析风格。

【多媒体处理原则】
1. 若图片或视频对理解核心内容或槽点至关重要，则进行必要的简要概述。
2. 若多媒体未提供新增信息，则直接忽略，不输出任何相关说明。

【总结要求（需按顺序执行）】
1. 完整概括笔记本体内容
   - 必须体现主要内容、核心观点或意图。
   - 如有槽点或亮点，可酌情补充。
   - 若标签无实际信息或亮点，则不予概括。

2. 单独概括评论区内容（如存在）
   - 包括态度、补充信息、槽点或吐槽点。
   - 不能以评论区代替笔记本体总结。

3. 多媒体仅在必要时简要说明，不得机械复述画面。

4. 语言自然、流畅。  
5. 禁止输出无关内容。  
6. 直接输出正文，无标题、无前后缀、无 Markdown。  
7. 使用简体中文。
8. 字数尽量不超过 {content_length}，若超出则在确保内容完整前提下尽量接近该限制。

笔记内容如下：
{note_content}'''
        
        bot_logger.debug(f"LLM Query:\\n{llm_query}")
        
        contents: list[types.Part] = [types.Part(text=llm_query)]
        
        await asyncio.sleep(0.5)
        await ai_msg.edit_text(
            text=f"*{tg_msg_escape_markdown_v2('✨ AI Summary:')}*\n```\n{tg_msg_escape_markdown_v2('Downloading media...')}```",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
        # Download media and convert to Gemini Part
        for media in media_data:
            if media.get('type', '') == 'image' and 'url' in media:
                media_url = media['url']
                media_bytes = requests.get(media_url).content
                
                # Compress image to 720p before uploading to Gemini
                try:
                    img = Image.open(BytesIO(media_bytes))
                    # Calculate new size while maintaining aspect ratio
                    width, height = img.size
                    if height > 720 or width > 1280:
                        if height > width:
                            new_height = 720
                            new_width = int(width * (720 / height))
                        else:
                            new_width = 1280
                            new_height = int(height * (1280 / width))
                        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                    
                    # Convert to JPEG and compress
                    output = BytesIO()
                    img.convert('RGB').save(output, format='JPEG', quality=70, optimize=True)
                    media_bytes = output.getvalue()
                    bot_logger.debug(f"Compressed image from {width}x{height} to {img.size[0]}x{img.size[1]}")
                except Exception as e:
                    bot_logger.warning(f"Failed to compress image, using original: {e}")
                
                image_part = types.Part.from_bytes(
                    data=media_bytes,
                    mime_type="image/jpeg"
                )
                contents.append(image_part)
        
        bot_logger.info(f"Generating summary with content length limit: {content_length}")
        await asyncio.sleep(0.5)
        await ai_msg.edit_text(
            text=f"*{tg_msg_escape_markdown_v2('✨ AI Summary:')}*\n```\n{tg_msg_escape_markdown_v2('Generating summary...')}```",
            parse_mode=ParseMode.MARKDOWN_V2
        )
        
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=types.Content(parts=contents)
        )
        text = response.text
        bot_logger.info(f"Generated summary:\\n{text}")
        
        if not response or not text:
            bot_logger.error("No response from Gemini API")
            await asyncio.sleep(0.5)
            await ai_msg.edit_text(
                text=f"*{tg_msg_escape_markdown_v2('✨ AI Summary:')}*\n```\n{tg_msg_escape_markdown_v2('Error: No response from AI service.')}```",
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return
        
        await asyncio.sleep(0.5)
        await ai_msg.edit_text(
            text=f"*{tg_msg_escape_markdown_v2('✨ AI Summary:')}*\n```Note\n{tg_msg_escape_markdown_v2(text)}```",
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        bot_logger.error(f"Error generating AI summary: {e}\\n{traceback.format_exc()}")
        try:
            await asyncio.sleep(0.5)
            await ai_msg.edit_text(
                text=f"*{tg_msg_escape_markdown_v2('✨ AI Summary:')}*\n```\n{tg_msg_escape_markdown_v2(f'Error: {str(e)}')}```",
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except:
            pass

async def AI_summary_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks for generating more info"""
    query = update.callback_query
    if not query:
        return
    
    user_id = update.effective_user.id if update.effective_user else None
    
    # Check whitelist
    if not await is_user_whitelisted(user_id, context.bot):
        bot_logger.warning(f"Unauthorized callback attempt from user {user_id}")
        # await query.answer(f"Sorry, you are not authorized to use this feature. If you wish to gain access, please contact the bot administrator.", show_alert=True)
        return

    await query.answer()
    
    # Parse callback data: "more_info:{noteId}:{message_id}"
    callback_data = query.data
    if not callback_data or not callback_data.startswith("summary:"):
        return
    
    try:
        noteId, msg_identifier = callback_data.split(":")
        bot_logger.info(f"Button clicked: message {msg_identifier} for note {noteId}")
        
        chat_id = query.message.chat.id if query.message else None
        bot_logger.debug(f"Chat ID: {chat_id}")
        if not chat_id:
            return
        
        # Add delay to avoid flood control
        await asyncio.sleep(1)
        
        # Send a typing action
        await context.bot.send_chat_action(
            chat_id=chat_id,
            action=ChatAction.TYPING
        )
        
        # Add delay before editing message
        await asyncio.sleep(0.5)
        
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=int(msg_identifier.split(".")[-1]),
            reply_markup=None
        )
        
        # Add delay before sending new message
        await asyncio.sleep(0.5)
        ai_msg = await context.bot.send_message(
            chat_id=chat_id,
            reply_to_message_id=int(msg_identifier.split(".")[-1]),
            text=f"*_{tg_msg_escape_markdown_v2('✨ AI Summary:\n')}_*```\n{tg_msg_escape_markdown_v2('Loading...')}```",
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_notification=True
        )
        
        # Read the stored message string if available
        note_content = ''
        media_data: list[dict[str, str]] = []
        msg_file_path = os.path.join('data', f'{msg_identifier}.json')
        if os.path.exists(msg_file_path):
            with open(msg_file_path, 'r', encoding='utf-8') as f:
                msg_data = json.load(f)
                note_content = msg_data.get('content', '')
                media_data = msg_data.get('media', [])
                f.close()
            # Delete file after reading
            os.remove(msg_file_path)
        if not note_content or not media_data:
            return
        await ai_msg.edit_text(
            text=f"*_{tg_msg_escape_markdown_v2('✨ AI Summary:\n')}_*```\n{tg_msg_escape_markdown_v2('Gathering note data...')}```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )

        content_length = max(100, min(200, len(note_content)//2))
        llm_query: str = f'''以下是一篇小红书笔记的完整内容。请先判断笔记本体的性质，再据此确定总结的语气与取向。

【语气判断规则】
1. 若笔记或评论呈现明显槽点、反差、搞笑情节、离谱行为、过度矫情、自我矛盾或“废物行为”（包括但不限于巨婴操作、反智自信、嘴硬硬撑、生活不自理等），可适度使用克制的幽默与轻度吐槽，但仅针对行为本身。
2. 若槽点主体属于弱势群体（如老人、残障人士、认知障碍者等），即使行为可吐槽，也仅作客观、温和的事实描述。
3. 若内容正常、信息性强、无槽点，则保持中立、简洁的分析风格。

【多媒体处理原则】
1. 若图片或视频对理解核心内容或槽点至关重要，则进行必要的简要概述。
2. 若多媒体未提供新增信息，则直接忽略，不输出任何相关说明。

【总结要求（需按顺序执行）】
1. 完整概括笔记本体内容
   - 必须体现主要内容、核心观点或意图。
   - 如有槽点或亮点，可酌情补充。
   - 若标签无实际信息或亮点，则不予概括。

2. 单独概括评论区内容（如存在）
   - 包括态度、补充信息、槽点或吐槽点。
   - 不能以评论区代替笔记本体总结。

3. 多媒体仅在必要时简要说明，不得机械复述画面。

4. 语言自然、流畅。  
5. 禁止输出无关内容。  
6. 直接输出正文，无标题、无前后缀、无 Markdown。  
7. 使用简体中文。
8. 字数尽量不超过 {content_length}，若超出则在确保内容完整前提下尽量接近该限制。

笔记内容如下：
{note_content}'''
        bot_logger.debug(f"LLM Query:\n{llm_query}")

        contents: list[types.Part] = [
            types.Part(text=llm_query)
        ]
        await ai_msg.edit_text(
            text=f"*_{tg_msg_escape_markdown_v2('✨ AI Summary:\n')}_*```\n{tg_msg_escape_markdown_v2('Downloading media...')}```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        # download media and convert to Gemini Part
        for media in media_data:
            if media.get('type', '') == 'image' and 'url' in media:
                media_url = media['url']
                media_bytes = requests.get(media_url).content
                
                # Compress image to 720p before uploading to Gemini
                try:
                    img = Image.open(BytesIO(media_bytes))
                    # Calculate new size while maintaining aspect ratio
                    width, height = img.size
                    if height > 720 or width > 1280:
                        if height > width:
                            new_height = 720
                            new_width = int(width * (720 / height))
                        else:
                            new_width = 1280
                            new_height = int(height * (1280 / width))
                        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                    
                    # Convert to JPEG and compress
                    output = BytesIO()
                    img.convert('RGB').save(output, format='JPEG', quality=70, optimize=True)
                    media_bytes = output.getvalue()
                    bot_logger.debug(f"Compressed image from {width}x{height} to {img.size[0]}x{img.size[1]}")
                except Exception as e:
                    bot_logger.warning(f"Failed to compress image, using original: {e}")
                
                image_part = types.Part.from_bytes(
                    data=media_bytes,
                    mime_type="image/jpeg"
                )
                contents.append(image_part)

        bot_logger.info(f"Generating summary with content length limit: {content_length}")
        await ai_msg.edit_text(
            text=f"*_{tg_msg_escape_markdown_v2('✨ AI Summary:\n')}_*```\n{tg_msg_escape_markdown_v2('Generating summary...')}```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=types.Content(parts=contents),
        )
        text = response.text
        bot_logger.info(f"Generated summary for note {noteId}:\n{text}")
        if not response or not text:
            bot_logger.error("No response from Gemini API")
            return
        await ai_msg.edit_text(
            text=f"*_{tg_msg_escape_markdown_v2('✨ AI Summary:\n')}_*```Note\n{tg_msg_escape_markdown_v2(text)}```",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    except Exception as e:
        bot_logger.error(f"Error in button callback: {e}\n{traceback.format_exc()}")
        if query.message:
            await context.bot.send_message(
                chat_id=query.message.chat.id,
                text=f"An error occurred while processing your request:\n```python\n{tg_msg_escape_markdown_v2(str(e))}\n```",
                parse_mode=ParseMode.MARKDOWN_V2,
                disable_notification=True
            )

async def process_note_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process a single note request with concurrency control"""
    user_id = update.effective_user.id if update.effective_user else "unknown"
    chat_id = update.effective_chat.id if update.effective_chat else "unknown"
    
    # Log when we're waiting for semaphore
    available_slots = processing_semaphore._value
    bot_logger.debug(f"Processing request from user {user_id} in chat {chat_id}. Available slots: {available_slots}")
    
    async with processing_semaphore:
        bot_logger.debug(f"Started concurrent processing for user {user_id}")
        try:
            await _note2feed_internal(update, context)
        except Exception as e:
            bot_logger.error(f"Error in concurrent processing for user {user_id}: {e}")
        finally:
            bot_logger.debug(f"Finished concurrent processing for user {user_id}")

async def note2feed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main handler that creates concurrent tasks for note processing"""
    asyncio.create_task(process_note_request(update, context))

async def _note2feed_internal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Internal note processing function"""
    user_id = update.effective_user.id if update.effective_user else None
    
    chat = update.effective_chat
    if not chat:
        return
    msg = update.message
    if not msg:
        return
    
    # Get message text, handling both regular messages and command arguments
    message_text = msg.text or ''
    if msg.caption:
        message_text += f" {msg.caption}"
    
    # For /note command, get the URL from command arguments
    if context.args:
        message_text += ' ' + ' '.join(context.args)
        bot_logger.debug(f"Command args: {context.args}, combined text: {message_text}")
    
    if 'xhslink.com' not in message_text and 'xiaohongshu.com' not in message_text and not msg.photo:
        bot_logger.debug(f"No XHS link found in message: {message_text[:100]}")
        return
    
    # React with 'OK' gesture when receiving user's message
    try:
        await msg.set_reaction("👌")
    except Exception as e:
        bot_logger.debug(f"Failed to set OK reaction: {e}")
    
    await context.bot.send_chat_action(
        chat_id=chat.id,
        action=ChatAction.TYPING
    )

    # Check whitelist
    if not await is_user_whitelisted(user_id, context.bot):
        bot_logger.warning(f"Unauthorized access attempt from user {user_id}")
        with_xsec_token = bool(re.search(r"[^\S]+-x(?!\S)", message_text))
        url_info = get_url_info(message_text)
        if not url_info['success']:
            return
        noteId = str(url_info['noteId'])
        xsec_token = str(url_info['xsec_token'])
        bot_logger.info(f'Note ID: {noteId}, xsec_token: {xsec_token if xsec_token else "None"}')
        msg = update.message
        if msg and chat:
            try:
                await context.bot.send_message(
                    chat_id=chat.id,
                    text=f"Clean URL:\nhttps://www.xiaohongshu.com/discovery/item/{noteId}{f'?xsec_token={xsec_token}' if xsec_token and with_xsec_token else ''}",
                    reply_to_message_id=msg.message_id
                )
            except Exception as e:
                bot_logger.error(f"Failed to send unauthorized message: {e}")
        return
    # If there is a photo, try to decode QR code
    if msg.photo:
        try:
            # Get the lowest resolution photo
            photo_file = await msg.photo[-1].get_file()
            
            # Download to memory
            img_byte_arr: BytesIO = BytesIO()
            await photo_file.download_to_memory(img_byte_arr)
            img_byte_arr.seek(0)

            # Decode QR code
            image = Image.open(img_byte_arr)
            decoded_objects: list[Any] = decode(image) # pyright: ignore[reportUnknownVariableType]

            for obj in decoded_objects:
                if obj.type == 'QRCODE':
                    qr_data = obj.data.decode("utf-8")
                    bot_logger.info(f"QR Code detected: {qr_data}")
                    # Append decoded URL to message text so it gets processed
                    message_text += f" {qr_data} "
        except Exception as e:
            bot_logger.error(f"Failed to decode QR code: {e}")

    with_xsec_token = bool(re.search(r"[^\S]+-x(?!\S)", message_text))
    url_info = get_url_info(message_text)
    if not url_info['success']:
        return
    noteId = str(url_info['noteId'])
    xsec_token = str(url_info['xsec_token'])
    anchorCommentId = str(url_info['anchorCommentId'])
    bot_logger.info(f'Note ID: {noteId}, xsec_token: {xsec_token if xsec_token else "None"}, anchorCommentId: {anchorCommentId if anchorCommentId else "None"}')

    bot_logger.debug('try open note on device')
    open_note(noteId, anchorCommentId=anchorCommentId)
    await asyncio.sleep(1.5)

    note_data: dict[str, Any] = {}
    comment_list_data: dict[str, Any] = {'data': {}}

    try:
        note_data = requests.get(
            f"https://{FLASK_SERVER_NAME}/get_note/{noteId}"
        ).json()
        with open(os.path.join("data", f"note_data-{noteId}.json"), "w", encoding='utf-8') as f:
            json.dump(note_data, f, indent=4, ensure_ascii=False)
            f.close()
        times = 0
        while True:
            times += 1
            try:
                comment_list_data = requests.get(
                    f"https://{FLASK_SERVER_NAME}/get_comment_list/{noteId}"
                ).json()
                with open(os.path.join("data", f"comment_list_data-{noteId}.json"), "w", encoding='utf-8') as f:
                    json.dump(comment_list_data, f, indent=4, ensure_ascii=False)
                    f.close()
                bot_logger.debug('got comment list data')
                break
            except:
                if times <= 3:
                    await asyncio.sleep(0.1)
                else:
                    raise Exception('error when getting comment list data')
    except:
        bot_logger.error(traceback.format_exc())
    finally:
        if not note_data or 'data' not in note_data:
            # React with tear emoji if note data is not available
            try:
                await msg.set_reaction("😢")
            except Exception as e:
                bot_logger.debug(f"Failed to set tear reaction: {e}")
            return
    if note_data['data']['data'][0]['note_list'][0]['model_type'] == 'error':
        bot_logger.warning(f'Note data not available\n{note_data['data']}')
        # React with tear emoji if note model type is error
        try:
            await msg.set_reaction("😢")
        except Exception as e:
            bot_logger.debug(f"Failed to set tear reaction: {e}")
        return

    try:
        try:
            await telegraph_account.get_account_info()  # type: ignore
        except:
            await telegraph_account.create_account( # type: ignore
                short_name='@xhsfwbot',
            )
        note = Note(
            note_data['data'],
            comment_list_data=comment_list_data['data'],
            live=True,
            telegraph=True,
            with_xsec_token=with_xsec_token,
            original_xsec_token=xsec_token,
            telegraph_account=telegraph_account,
            anchorCommentId=anchorCommentId
        )
        await note.initialize()
        try:
            # reply with rich text when sending media failed
            await context.bot.send_chat_action(
                chat_id=chat.id,
                action=ChatAction.TYPING
            )
            if not note.telegraph_url:
                await note.to_telegraph()
            telegraph_msg = await context.bot.send_message(
                chat_id = chat.id,
                text = f"📕 [{tg_msg_escape_markdown_v2(note.title)}]({note.url})\n{f"\n{tg_msg_escape_markdown_v2(note.tag_string)}" if note.tags else ""}\n\n👤 [@{tg_msg_escape_markdown_v2(note.user['name'])}](https://www.xiaohongshu.com/user/profile/{note.user['id']})\n\n📰 [View via Telegraph]({note.telegraph_url})",
                parse_mode=ParseMode.MARKDOWN_V2,
                reply_to_message_id=msg.message_id,
                disable_notification=True,
                link_preview_options=LinkPreviewOptions(
                    is_disabled=False,
                    url=note.telegraph_url,
                    prefer_small_media=True,
                    show_above_text=False,
                )
            )
            
            # Store note data for telegraph message to enable AI summary on reaction
            try:
                telegraph_msg_identifier = f"{chat.id}.{telegraph_msg.message_id}"
                msg_data = {
                    'content': str(note),
                    'media': note.media_for_llm()
                }
                telegraph_msg_file_path = os.path.join('data', f'{telegraph_msg_identifier}.json')
                with open(telegraph_msg_file_path, 'w', encoding='utf-8') as f:
                    json.dump(msg_data, f, ensure_ascii=False, indent=2)
                bot_logger.debug(f"Saved telegraph message data to {telegraph_msg_file_path} for AI summary")
            except Exception as e:
                bot_logger.error(f"Failed to save telegraph message data: {e}")
            
            await note.send_as_telegram_message(context.bot, chat.id, msg.message_id)
            if telegraph_msg:
                await telegraph_msg.delete()
            update_network_status(success=True)  # Successfully sent message
            
            # Delete user's original message after successful parsing
            try:
                await msg.delete()
                bot_logger.debug("Deleted user's original message after successful processing")
            except Exception as e:
                bot_logger.debug(f"Failed to delete user's message: {e}")
        except:
            bot_logger.error(traceback.format_exc())
            # React with tear emoji if message sending fails
            try:
                await msg.set_reaction("😢")
            except Exception as e:
                bot_logger.debug(f"Failed to set tear reaction: {e}")
    except Exception as e:
        bot_logger.error(f"Error in note2feed: {e}\n{traceback.format_exc()}")
        # React with tear emoji on any unhandled error
        try:
            await msg.set_reaction("😢")
        except Exception as react_err:
            bot_logger.debug(f"Failed to set tear reaction: {react_err}")
        # React with tear emoji on any unhandled error
        try:
            await msg.set_reaction("😢")
        except Exception as react_err:
            bot_logger.debug(f"Failed to set tear reaction: {react_err}")

async def process_inline_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process a single inline query request with concurrency control"""
    user_id = update.effective_user.id if update.effective_user else "unknown"
    
    available_slots = processing_semaphore._value
    bot_logger.debug(f"Processing inline query from user {user_id}. Available slots: {available_slots}")
    
    async with processing_semaphore:
        bot_logger.debug(f"Started concurrent inline processing for user {user_id}")
        try:
            await _inline_note2feed_internal(update, context)
        except Exception as e:
            bot_logger.error(f"Error in concurrent inline processing for user {user_id}: {e}")
        finally:
            bot_logger.debug(f"Finished concurrent inline processing for user {user_id}")

async def inline_note2feed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main inline handler that creates concurrent tasks"""
    # For inline queries, we need to respond quickly, so we await the result
    # but still use the semaphore for rate limiting
    await process_inline_request(update, context)

async def _inline_note2feed_internal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Internal inline query processing function"""
    user_id = update.effective_user.id if update.effective_user else None
    inline_query = update.inline_query
    bot_logger.debug(inline_query)
    if inline_query is None:
        return
    message_text = inline_query.query
    if not message_text:
        return
    if 'xhslink.com' not in (message_text or '') and 'xiaohongshu.com' not in (message_text or ''):
        return
    

    with_xsec_token = bool(re.search(r"[^\S]+-x(?!\S)", message_text))
    url_info = get_url_info(message_text)
    if not url_info['success']:
        return
    noteId = str(url_info['noteId'])
    xsec_token = str(url_info['xsec_token'])
    bot_logger.info(f'Note ID: {noteId}, xsec_token: {xsec_token if xsec_token else "None"}')
    anchorCommentId = str(url_info['anchorCommentId'])
    bot_logger.info(f'Note ID: {noteId}, xsec_token: {xsec_token if xsec_token else "None"}, anchorCommentId: {anchorCommentId if anchorCommentId else "None"}')

    # Check whitelist
    if not await is_user_whitelisted(user_id, context.bot):
        bot_logger.warning(f"Unauthorized inline access attempt from user {user_id}")
        if inline_query:
            try:
                await context.bot.answer_inline_query(
                    inline_query_id=inline_query.id,
                    results=[
                        InlineQueryResultArticle(
                            id=str(uuid4()),
                            title="Clean URL",
                            input_message_content=InputTextMessageContent(
                                message_text=f"https://www.xiaohongshu.com/discovery/item/{noteId}{f'?xsec_token={xsec_token}' if xsec_token and with_xsec_token else ''}",
                            ),
                            description="Clean xiaohongshu.com URL without tracking parameters",
                        )
                    ],
                    cache_time=0
                )
            except Exception as e:
                bot_logger.error(f"Failed to respond to unauthorized inline query: {e}")
        return

    bot_logger.debug('try open note on device')
    open_note(noteId, anchorCommentId=anchorCommentId)
    await asyncio.sleep(3)

    note_data: dict[str, Any] = {}
    comment_list_data: dict[str, Any] = {'data': {}}

    try:
        note_data = requests.get(
            f"https://{FLASK_SERVER_NAME}/get_note/{noteId}"
        ).json()
        with open(os.path.join("data", f"note_data-{noteId}.json"), "w", encoding='utf-8') as f:
            json.dump(note_data, f, indent=4, ensure_ascii=False)
            f.close()
        times = 0
        while True:
            times += 1
            try:
                comment_list_data = requests.get(
                    f"https://{FLASK_SERVER_NAME}/get_comment_list/{noteId}"
                ).json()
                with open(os.path.join("data", f"comment_list_data-{noteId}.json"), "w", encoding='utf-8') as f:
                    json.dump(comment_list_data, f, indent=4, ensure_ascii=False)
                    f.close()
                bot_logger.debug('got comment list data')
                break
            except:
                if times <= 3:
                    await asyncio.sleep(0.1)
                else:
                    raise Exception('error when getting comment list data')
    except:
        bot_logger.error(traceback.format_exc())
    finally:
        if not note_data or 'data' not in note_data:
            return
    if note_data['data']['data'][0]['note_list'][0]['model_type'] == 'error':
        bot_logger.warning(f'Note data not available\n{note_data["data"]}')
        return
    try:
        try:
            await telegraph_account.get_account_info()  # type: ignore
        except:
            await telegraph_account.create_account( # type: ignore
                short_name='@xhsfwbot',
            )
        note = Note(
            note_data['data'],
            comment_list_data=comment_list_data['data'],
            live=True,
            telegraph=True,
            with_xsec_token=with_xsec_token,
            original_xsec_token=xsec_token,
            telegraph_account=telegraph_account,
            anchorCommentId=anchorCommentId
        )
        await note.initialize()
        telegraph_url = note.telegraph_url if hasattr(note, 'telegraph_url') else await note.to_telegraph()
        inline_query_result = [
            InlineQueryResultArticle(
                id=str(uuid4()),
                title=note.title,
                input_message_content=InputTextMessageContent(
                    message_text=f"📕 [{tg_msg_escape_markdown_v2(note.title)}]({note.url})\n{f"\n{tg_msg_escape_markdown_v2(note.tag_string)}" if note.tags else ""}\n\n👤 [@{tg_msg_escape_markdown_v2(note.user['name'])}](https://www.xiaohongshu.com/user/profile/{note.user['id']})\n\n📰 [View via Telegraph]({telegraph_url})",
                    parse_mode=ParseMode.MARKDOWN_V2,
                    link_preview_options=LinkPreviewOptions(
                        is_disabled=False,
                        url=telegraph_url,
                        prefer_large_media=False
                    ),
                ),
                description=f"Telegraph URL with xiaohongshu.com URL ({'with' if with_xsec_token else 'no'} xsec_token)",
                thumbnail_url=note.thumbnail
            )
        ]
        await context.bot.answer_inline_query(
            inline_query_id=inline_query.id,
            results=inline_query_result
        )
        update_network_status(success=True)
    except Exception as e:
        bot_logger.error(f"Error in inline_note2feed: {e}\n{traceback.format_exc()}")
        update_network_status(success=False)
    return

async def error_handler(update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
    global logging_file
    admin_id = os.getenv('ADMIN_ID')
    error_str = str(context.error).lower()
    
    # Check for pool timeout - this is critical and should trigger immediate restart
    if 'pool timeout' in error_str or 'all connections in the connection pool are occupied' in error_str:
        bot_logger.error(f"CRITICAL: Pool timeout detected - triggering immediate restart:\n{context.error}")
        bark_notify("xhsfwbot: Pool timeout detected, restarting immediately")
        restart_script()
        return
    
    # Check for network-related errors that should trigger restart
    network_keywords = [
        'timeout', 'connection', 'network', 
        'timed out', 'connecttimeout', 'readtimeout', 'writetimeout'
    ]
    
    if isinstance(context.error, NetworkError) or any(keyword in error_str for keyword in network_keywords):
        bot_logger.error(f"Network-related error detected:\n{context.error}\n\n{traceback.format_exc()}")
        update_network_status(success=False)
        if not is_network_healthy or not check_network_connectivity():
            bot_logger.error("Network appears unhealthy - triggering restart")
            restart_script()
        return
    elif isinstance(context.error, BadRequest):
        bot_logger.error(f"BadRequest error:\n{context.error}\n\n{traceback.format_exc()}")
        return
    elif isinstance(context.error, KeyboardInterrupt):
        os._exit(0)
        return
    else:
        if admin_id:
            try:
                await context.bot.send_document(
                    chat_id=admin_id,
                    caption=f'```python\n{tg_msg_escape_markdown_v2(pformat(update))}\n```\n CAUSED \n```python\n{tg_msg_escape_markdown_v2(pformat(context.error))[-888:]}\n```',
                    parse_mode=ParseMode.MARKDOWN_V2,
                    document=logging_file,
                    disable_notification=True
                )
                update_network_status(success=True)  # Successfully sent message
            except Exception as send_error:
                bot_logger.error(f"Failed to send error report: {send_error}")
                update_network_status(success=False)
        bot_logger.error(f"Update {update} caused error:\n{context.error}\n\n{traceback.format_exc()}")

def run_telegram_bot():
    bot_token = os.getenv('BOT_TOKEN')
    if not bot_token:
        raise ValueError("BOT_TOKEN environment variable is required")
    
    # Start network monitoring in background thread
    monitor_thread = threading.Thread(target=network_monitor, daemon=True)
    monitor_thread.start()
    
    # Start scheduled restart monitor in background thread
    restart_thread = threading.Thread(target=scheduled_restart_monitor, daemon=True)
    restart_thread.start()
    
    application = ApplicationBuilder()\
        .concurrent_updates(True)\
        .token(bot_token)\
        .read_timeout(30)\
        .write_timeout(30)\
        .media_write_timeout(120)\
        .connect_timeout(15)\
        .pool_timeout(20)\
        .connection_pool_size(16)\
        .concurrent_updates(True)\
        .build()

    bark_notify("xhsfwbot tries to start polling.")
    try:
        start_handler = CommandHandler("start", start)
        application.add_handler(start_handler)
        help_handler = CommandHandler("help", help)
        application.add_handler(help_handler)
        
        # AI summary button callback handler disabled
        # AI_summary_button_callback_handler = CallbackQueryHandler(AI_summary_button_callback)
        # application.add_handler(AI_summary_button_callback_handler)
        
        # Message reaction handler for AI summary
        reaction_handler = MessageReactionHandler(handle_message_reaction)
        application.add_handler(reaction_handler)

        application.add_error_handler(error_handler)

        note2feed_handler = MessageHandler(
            (~ filters.COMMAND) & (
                (filters.TEXT & (filters.Entity(MessageEntity.URL) | filters.Entity(MessageEntity.TEXT_LINK))) |
                filters.PHOTO
            ),
            note2feed
        )
        application.add_handler(note2feed_handler)

        note2feed_command_handler = CommandHandler(
            "note",
            note2feed,
            block=False
        )
        application.add_handler(InlineQueryHandler(inline_note2feed, block=False))
        application.add_handler(note2feed_command_handler)
        
        bot_logger.info(f'Bot started polling with concurrent processing enabled (max {max_concurrent_requests} concurrent requests)')
        
        # Run polling with allowed_updates to include message reactions
        application.run_polling(
            allowed_updates=[
                "message",
                "edited_message",
                "channel_post",
                "edited_channel_post",
                "inline_query",
                "chosen_inline_result",
                "callback_query",
                "message_reaction"
            ]
        )

    except KeyboardInterrupt:
        shutdown_result = application.shutdown()
        bot_logger.debug(f'KeyboardInterrupt received, shutdown:{shutdown_result}')
        raise Exception('KeyboardInterrupt received, script will quit now.')
    except NetworkError as e:
        bot_logger.error(f'NetworkError: {e}\n{traceback.format_exc()}')
        update_network_status(success=False)
        if not check_network_connectivity():
            bot_logger.error('Network connectivity test failed - restarting')
            restart_script()
        raise Exception('NetworkError received, script will quit now.')
    except Exception as e:
        error_str = str(e).lower()
        network_keywords = ['timeout', 'connection', 'network', 'timed out']
        
        if any(keyword in error_str for keyword in network_keywords):
            bot_logger.error(f'Network-related error in main loop: {e}\n{traceback.format_exc()}')
            update_network_status(success=False)
            if not check_network_connectivity():
                bot_logger.error('Network connectivity test failed - restarting')
                restart_script()
        else:
            bot_logger.error(f'Unexpected error:\n{traceback.format_exc()}\n\n SCRIPT WILL QUIT NOW')
        raise Exception(f'Error in main loop: {e}')

def bark_notify(message: str) -> None:
    bark_token = os.getenv('BARK_TOKEN')
    bark_key = os.getenv('BARK_KEY')  # 32-character encryption key
    bark_iv = os.getenv('BARK_IV', '472')  # IV, default to '472'
    
    if not bark_token:
        bot_logger.warning('BARK_TOKEN not set, cannot send bark notification')
        return
    
    try:
        # If encryption key is provided, send encrypted notification
        if bark_key:
            # Create JSON payload
            payload = json.dumps({
                "body": message,
                "sound": "birdsong",
                "title": "xhsfwbot"
            }, ensure_ascii=False)
            
            # Ensure key is 32 bytes (256 bits for AES-256)
            if len(bark_key) != 32:
                bot_logger.error(f'BARK_KEY must be exactly 32 characters long, got {len(bark_key)}')
                # Fall back to unencrypted
                requests.get(
                    f'https://api.day.app/{bark_token}/{quote("xhsfwbot")}/{quote(message)}'
                )
                return
            
            # Convert key to bytes
            key_bytes = bark_key.encode('utf-8')
            
            # Encrypt using AES-256-ECB with PKCS7 padding
            # ECB mode doesn't use IV, so we ignore the bark_iv parameter for encryption
            cipher = AES.new(key_bytes, AES.MODE_ECB)  # pyright: ignore[reportUnknownMemberType]
            encrypted = cipher.encrypt(pad(payload.encode('utf-8'), AES.block_size))
            
            # Base64 encode the ciphertext
            ciphertext = base64.b64encode(encrypted).decode('utf-8')
            
            # Send encrypted notification
            response = requests.post(
                f'https://api.day.app/{bark_token}',
                data={
                    'ciphertext': ciphertext,
                    'iv': bark_iv
                }
            )
            
            if response.status_code == 200:
                bot_logger.info('Encrypted Bark notification sent successfully')
            else:
                bot_logger.error(f'Failed to send encrypted bark notification: {response.status_code} {response.text}')
        else:
            # Send unencrypted notification (original behavior)
            requests.get(
                f'https://api.day.app/{bark_token}/{quote("xhsfwbot")}/{quote(message)}'
            )
            bot_logger.info('Bark notification sent successfully')
            
    except Exception as e:
        bot_logger.error(f'Failed to send bark notification: {e}\n{traceback.format_exc()}')

def restart_script():
    bot_logger.info("Restarting script...")
    # notify bot owner with bark
    bark_notify("xhsfwbot is restarting due to network issues.")
    try:
        process = psutil.Process(os.getpid())
        for handler in process.open_files() + process.net_connections():
            os.close(handler.fd)
    except Exception as e:
        bot_logger.error(f'Error when closing file descriptors: {e}\n{traceback.format_exc()}')
    python = sys.executable
    os.execl(python, python, *sys.argv)

if __name__ == "__main__":
    try:
        telegraph_account = Telegraph()
        client = genai.Client()
        run_telegram_bot()
    except Exception as e:
        restart_script()

