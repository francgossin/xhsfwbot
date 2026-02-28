import os
import sys
import re
import json
import time
import asyncio
import logging
import psutil
import requests
import traceback
import subprocess
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
from google.genai import types as genai_types

from telethon import TelegramClient, events, Button
from telethon.tl import functions, types as tl_types
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeVideo,
    InputWebDocument,
    ReactionEmoji,
    UpdateBotMessageReaction,
)
from telethon.utils import get_peer_id
from telethon.errors import (
    FloodWaitError,
    MessageNotModifiedError,
    MessageDeleteForbiddenError,
    NetworkMigrateError,
    QueryIdInvalidError,
)

from telegraph.aio import Telegraph  # type: ignore
from PIL import Image
from pyzbar.pyzbar import decode  # pyright: ignore[reportUnknownVariableType, reportMissingTypeStubs]

# ── Environment ────────────────────────────────────────────────────────────────

load_dotenv()

telegram_proxy = os.getenv('TELEGRAM_PROXY', '')
FLASK_SERVER_NAME = os.getenv('FLASK_SERVER_NAME', '127.0.0.1')

# ── Logging ────────────────────────────────────────────────────────────────────

os.makedirs("log", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging_file = os.path.join("log", f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log")

logging.basicConfig(
    handlers=[
        logging.FileHandler(filename=logging_file, encoding="utf-8", mode="w+"),
        logging.StreamHandler(),
    ],
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s: %(message)s",
    datefmt="%F %A %T",
    level=logging.INFO,
)

bot_logger = logging.getLogger("xhsfwbot")
bot_logger.setLevel(logging.INFO)

# ── Concurrency ────────────────────────────────────────────────────────────────

max_concurrent_requests = 5
processing_semaphore = asyncio.Semaphore(max_concurrent_requests)

# ── AI summary limits ─────────────────────────────────────────────────────────

# Maximum total media size (bytes) for AI summary to be available.
# If the note's video or combined photo size exceeds this, the AI summary
# hint is hidden and the 🤔 reaction is rejected.
AI_SUMMARY_MAX_MEDIA_BYTES = 8 * 1024 * 1024  # 8 MB

# ── Help config ────────────────────────────────────────────────────────────────

HELP_CONFIG_FILE = 'help_config.json'


def load_help_config() -> dict:
    if os.path.exists(HELP_CONFIG_FILE):
        try:
            with open(HELP_CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            bot_logger.error(f"Failed to load help config: {e}")
    return {}


def save_help_config(cfg: dict) -> None:
    try:
        with open(HELP_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as e:
        bot_logger.error(f"Failed to save help config: {e}")


help_config = load_help_config()

# ── Emoji / URL constants ──────────────────────────────────────────────────────

with open('redtoemoji.json', 'r', encoding='utf-8') as _f:
    redtoemoji: dict = json.load(_f)

URL_REGEX = r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:\'\".,<>?«»""''])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"""

# ── Pure-Python helpers (identical to xhsfwbot.py) ────────────────────────────

def replace_redemoji_with_emoji(text: str) -> str:
    for red_emoji, emoji in redtoemoji.items():
        text = text.replace(red_emoji, emoji)
    return text


def tg_msg_escape_html(t: str | int) -> str:
    return str(t).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def _make_progress_bar(pct: float, width: int = 20) -> str:
    """Return a text progress bar like [████████░░░░░░░░░░░░] 40%"""
    pct = max(0.0, min(1.0, pct))
    filled = int(width * pct)
    bar = '█' * filled + '░' * (width - filled)
    return f'[{bar}] {pct * 100:.0f}%'


def _build_summary_footer(
    send_as_file: bool,
    include_live_videos: bool,
    use_xsec: bool,
    has_live_photos: bool,
    reactions_used: dict[str, bool] | None = None,
    ai_summary: str = '',
    has_anchor_comments: bool = False,
    anchor_comments_sent: bool = False,
    has_xsec_token: bool = False,
    media_too_large: bool = False,
) -> str:
    """Build a rich HTML footer for the progress/summary message."""
    if reactions_used is None:
        reactions_used = {}

    parts: list[str] = []

    # AI summary section (before flags)
    if ai_summary:
        parts.append(f'\n<b>✨ AI Summary</b>\n<pre language="Note">{ai_summary}</pre>')

    # ── Flags ──
    flag_items: list[tuple[str, str, bool]] = []
    if has_xsec_token:
        flag_items.append(('-x', 'xsec_token', use_xsec))
    flag_items.append(('-f', 'send as file', send_as_file))
    if has_live_photos:
        flag_items.append(('-l', 'live photos', include_live_videos))
    flag_lines = []
    indent = '\u00A0\u00A0\u00A0'  # non-breaking spaces for Telegram indent
    for flag, label, on in flag_items:
        mark = '✅' if on else '◻️'
        flag_lines.append(f'{indent}{mark} <code>{flag}</code>\u2002<code>{label}</code>')
    parts.append(f'\n<b>🏷 Flags</b>\n' + '\n'.join(flag_lines))

    # ── Reactions ──
    react_lines: list[str] = []

    if send_as_file:
        react_lines.append(f'{indent}👨‍💻 Files — <i>included</i> <code>-f</code>')
    elif reactions_used.get('file'):
        react_lines.append(f'{indent}👨‍💻 <s>Files</s>\u2002✅')
    else:
        react_lines.append(f'{indent}👨‍💻 Files — <i>react to get</i>')

    if has_live_photos and not include_live_videos:
        if reactions_used.get('eyes'):
            react_lines.append(f'{indent}👀 <s>Live photos</s>\u2002✅')
        else:
            react_lines.append(f'{indent}👀 Live photos — <i>react to get</i>')
    elif has_live_photos and include_live_videos:
        react_lines.append(f'{indent}👀 Live photos — <i>included</i> <code>-l</code>')

    if not media_too_large:
        if reactions_used.get('thinking'):
            react_lines.append(f'{indent}🤔 <s>AI Summary</s>\u2002✅')
        else:
            react_lines.append(f'{indent}🤔 AI Summary — <i>react to get</i>')

    if has_anchor_comments:
        if anchor_comments_sent:
            react_lines.append(f'{indent}💬 Anchor comment\u2002✅')
        else:
            react_lines.append(f'{indent}💬 Anchor comment — <i>⏳ sending</i>')

    parts.append(f'\n<b>⚡ Reactions</b>\n' + '\n'.join(react_lines))

    return ''.join(parts)

def make_block_quotation_html(text: str, expandable: bool = True) -> str:
    """Wrap text in an HTML <blockquote>. Uses expandable when text is long."""
    if not text:
        return ''
    escaped = tg_msg_escape_html(text)
    lines = [l for l in escaped.split('\n') if l.strip()]
    if not lines:
        return ''
    content = '\n'.join(lines)
    tag = 'blockquote expandable' if expandable and len(lines) > 3 else 'blockquote'
    return f'<{tag}>{content}</blockquote>'


def get_redirected_url(url: str) -> str:
    return unquote(
        requests.get(url if 'http' in url else f'http://{url}').url.split("redirectPath=")[-1]
    )


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


def open_note(noteId: str, anchorCommentId: str | None = None) -> dict[str, Any] | None:
    try:
        return requests.get(
            f'https://{FLASK_SERVER_NAME}/open_note/{noteId}' +
            (f"?anchorCommentId={anchorCommentId}" if anchorCommentId else '')
        ).json()
    except Exception:
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
                    redirectPath = unquote(
                        redirectPath
                        .replace('https://www.xiaohongshu.com/login?redirectPath=', '')
                        .replace('https://www.xiaohongshu.com/404?redirectPath=', '')
                    )
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
    content = re.sub(r'(?P<tag>#\S+?)\[\S+\]#', r'\g<tag> ', content)
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
        picture_urls.append(
            re.sub(r'sns-note-i\d.xhscdn.com', 'sns-na-i6.xhscdn.com', original_url)
            .split('?imageView')[0] + '?imageView2/2/w/5000/h/5000/format/webp/q/56&redImage/frame/0'
        )
    audio_info = comment_data.get('audio_info', '')
    audio_url = ''
    if audio_info:
        audio_data = audio_info.get('play_info', {})
        if audio_data:
            audio_url = audio_data.get('url', '')
    data: dict[str, Any] = {
        'user': user,
        'content': content,
        'pictures': picture_urls,
        'id': comment_data.get('id', ''),
        'time': comment_data.get('time', 0),
        'like_count': comment_data.get('like_count', 0),
        'sub_comment_count': comment_data.get('sub_comment_count', 0),
        'ip_location': comment_data.get('ip_location', '?'),
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
        data_parsed.append(parse_comment(c))
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
            sub_comments.append(parse_comment(sub_comment))
        parsed_comment["sub_comments"] = sub_comments
        data_parsed.append(parsed_comment)
    return data_parsed


def convert_to_ogg_opus_pipe(input_bytes: bytes) -> bytes:
    process = subprocess.Popen(
        ["ffmpeg", "-i", "pipe:0", "-c:a", "libopus", "-f", "ogg", "pipe:1"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    out, _ = process.communicate(input_bytes)
    return out


def convert_to_mp3_pipe(input_bytes: bytes) -> bytes:
    process = subprocess.Popen(
        ["ffmpeg", "-i", "pipe:0", "-vn", "-c:a", "libmp3lame", "-q:a", "3", "-f", "mp3", "pipe:1"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    out, _ = process.communicate(input_bytes)
    return out


# ── Note class ─────────────────────────────────────────────────────────────────

class Note:
    def __init__(
            self,
            note_data: dict[str, Any],
            comment_list_data: dict[str, Any],
            live: bool = False,
            telegraph: bool = False,
            with_full_data: bool = False,
            telegraph_account: Telegraph | None = None,
            anchorCommentId: str = '',
            xsec_token: str = '',
    ) -> None:
        self.telegraph_account = telegraph_account
        self.telegraph = telegraph
        self.live = live
        self.xsec_token = xsec_token
        if not note_data['data']:
            raise Exception("Note data not found!")
        self.user: dict[str, str | int] = {
            'id': note_data['data'][0]['user']['id'],
            'name': note_data['data'][0]['user']['name'],
            'red_id': note_data['data'][0]['user'].get('red_id', ''),
            'image': get_clean_url(note_data['data'][0]['user']['image']),
        }
        self.title: str = note_data['data'][0]['note_list'][0]['title'] if note_data['data'][0]['note_list'][0]['title'] else ''
        self.type: str = note_data['data'][0]['note_list'][0]['type']
        self.raw_desc = replace_redemoji_with_emoji(note_data['data'][0]['note_list'][0]['desc'])
        bot_logger.debug(f"Note raw_desc\n\n {self.raw_desc}")
        self.desc = re.sub(r'(?P<tag>#\S+?)\[\S+\]#', r'\g<tag> ', self.raw_desc)
        self.time = note_data['data'][0]['note_list'][0]['time']
        self.ip_location = (
            note_data['data'][0]['note_list'][0]['ip_location']
            if 'ip_location' in note_data['data'][0]['note_list'][0] else '?'
        )
        self.collected_count = note_data['data'][0]['note_list'][0]['collected_count']
        self.comments_count = note_data['data'][0]['note_list'][0]['comments_count']
        self.shared_count = note_data['data'][0]['note_list'][0]['shared_count']
        self.liked_count = note_data['data'][0]['note_list'][0]['liked_count']
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
                        self.images_list.append({'live': 'True', 'url': live_urls[0], 'thumbnail': remove_image_url_params(each['url'])})
                original_img_url = each['original']
                if re.findall(r'sns-na-i\d.xhscdn.com', original_img_url):
                    original_img_url = (
                        re.sub(r'sns-na-i\d.xhscdn.com', 'sns-na-i6.xhscdn.com', original_img_url)
                        .split('?imageView')[0] + '?imageView2/2/w/5000/h/5000/format/webp/q/56&redImage/frame/0'
                    )
                self.images_list.append({
                    'live': '',
                    'url': remove_image_url_params(original_img_url),
                    'thumbnail': remove_image_url_params(each['url_multi_level']['low']),
                })
        bot_logger.debug(f"Images found: {self.images_list}")
        self.url = get_clean_url(note_data['data'][0]['note_list'][0]['share_info']['link'])
        if self.xsec_token:
            sep = '&' if '?' in self.url else '?'
            self.url += f'{sep}xsec_token={quote(self.xsec_token)}'
        self.noteId = re.findall(r"[a-z0-9]{24}", self.url)[0]
        self.video_url = ''
        if 'video' in note_data['data'][0]['note_list'][0]:
            self.video_url = note_data['data'][0]['note_list'][0]['video']['url']
            if not re.findall(r'sign=[0-9a-z]+', self.video_url):
                self.video_url = re.sub(r'[0-9a-z\-]+\.xhscdn\.(com|net)', 'sns-bak-v1.xhscdn.com', self.video_url)
        if telegraph:
            self.to_html()

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
                media_list.append({'type': 'image', 'url': img['url']})
        if self.video_url and self.thumbnail:
            media_list.append({'type': 'image', 'url': self.thumbnail})
        return media_list

    def to_html(self) -> str:
        """Build HTML for the Telegraph page (unchanged from xhsfwbot.py)."""
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
        author_profile_url = f'https://www.xiaohongshu.com/user/profile/{self.user["id"]}'
        if self.xsec_token:
            author_profile_url += f'?xsec_token={quote(self.xsec_token)}'
        html += f'<h4>👤 <a href="{author_profile_url}"> @{self.user["name"]} ({self.user.get("red_id", "")})</a></h4>'
        html += f'<img src="{self.user["image"]}"></img>'
        html += f'<p>{get_time_emoji(self.time)} {convert_timestamp_to_timestr(self.time)}</p>'
        html += f'<p>❤️ {self.liked_count} ⭐ {self.collected_count} 💬 {self.comments_count} 🔗 {self.shared_count}</p>'
        ipaddr_html = tg_msg_escape_html(self.ip_location) if hasattr(self, 'ip_location') else '?'
        html += f'<p>📍 {ipaddr_html}</p>'
        html += f'<blockquote><a href="{self.url}">Source</a></blockquote>' if not self.title else ''
        if self.comments:
            html += '<hr>'
            for i, comment in enumerate(self.comments):
                comment_url = f'https://www.xiaohongshu.com/discovery/item/{self.noteId}?anchorCommentId={comment["id"]}'
                if self.xsec_token:
                    comment_url += f'&xsec_token={quote(self.xsec_token)}'
                html += f'<h4>💬 <a href="{comment_url}">Comment</a></h4>'
                if 'target_comment' in comment:
                    tc_profile = f'https://www.xiaohongshu.com/user/profile/{comment["target_comment"]["user"]["userid"]}'
                    if self.xsec_token:
                        tc_profile += f'?xsec_token={quote(self.xsec_token)}'
                    html += f'<p>↪️ <a href="{tc_profile}"> {'@' + comment["target_comment"]["user"].get("nickname", "")} ({comment["target_comment"]["user"].get('red_id', '')})</a></p>'
                html += f'<p>{tg_msg_escape_html(replace_redemoji_with_emoji(comment["content"]))}</p>'
                for pic in comment['pictures']:
                    if 'mp4' in pic:
                        html += f'<video src="{pic}"></video>'
                    else:
                        html += f'<img src="{pic}"></img>'
                if comment.get('audio_url', ''):
                    html += f'<p><a href="{comment["audio_url"]}">🎤 Voice</a></p>'
                html += f'<p>❤️ {comment["like_count"]} 💬 {comment["sub_comment_count"]}<br>📍 {tg_msg_escape_html(comment["ip_location"])}<br>{get_time_emoji(comment["time"])} {convert_timestamp_to_timestr(comment["time"])}</p>'
                cu_profile = f'https://www.xiaohongshu.com/user/profile/{comment["user"]["userid"]}'
                if self.xsec_token:
                    cu_profile += f'?xsec_token={quote(self.xsec_token)}'
                html += f'<p>👤 <a href="{cu_profile}"> {'@' + comment["user"].get("nickname", "")} ({comment["user"].get("red_id", "")})</a></p>'
                for sub_comment in comment.get('sub_comments', []):
                    html += '<blockquote><blockquote>'
                    sub_comment_url = f'https://www.xiaohongshu.com/discovery/item/{self.noteId}?anchorCommentId={sub_comment["id"]}'
                    if self.xsec_token:
                        sub_comment_url += f'&xsec_token={quote(self.xsec_token)}'
                    html += f'<h4>💬 <a href="{sub_comment_url}">Comment</a></h4>'
                    if 'target_comment' in sub_comment:
                        stc_profile = f'https://www.xiaohongshu.com/user/profile/{sub_comment["target_comment"]["user"]["userid"]}'
                        if self.xsec_token:
                            stc_profile += f'?xsec_token={quote(self.xsec_token)}'
                        html += f'<br><p>  ↪️  <a href="{stc_profile}"> {'@' + sub_comment["target_comment"]["user"].get("nickname", "")} ({sub_comment["target_comment"]["user"].get("red_id", "")})</a></p>'
                    html += f'<br><p>{tg_msg_escape_html(replace_redemoji_with_emoji(sub_comment["content"]))}</p>'
                    for pic in sub_comment['pictures']:
                        if 'mp4' in pic:
                            html += f'<br><video src="{pic}"></video>'
                        else:
                            html += f'<br><img src="{pic}"></img>'
                    if sub_comment.get('audio_url', ''):
                        html += f'<br><p><a href="{sub_comment["audio_url"]}">🎤 Voice</a></p>'
                    html += f'<br><p>❤️ {sub_comment["like_count"]} 💬 {sub_comment["sub_comment_count"]}<br>📍 {tg_msg_escape_html(sub_comment["ip_location"])}<br>{get_time_emoji(sub_comment["time"])} {convert_timestamp_to_timestr(sub_comment["time"])}</p>'
                    scu_profile = f'https://www.xiaohongshu.com/user/profile/{sub_comment["user"]["userid"]}'
                    if self.xsec_token:
                        scu_profile += f'?xsec_token={quote(self.xsec_token)}'
                    html += f'<br><p>👤 <a href="{scu_profile}"> {'@' + sub_comment["user"].get("nickname", "")} ({sub_comment["user"].get("red_id", "")})</a></p>'
                    html += '</blockquote></blockquote>'
                if i != len(self.comments) - 1:
                    html += '<hr>'
        self.html = html
        bot_logger.debug(f"HTML generated, \n\n{self.html}\n\n")
        return self.html

    def __str__(self) -> str:
        self.content = '笔记标题：' + self.title + '\n' + '笔记正文：' + self.desc
        self.content += f'\n发布者：@{self.user["name"]} ({self.user.get('red_id', '')})\n'
        self.content += f'{get_time_emoji(self.time)} {convert_timestamp_to_timestr(self.time)}\n'
        self.content += f'点赞：{self.liked_count}收藏：{self.collected_count}评论：{self.comments_count}分享：{self.shared_count}\n'
        ipaddr_html = (tg_msg_escape_html(self.ip_location) + '\n') if hasattr(self, 'ip_location') else '?\n'
        self.content += f'IP 地址：{ipaddr_html}\n\n评论区：\n\n'
        if self.comments:
            self.content += '\n'
            for i, comment in enumerate(self.comments):
                if comment["content"]:
                    self.content += '💬 评论\n'
                    self.content += f'{tg_msg_escape_html(replace_redemoji_with_emoji(comment["content"]))}\n'
                    self.content += f'点赞：{comment["like_count"]}\nIP 地址：{tg_msg_escape_html(comment["ip_location"])}\n{get_time_emoji(comment["time"])} {convert_timestamp_to_timestr(comment["time"])}\n'
                for sub_comment in comment.get('sub_comments', []):
                    if sub_comment["content"]:
                        self.content += '💬 回复\n'
                        self.content += f'{tg_msg_escape_html(replace_redemoji_with_emoji(sub_comment["content"]))}\n'
                        self.content += f'点赞：{sub_comment["like_count"]}\nIP 地址：{tg_msg_escape_html(sub_comment["ip_location"])}\n{get_time_emoji(sub_comment["time"])} {convert_timestamp_to_timestr(sub_comment["time"])}\n'
                if i != len(self.comments) - 1:
                    self.content += '\n'
        bot_logger.debug(f"String generated, \n\n{self.content}\n\n")
        return self.content

    async def to_telegraph(self) -> str:
        if not hasattr(self, 'html'):
            self.to_html()
        if not self.telegraph_account:
            self.telegraph_account = Telegraph()
            await self.telegraph_account.create_account(short_name='@xhsfwbot')  # type: ignore
        response = await self.telegraph_account.create_page(  # type: ignore
            title=f"{self.title} @{self.user['name']}",
            author_name=f'@{self.user["name"]} ({self.user.get('red_id', '')})',
            author_url=f"https://www.xiaohongshu.com/user/profile/{self.user['id']}",
            html_content=self.html,
        )
        self.telegraph_url = response['url']
        bot_logger.debug(f"Generated Telegraph URL: {self.telegraph_url}")
        return self.telegraph_url

    # ── Telethon-specific message methods ─────────────────────────────────────

    async def to_telethon_message(self, preview: bool = False) -> str:
        """Generate an HTML-formatted message for Telegram (via Telethon)."""
        message = ''
        if self.title:
            message += f'<b><a href="{self.url}">{tg_msg_escape_html(self.title)}</a></b>\n'

        if preview:
            desc_preview = (self.desc[:555] + '...') if self.desc and len(self.desc) > 555 else (self.desc or '')
            message += make_block_quotation_html(desc_preview)
            if hasattr(self, 'telegraph_url'):
                tg_url = self.telegraph_url
            else:
                tg_url = await self.to_telegraph()
            message += f'\n📝 <a href="{tg_url}">View more via Telegraph</a>'
        else:
            if self.desc:
                message += make_block_quotation_html(self.desc)
            if hasattr(self, 'telegraph_url'):
                message += f'\n📝 <a href="{self.telegraph_url}">Telegraph</a>'
            elif self.telegraph:
                tg_url = await self.to_telegraph()
                message += f'\n📝 <a href="{tg_url}">Telegraph</a>'

        name_esc = tg_msg_escape_html(self.user['name'])
        red_id_esc = tg_msg_escape_html(self.user.get('red_id', ''))
        uid = self.user['id']
        profile_url = f'https://www.xiaohongshu.com/user/profile/{uid}'
        if self.xsec_token:
            profile_url += f'?xsec_token={quote(self.xsec_token)}'
        message += f'\n\n<a href="{profile_url}">@{name_esc} ({red_id_esc})</a>'

        message += (
            f'\n<blockquote>'
            f'❤️ {self.liked_count} ⭐ {self.collected_count} 💬 {self.comments_count} 🔗 {self.shared_count}\n'
            f'{get_time_emoji(self.time)} {tg_msg_escape_html(convert_timestamp_to_timestr(self.time))}\n'
            f'📍 {tg_msg_escape_html(self.ip_location) if hasattr(self, "ip_location") else "?"}'
            f'</blockquote>'
        )

        if not self.title:
            message += f'\n📕 <a href="{self.url}">Note Source</a>'

        self.message = message
        bot_logger.debug(f"Telethon HTML message generated, \n\n{self.message}\n\n")
        return message

    async def send_as_telethon_message(
        self,
        bot: TelegramClient,
        chat_id: int,
        reply_to: int = 0,
        send_as_file: bool = False,
        include_live_videos: bool = False,
        progress_msg=None,
        use_xsec: bool = False,
        has_xsec_token: bool = False,
    ) -> None:
        """Send this note to Telegram using the Telethon client."""
        sent_messages: list = []

        caption = (
            self.message if hasattr(self, 'message')
            else await self.to_telethon_message(preview=bool(self.length >= 666))
        )

        # Collect photo URLs (non-live) and live photo video URLs
        photo_urls = [img['url'] for img in self.images_list if not img['live']]
        live_photo_urls = [img['url'] for img in self.images_list if img['live']]
        live_photo_count = len(live_photo_urls)

        # Handle video
        video_data: bytes | None = None
        dl_start_time = 0.0
        total_media = len(photo_urls) + (1 if self.video_url else 0)
        total_media_bytes = 0  # Track total media size for AI summary eligibility
        show_progress = progress_msg is not None or total_media > 1 or self.video_url

        # Count comment media for overall progress bar decision
        comment_with_media_count = 0
        comment_file_count = 0
        if self.comments_with_context:
            for c in self.comments_with_context:
                cp = c['pictures']
                if cp:
                    n = len(cp) if include_live_videos else len([p for p in cp if 'mp4' not in p])
                    if n > 0:
                        comment_with_media_count += 1
                        comment_file_count += n
                elif c.get('audio_url', ''):
                    comment_with_media_count += 1
                    comment_file_count += 1
        if not show_progress and (total_media + comment_file_count) > 1:
            show_progress = True

        if self.video_url:
            try:
                head = requests.head(self.video_url, timeout=10)
                total_bytes = int(head.headers.get('Content-Length', '0'))
                size_mb = total_bytes / (1024 * 1024)
                bot_logger.info(f"Video size: {size_mb:.2f}MB")

                if show_progress:
                    bar = _make_progress_bar(0)
                    msg_text = f'📥 Downloading video ({size_mb:.1f} MB)...\n{bar}'
                    if progress_msg:
                        try:
                            await progress_msg.edit(msg_text)
                        except MessageNotModifiedError:
                            pass
                    else:
                        progress_msg = await bot.send_message(
                            chat_id, msg_text,
                            reply_to=reply_to, silent=True,
                        )

                # Stream download with progress
                chunks: list[bytes] = []
                downloaded = 0
                last_update = time.monotonic()
                dl_start_time = last_update
                resp = requests.get(self.video_url, stream=True)
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    chunks.append(chunk)
                    downloaded += len(chunk)
                    now = time.monotonic()
                    if progress_msg and total_bytes and now - last_update >= 2:
                        last_update = now
                        pct = downloaded / total_bytes
                        bar = _make_progress_bar(pct)
                        dl_mb = downloaded / (1024 * 1024)
                        try:
                            await progress_msg.edit(
                                f'📥 Downloading video ({size_mb:.1f} MB)...\n{bar}  {dl_mb:.1f}/{size_mb:.1f} MB'
                            )
                        except MessageNotModifiedError:
                            pass
                video_data = b''.join(chunks)
                total_media_bytes += len(video_data)
                dl_elapsed = time.monotonic() - dl_start_time

                if progress_msg:
                    try:
                        bar = _make_progress_bar(1)
                        await progress_msg.edit(
                            f'📥 Video downloaded ({size_mb:.1f} MB, {dl_elapsed:.1f}s). Uploading...'
                        )
                    except MessageNotModifiedError:
                        pass
            except Exception as e:
                bot_logger.error(f"Failed to download video: {e}")

        if video_data and send_as_file:
            # ── Send video as document (file) ─────────────────────────────
            async with bot.action(chat_id, 'document'):
                try:
                    video_io = BytesIO(video_data)
                    video_io.name = 'video.mp4'
                    upload_mb = len(video_data) / (1024 * 1024)
                    ul_start_time = time.monotonic()

                    if progress_msg:
                        try:
                            await progress_msg.edit(
                                f'📤 Uploading file ({upload_mb:.1f} MB)...\n{_make_progress_bar(0)}'
                            )
                        except MessageNotModifiedError:
                            pass

                    upload_last_update = time.monotonic()

                    async def _file_upload_progress(current: int, total: int) -> None:
                        nonlocal upload_last_update
                        if not progress_msg:
                            return
                        now = time.monotonic()
                        if now - upload_last_update < 2:
                            return
                        upload_last_update = now
                        pct = current / total if total else 0
                        bar = _make_progress_bar(pct)
                        cur_mb = current / (1024 * 1024)
                        tot_mb = total / (1024 * 1024)
                        try:
                            await progress_msg.edit(
                                f'📤 Uploading file ({tot_mb:.1f} MB)...\n{bar}  {cur_mb:.1f}/{tot_mb:.1f} MB'
                            )
                        except MessageNotModifiedError:
                            pass

                    result = await bot.send_file(
                        chat_id, video_io,
                        caption=caption, parse_mode='html',
                        reply_to=reply_to, silent=True,
                        force_document=True,
                        progress_callback=_file_upload_progress if progress_msg else None,
                    )
                    sent_messages = result if isinstance(result, list) else [result]
                    ul_elapsed = time.monotonic() - ul_start_time

                    if progress_msg:
                        try:
                            summary = f'✅ Video file sent ({upload_mb:.1f} MB)\n'
                            summary += f'📥 Download: {dl_elapsed:.1f}s | 📤 Upload: {ul_elapsed:.1f}s'
                            await progress_msg.edit(summary)
                        except Exception:
                            pass
                except Exception as e:
                    bot_logger.error(f"Failed to send video as file: {e}")

        elif video_data:
            async with bot.action(chat_id, 'video'):
                try:
                    # Probe video dimensions and duration with ffprobe
                    v_w, v_h, v_dur = 0, 0, 0
                    v_codec = ''
                    v_bitrate = 0
                    try:
                        probe = subprocess.run(
                            ['ffprobe', '-v', 'quiet', '-print_format', 'json',
                             '-show_streams', '-show_format', 'pipe:0'],
                            input=video_data, capture_output=True, timeout=15,
                        )
                        probe_info = json.loads(probe.stdout)
                        for s in probe_info.get('streams', []):
                            if s.get('codec_type') == 'video':
                                v_w = int(s.get('width', 0))
                                v_h = int(s.get('height', 0))
                                v_codec = s.get('codec_name', '')
                                break
                        fmt = probe_info.get('format', {})
                        v_dur = int(float(fmt.get('duration', 0)))
                        v_bitrate = int(int(fmt.get('bit_rate', 0)) / 1000)
                    except Exception as pe:
                        bot_logger.warning(f"ffprobe failed, sending without dimensions: {pe}")

                    # Generate a thumbnail from the first frame
                    thumb_bytes: bytes | None = None
                    try:
                        thumb_proc = subprocess.run(
                            ['ffmpeg', '-i', 'pipe:0', '-vframes', '1',
                             '-f', 'image2', '-c:v', 'mjpeg', 'pipe:1'],
                            input=video_data, capture_output=True, timeout=15,
                        )
                        if thumb_proc.returncode == 0 and thumb_proc.stdout:
                            thumb_bytes = thumb_proc.stdout
                    except Exception as te:
                        bot_logger.warning(f"Thumbnail extraction failed: {te}")

                    video_io = BytesIO(video_data)
                    video_io.name = 'video.mp4'

                    thumb_io = None
                    if thumb_bytes:
                        thumb_io = BytesIO(thumb_bytes)
                        thumb_io.name = 'thumb.jpg'

                    # Upload with progress bar
                    upload_mb = len(video_data) / (1024 * 1024)
                    upload_last_update = time.monotonic()
                    ul_start_time = upload_last_update

                    if progress_msg:
                        try:
                            bar = _make_progress_bar(0)
                            await progress_msg.edit(
                                f'📤 Uploading video ({upload_mb:.1f} MB)...\n{bar}'
                            )
                        except MessageNotModifiedError:
                            pass
                    elif show_progress:
                        bar = _make_progress_bar(0)
                        msg_text = f'📤 Uploading video ({upload_mb:.1f} MB)...\n{bar}'
                        if progress_msg:
                            try:
                                await progress_msg.edit(msg_text)
                            except MessageNotModifiedError:
                                pass
                        else:
                            progress_msg = await bot.send_message(
                                chat_id, msg_text,
                                reply_to=reply_to, silent=True,
                            )

                    async def _upload_progress(current: int, total: int) -> None:
                        nonlocal upload_last_update, progress_msg
                        if not progress_msg:
                            return
                        now = time.monotonic()
                        if now - upload_last_update < 2:
                            return
                        upload_last_update = now
                        pct = current / total if total else 0
                        bar = _make_progress_bar(pct)
                        cur_mb = current / (1024 * 1024)
                        tot_mb = total / (1024 * 1024)
                        try:
                            await progress_msg.edit(
                                f'📤 Uploading video ({tot_mb:.1f} MB)...\n{bar}  {cur_mb:.1f}/{tot_mb:.1f} MB'
                            )
                        except MessageNotModifiedError:
                            pass

                    result = await bot.send_file(
                        chat_id, video_io,
                        caption=caption, parse_mode='html',
                        reply_to=reply_to, silent=True,
                        supports_streaming=True,
                        thumb=thumb_io,
                        progress_callback=_upload_progress if progress_msg else None,
                        attributes=[DocumentAttributeVideo(
                            duration=v_dur, w=v_w or 1280, h=v_h or 720,
                            supports_streaming=True,
                        )] if v_w and v_h else None,
                    )
                    sent_messages = result if isinstance(result, list) else [result]
                    ul_elapsed = time.monotonic() - ul_start_time

                    # Update progress message with intermediate summary
                    if progress_msg:
                        try:
                            summary = '✅ Video sent\n'
                            summary += f'📦 Size: {upload_mb:.1f} MB'
                            if v_w and v_h:
                                summary += f' | {v_w}×{v_h}'
                            if v_codec:
                                summary += f' | {v_codec.upper()}'
                            if v_bitrate:
                                summary += f' | {v_bitrate} kbps'
                            if v_dur:
                                mins, secs = divmod(v_dur, 60)
                                summary += f'\n⏱ Duration: {mins}:{secs:02d}'
                            summary += f'\n📥 Download: {dl_elapsed:.1f}s | 📤 Upload: {ul_elapsed:.1f}s'
                            await progress_msg.edit(summary)
                        except Exception:
                            pass
                except Exception as e:
                    bot_logger.error(f"Failed to send video: {e}\n{traceback.format_exc()}")

        elif photo_urls or (include_live_videos and live_photo_urls):
            # Build download list, preserving interleaved order from images_list
            download_list: list[dict[str, str]] = []
            for img in self.images_list:
                if img['live']:
                    if include_live_videos:
                        download_list.append({'url': img['url'], 'type': 'live_video'})
                else:
                    download_list.append({'url': img['url'], 'type': 'photo'})
            total_download = len(download_list)

            async with bot.action(chat_id, 'document' if send_as_file else 'photo'):
                if show_progress or total_download > 1:
                    msg_text = f'📥 Downloading {total_download} file(s)...\n{_make_progress_bar(0)}'
                    if progress_msg:
                        try:
                            await progress_msg.edit(msg_text)
                        except MessageNotModifiedError:
                            pass
                    else:
                        progress_msg = await bot.send_message(
                            chat_id, msg_text,
                            reply_to=reply_to, silent=True,
                        )
                dl_start_time = time.monotonic()
                last_update = dl_start_time
                all_files: list[BytesIO] = []
                for idx, item in enumerate(download_list):
                    resp = requests.get(item['url'])
                    total_media_bytes += len(resp.content)
                    bio = BytesIO(resp.content)
                    if item['type'] == 'live_video':
                        bio.name = f'live_{idx + 1}.mp4'
                    else:
                        bio.name = f'photo_{idx + 1}.jpg'
                    all_files.append(bio)
                    now = time.monotonic()
                    if progress_msg and now - last_update >= 1.5:
                        last_update = now
                        pct = (idx + 1) / total_download
                        bar = _make_progress_bar(pct)
                        try:
                            await progress_msg.edit(
                                f'📥 Downloading files...\n{bar}  {idx + 1}/{total_download}'
                            )
                        except MessageNotModifiedError:
                            pass
                dl_elapsed = time.monotonic() - dl_start_time

                if progress_msg:
                    try:
                        await progress_msg.edit(
                            f'📤 Uploading {len(photo_urls)} photo(s)...\n{_make_progress_bar(0)}'
                        )
                    except MessageNotModifiedError:
                        pass

                ul_start_time = time.monotonic()
                total_upload_bytes = sum(f.getbuffer().nbytes for f in all_files)
                total_size = total_upload_bytes / (1024 * 1024)
                uploaded_bytes = 0
                upload_last_update = ul_start_time
                num_batches = (len(all_files) + 9) // 10

                for i in range(0, len(all_files), 10):
                    batch = all_files[i:i + 10]
                    batch_idx = i // 10
                    batch_base_bytes = sum(f.getbuffer().nbytes for f in all_files[:i])
                    is_last_batch = (i + 10 >= len(all_files))
                    cap = caption if is_last_batch else None

                    async def _photo_upload_progress(current: int, total: int) -> None:
                        nonlocal upload_last_update
                        if not progress_msg:
                            return
                        now = time.monotonic()
                        if now - upload_last_update < 2:
                            return
                        upload_last_update = now
                        overall = batch_base_bytes + current
                        pct = overall / total_upload_bytes if total_upload_bytes else 0
                        bar = _make_progress_bar(pct)
                        cur_mb = overall / (1024 * 1024)
                        try:
                            await progress_msg.edit(
                                f'📤 Uploading photos ({batch_idx + 1}/{num_batches})...\n{bar}  {cur_mb:.1f}/{total_size:.1f} MB'
                            )
                        except MessageNotModifiedError:
                            pass

                    try:
                        result = await bot.send_file(
                            chat_id, batch,
                            caption=cap, parse_mode='html',
                            reply_to=reply_to, silent=True,
                            force_document=send_as_file,
                            progress_callback=_photo_upload_progress if progress_msg else None,
                        )
                        result_list = result if isinstance(result, list) else [result]
                        sent_messages.extend(result_list)
                        uploaded_bytes = batch_base_bytes + sum(f.getbuffer().nbytes for f in batch)
                    except Exception as e:
                        bot_logger.error(f"Photo batch {i} failed ({e})\n{traceback.format_exc()}")
                ul_elapsed = time.monotonic() - ul_start_time

                total_size = sum(f.getbuffer().nbytes for f in all_files) / (1024 * 1024)
                if progress_msg:
                    try:
                        lv_included = sum(1 for d in download_list if d['type'] == 'live_video')
                        summary = f'✅ {len(download_list)} file(s) sent\n'
                        summary += f'📦 Total size: {total_size:.1f} MB\n'
                        summary += f'📥 Download: {dl_elapsed:.1f}s | 📤 Upload: {ul_elapsed:.1f}s'
                        await progress_msg.edit(summary)
                    except Exception:
                        pass

        else:
            # No media at all – text only
            msg = await bot.send_message(
                chat_id, caption, parse_mode='html',
                reply_to=reply_to, silent=True, link_preview=False,
            )
            sent_messages = [msg]
            # Clean up progress bar for text-only notes without comment media
            if progress_msg and comment_with_media_count == 0:
                try:
                    await progress_msg.delete()
                    progress_msg = None
                except Exception:
                    pass

        if not sent_messages:
            bot_logger.error("No message was sent!")
            return

        # Persist message data for reactions (🤔 AI / 👨‍💻 files / 👀 live photos)
        # Check live photos in both main note and comments
        comment_has_live = any(
            any('mp4' in p for p in c.get('pictures', []))
            for c in self.comments_with_context
        ) if self.comments_with_context else False
        has_live_photos = live_photo_count > 0 or comment_has_live
        try:
            first_id = sent_messages[0].id
            msg_identifier = f"{chat_id}.{first_id}"
            msg_data = {
                'content': str(self),
                'media': self.media_for_llm(),
                'images_list': self.images_list,
                'video_url': getattr(self, 'video_url', ''),
                'progress_msg_id': progress_msg.id if progress_msg else None,
                'reactions_used': {'file': False, 'eyes': False, 'thinking': False},
                'ai_summary': '',
                'flags': {'send_as_file': send_as_file, 'include_live_videos': include_live_videos, 'use_xsec': use_xsec},
                'has_live_photos': has_live_photos,
                'has_xsec_token': has_xsec_token,
                'total_media_bytes': total_media_bytes,
                'has_anchor_comments': bool(self.comments_with_context),
                'anchor_comments_sent': False,
            }
            with open(os.path.join('data', f'{msg_identifier}.json'), 'w', encoding='utf-8') as f:
                json.dump(msg_data, f, indent=4, ensure_ascii=False)
            bot_logger.debug(f"Saved message data to data/{msg_identifier}.json")
        except Exception as e:
            bot_logger.error(f"Failed to save message data: {e}")

        # Send anchor-comment thread
        reply_id = sent_messages[0].id
        comment_id_to_msg_id: dict[str, int] = {}

        if self.comments_with_context:
            comment_media_sent = 0
            if comment_with_media_count > 0:
                msg_text = f'💬 Sending comment media (0/{comment_with_media_count})...\n{_make_progress_bar(0)}'
                if progress_msg:
                    try:
                        await progress_msg.edit(msg_text)
                    except MessageNotModifiedError:
                        pass
                elif show_progress:
                    progress_msg = await bot.send_message(
                        chat_id, msg_text,
                        reply_to=reply_to, silent=True,
                    )
            for ci, comment in enumerate(self.comments_with_context):
                comment_html = _build_comment_html(comment, self.noteId, xsec_token=self.xsec_token)

                this_reply_id = reply_id
                if 'target_comment' in comment and ci > 0:
                    target_id = comment['target_comment']['id']
                    if target_id in comment_id_to_msg_id:
                        this_reply_id = comment_id_to_msg_id[target_id]

                if comment['pictures']:
                    if include_live_videos:
                        # Include all media (photos + videos) in original order
                        pics = comment['pictures']
                    else:
                        # Exclude video URLs
                        pics = [p for p in comment['pictures'] if 'mp4' not in p]
                    chunks = [pics[k:k + 10] for k in range(0, len(pics), 10)]
                    async with bot.action(chat_id, 'document' if send_as_file else 'photo'):
                        for j, chunk in enumerate(chunks):
                            files = []
                            for idx, url in enumerate(chunk):
                                resp = requests.get(url)
                                bio = BytesIO(resp.content)
                                ext = '.mp4' if 'mp4' in url else '.jpg'
                                bio.name = f'comment_{ci + 1}_{j + 1}_{idx + 1}{ext}'
                                files.append(bio)
                            cap = comment_html if j == len(chunks) - 1 else None
                            result = await bot.send_file(
                                chat_id, files,
                                caption=cap, parse_mode='html',
                                reply_to=this_reply_id, silent=True,
                                force_document=send_as_file,
                            )
                            if j == len(chunks) - 1:
                                r0 = result[0] if isinstance(result, list) else result
                                comment_id_to_msg_id[comment['id']] = r0.id

                elif comment.get('audio_url', ''):
                    if send_as_file:
                        # Send audio directly as file
                        async with bot.action(chat_id, 'document'):
                            src_audio = requests.get(comment['audio_url']).content
                            mp3 = convert_to_mp3_pipe(src_audio)
                            file_io = BytesIO(mp3 if mp3 else src_audio)
                            file_io.name = f'comment_{comment.get("id", "voice")}.mp3'
                            try:
                                result = await bot.send_file(
                                    chat_id, file_io,
                                    caption=comment_html, parse_mode='html',
                                    reply_to=this_reply_id, silent=True,
                                    force_document=True,
                                )
                            except Exception as fe:
                                bot_logger.warning(f"Audio file send failed, fallback to text-only: {fe}")
                                result = await bot.send_message(
                                    chat_id, comment_html,
                                    parse_mode='html',
                                    reply_to=this_reply_id, silent=True,
                                    link_preview=False,
                                )
                            comment_id_to_msg_id[comment['id']] = result.id  # type: ignore[union-attr]
                    else:
                        async with bot.action(chat_id, 'record-audio'):
                            src_audio = requests.get(comment['audio_url']).content
                            ogg = convert_to_ogg_opus_pipe(src_audio)
                            mp3 = convert_to_mp3_pipe(src_audio)
                            try:
                                voice_io = BytesIO(ogg)
                                voice_io.name = f'comment_{comment.get("id", "voice")}.ogg'
                                result = await bot.send_file(
                                    chat_id, voice_io,
                                    caption=comment_html, parse_mode='html',
                                    reply_to=this_reply_id, silent=True,
                                    voice_note=True,
                                    attributes=[DocumentAttributeAudio(
                                        duration=0,
                                        voice=True,
                                    )],
                                )
                            except Exception as ve:
                                bot_logger.warning(f"Voice-note send failed, fallback to music audio: {ve}")
                                try:
                                    music_io = BytesIO(mp3 if mp3 else src_audio)
                                    music_io.name = f'comment_{comment.get("id", "voice")}.mp3'
                                    result = await bot.send_file(
                                        chat_id, music_io,
                                        caption=comment_html, parse_mode='html',
                                        reply_to=this_reply_id, silent=True,
                                        voice_note=False,
                                        attributes=[DocumentAttributeAudio(
                                            duration=0,
                                            voice=False,
                                            title='Comment Audio',
                                            performer='xhsfwbot',
                                        )],
                                    )
                                except Exception as ae:
                                    bot_logger.warning(f"Music-audio send failed, fallback to file: {ae}")
                                    try:
                                        file_io = BytesIO(mp3 if mp3 else (ogg if ogg else src_audio))
                                        file_io.name = f'comment_{comment.get("id", "voice")}.mp3'
                                        result = await bot.send_file(
                                            chat_id, file_io,
                                            caption=comment_html, parse_mode='html',
                                            reply_to=this_reply_id, silent=True,
                                            force_document=True,
                                        )
                                    except Exception as fe:
                                        bot_logger.warning(f"All audio send methods failed, fallback to text-only: {fe}")
                                        result = await bot.send_message(
                                            chat_id, comment_html,
                                            parse_mode='html',
                                            reply_to=this_reply_id, silent=True,
                                            link_preview=False,
                                        )

                            comment_id_to_msg_id[comment['id']] = result.id  # type: ignore[union-attr]

                else:
                    async with bot.action(chat_id, 'typing'):
                        result = await bot.send_message(
                            chat_id, comment_html,
                            parse_mode='html',
                            reply_to=this_reply_id, silent=True,
                            link_preview=False,
                        )
                        comment_id_to_msg_id[comment['id']] = result.id

                # Update comment media progress
                if progress_msg and comment_with_media_count > 0:
                    cp = comment['pictures']
                    if cp:
                        n = len(cp) if include_live_videos else len([p for p in cp if 'mp4' not in p])
                        had_media = n > 0
                    elif comment.get('audio_url', ''):
                        had_media = True
                    else:
                        had_media = False
                    if had_media:
                        comment_media_sent += 1
                        pct = comment_media_sent / comment_with_media_count
                        bar = _make_progress_bar(pct)
                        try:
                            await progress_msg.edit(
                                f'💬 Sending comment media...\n{bar}  {comment_media_sent}/{comment_with_media_count}'
                            )
                        except MessageNotModifiedError:
                            pass

            # Final progress summary for comment media
            if progress_msg and comment_media_sent > 0:
                try:
                    parts = []
                    if total_media > 0:
                        parts.append(f'{total_media} main')
                    parts.append(f'{comment_media_sent} comment')
                    await progress_msg.edit(f'✅ Done — {" + ".join(parts)} media sent')
                except Exception:
                    pass

        # Mark anchor comments as sent in JSON
        anchor_comments_sent = bool(self.comments_with_context)
        if anchor_comments_sent:
            try:
                fp = os.path.join('data', f'{msg_identifier}.json')
                with open(fp, 'r', encoding='utf-8') as f:
                    saved = json.load(f)
                saved['anchor_comments_sent'] = True
                with open(fp, 'w', encoding='utf-8') as f:
                    json.dump(saved, f, indent=4, ensure_ascii=False)
            except Exception as e:
                bot_logger.debug(f"Failed to update anchor_comments_sent: {e}")

        # ── Build final summary with footer ──────────────────────────────────
        if progress_msg:
            try:
                # Read the current progress message text as base
                current = (await bot.get_messages(chat_id, ids=progress_msg.id)).message or ''
                footer = _build_summary_footer(
                    send_as_file=send_as_file,
                    include_live_videos=include_live_videos,
                    use_xsec=use_xsec,
                    has_live_photos=has_live_photos,
                    has_anchor_comments=bool(self.comments_with_context),
                    anchor_comments_sent=anchor_comments_sent,
                    has_xsec_token=has_xsec_token,
                    media_too_large=total_media_bytes > AI_SUMMARY_MAX_MEDIA_BYTES,
                )
                await progress_msg.edit(current + footer, parse_mode='html')
            except MessageNotModifiedError:
                pass
            except Exception as e:
                bot_logger.debug(f"Failed to set final summary footer: {e}")


# ── Comment HTML builder ───────────────────────────────────────────────────────

def _build_comment_html(comment: dict[str, Any], noteId: str, xsec_token: str = '') -> str:
    """Build the HTML text for a single comment message."""
    anchor = f'https://www.xiaohongshu.com/discovery/item/{noteId}?anchorCommentId={comment["id"]}'
    if xsec_token:
        anchor += f'&xsec_token={quote(xsec_token)}'
    text = f'💬 <a href="{anchor}">Comment</a>'

    if 'target_comment' in comment:
        nick = tg_msg_escape_html(comment['target_comment']['user'].get('nickname', ''))
        red_id = tg_msg_escape_html(comment['target_comment']['user'].get('red_id', ''))
        tc_uid = comment['target_comment']['user']['userid']
        tc_profile = f'https://www.xiaohongshu.com/user/profile/{tc_uid}'
        if xsec_token:
            tc_profile += f'?xsec_token={quote(xsec_token)}'
        text += f'\n↪️ <a href="{tc_profile}">@{nick} ({red_id})</a>\n'
    else:
        text += '\n'

    text += make_block_quotation_html(replace_redemoji_with_emoji(comment['content'])) + '\n'

    nick = tg_msg_escape_html(comment['user'].get('nickname', ''))
    red_id = tg_msg_escape_html(comment['user'].get('red_id', ''))
    uid = comment['user']['userid']
    ip = tg_msg_escape_html(comment['ip_location'])
    time_str = tg_msg_escape_html(convert_timestamp_to_timestr(comment['time']))
    text += (
        f'❤️ {comment["like_count"]} 💬 {comment["sub_comment_count"]} '
        f'📍 {ip} {get_time_emoji(comment["time"])} {time_str}\n'
    )
    cu_profile = f'https://www.xiaohongshu.com/user/profile/{uid}'
    if xsec_token:
        cu_profile += f'?xsec_token={quote(xsec_token)}'
    text += f'👤 <a href="{cu_profile}">@{nick} ({red_id})</a>'
    return text


# ── Reaction helper ───────────────────────────────────────────────────────────

async def _set_reaction(bot: TelegramClient, peer: Any, msg_id: int, emoticon: str) -> None:
    try:
        await bot(functions.messages.SendReactionRequest(
            peer=peer,
            msg_id=msg_id,
            reaction=[tl_types.ReactionEmoji(emoticon=emoticon)],
        ))
    except Exception as e:
        bot_logger.debug(f"Failed to set reaction {emoticon!r}: {e}")


# ── AI summary (shared logic) ─────────────────────────────────────────────────

LLM_QUERY_TEMPLATE = '''以下是一篇小红书笔记的完整内容。请先判断笔记本体的性质，再据此确定总结的语气与取向。

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


def _compress_image_for_llm(media_bytes: bytes) -> bytes:
    try:
        img = Image.open(BytesIO(media_bytes))
        w, h = img.size
        if h > 720 or w > 1280:
            if h > w:
                new_h, new_w = 720, int(w * (720 / h))
            else:
                new_w, new_h = 1280, int(h * (1280 / w))
            img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
        out = BytesIO()
        img.convert('RGB').save(out, format='JPEG', quality=70, optimize=True)
        return out.getvalue()
    except Exception as e:
        bot_logger.warning(f"Failed to compress image, using original: {e}")
        return media_bytes


async def _run_ai_summary(
    bot: TelegramClient,
    gemini_client: genai.Client,
    chat_id: int,
    message_id: int,
    msg_file_path: str,
    data: dict[str, Any],
) -> None:
    """Generate AI summary and edit it into the progress/summary message."""
    progress_msg_id = data.get('progress_msg_id')
    if not progress_msg_id:
        bot_logger.warning("No progress_msg_id found for AI summary")
        return

    flags = data.get('flags', {})
    has_live = data.get('has_live_photos', False)
    reactions_used = data.get('reactions_used', {})

    async def _update_with_ai(ai_text: str) -> None:
        """Edit the progress/summary message with AI summary in the footer."""
        data['ai_summary'] = ai_text
        try:
            with open(msg_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            bot_logger.debug(f"Failed to save ai_summary to JSON: {e}")
        try:
            msg = await bot.get_messages(chat_id, ids=progress_msg_id)
            if not msg or not msg.message:
                return
            text = msg.message
            # Strip old footer
            ai_marker = '\n✨ AI Summary'
            idx = text.find(ai_marker)
            if idx != -1:
                text = text[:idx]
            else:
                marker = '\n🏷 Flags'
                idx = text.find(marker)
                if idx != -1:
                    text = text[:idx]
            footer = _build_summary_footer(
                send_as_file=flags.get('send_as_file', False),
                include_live_videos=flags.get('include_live_videos', False),
                use_xsec=flags.get('use_xsec', False),
                has_live_photos=has_live,
                reactions_used=reactions_used,
                ai_summary=ai_text,
                has_anchor_comments=data.get('has_anchor_comments', False),
                anchor_comments_sent=data.get('anchor_comments_sent', False),
                has_xsec_token=data.get('has_xsec_token', False),
                media_too_large=data.get('total_media_bytes', 0) > AI_SUMMARY_MAX_MEDIA_BYTES,
            )
            await asyncio.sleep(0.25)
            await msg.edit(text + footer, parse_mode='html')
        except MessageNotModifiedError:
            pass
        except Exception as e:
            bot_logger.debug(f"Edit failed: {e}")

    note_content: str = data.get('content', '')
    media_data: list[dict[str, str]] = data.get('media', [])

    if not note_content:
        await _update_with_ai('Error: Note content is empty.')
        return

    try:
        await _update_with_ai('Loading...')

        # Calculate available space: 4096 - base_msg - footer overhead
        # Use a conservative limit for the AI summary text
        content_length = max(60, min(120, len(note_content) // 3))
        llm_query = LLM_QUERY_TEMPLATE.format(
            content_length=content_length,
            note_content=note_content,
        )
        bot_logger.debug(f"LLM Query:\n{llm_query}")

        contents: list[genai_types.Part] = [genai_types.Part(text=llm_query)]

        await _update_with_ai('Downloading media...')

        for media in media_data:
            if media.get('type') == 'image' and 'url' in media:
                media_bytes = requests.get(media['url']).content
                compressed = _compress_image_for_llm(media_bytes)
                contents.append(genai_types.Part.from_bytes(data=compressed, mime_type='image/jpeg'))

        bot_logger.info(f"Generating summary, content_length limit={content_length}")
        await _update_with_ai('Generating summary...')

        response = gemini_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=genai_types.Content(parts=contents),
        )
        text = response.text
        bot_logger.info(f"Generated summary:\n{text}")

        if not response or not text:
            await _update_with_ai('Error: No response from AI service.')
            return

        # Escape and store the final summary
        escaped = tg_msg_escape_html(text)
        await _update_with_ai(escaped)

    except Exception as e:
        bot_logger.error(f"Error generating AI summary: {e}\n{traceback.format_exc()}")
        try:
            await _update_with_ai(f'Error: {tg_msg_escape_html(str(e))}')
        except Exception:
            pass


# ── Bark notify ────────────────────────────────────────────────────────────────

def bark_notify(message: str) -> None:
    bark_token = os.getenv('BARK_TOKEN')
    bark_key = os.getenv('BARK_KEY')
    bark_iv = os.getenv('BARK_IV', '472')

    if not bark_token:
        bot_logger.warning('BARK_TOKEN not set, cannot send bark notification')
        return

    try:
        if bark_key:
            payload = json.dumps({"body": message, "sound": "birdsong", "title": "xhsfwbot"}, ensure_ascii=False)
            if len(bark_key) != 32:
                bot_logger.error(f'BARK_KEY must be exactly 32 characters long, got {len(bark_key)}')
                requests.get(f'https://api.day.app/{bark_token}/{quote("xhsfwbot")}/{quote(message)}')
                return
            cipher = AES.new(bark_key.encode('utf-8'), AES.MODE_ECB)  # pyright: ignore
            encrypted = cipher.encrypt(pad(payload.encode('utf-8'), AES.block_size))
            ciphertext = base64.b64encode(encrypted).decode('utf-8')
            response = requests.post(f'https://api.day.app/{bark_token}', data={'ciphertext': ciphertext, 'iv': bark_iv})
            if response.status_code == 200:
                bot_logger.info('Encrypted Bark notification sent successfully')
            else:
                bot_logger.error(f'Failed to send encrypted bark notification: {response.status_code} {response.text}')
        else:
            requests.get(f'https://api.day.app/{bark_token}/{quote("xhsfwbot")}/{quote(message)}')
            bot_logger.info('Bark notification sent successfully')
    except Exception as e:
        bot_logger.error(f'Failed to send bark notification: {e}\n{traceback.format_exc()}')


def restart_script() -> None:
    bot_logger.info("Restarting script...")
    bark_notify("xhsfwbot is restarting.")
    try:
        process = psutil.Process(os.getpid())
        for handler in process.open_files() + process.net_connections():
            os.close(handler.fd)
    except Exception as e:
        bot_logger.error(f'Error when closing file descriptors: {e}\n{traceback.format_exc()}')
    python = sys.executable
    os.execl(python, python, *sys.argv)


# ── Main bot ───────────────────────────────────────────────────────────────────

def run_telegram_bot() -> None:
    api_id_str = os.getenv('API_ID', '')
    api_hash = os.getenv('API_HASH', '')
    bot_token = os.getenv('BOT_TOKEN', '')
    admin_id_str = os.getenv('ADMIN_ID', '')

    if not api_id_str or not api_hash or not bot_token:
        raise ValueError(
            "API_ID, API_HASH, and BOT_TOKEN are required in .env\n"
            "Get API_ID and API_HASH from https://my.telegram.org"
        )

    api_id = int(api_id_str)
    admin_id: int | None = int(admin_id_str) if admin_id_str else None

    # ── Proxy setup ────────────────────────────────────────────────────────────
    proxy = None
    if telegram_proxy:
        try:
            import socks  # PySocks
            parsed = urlparse(telegram_proxy)
            if parsed.scheme in ('socks5', 'socks4'):
                stype = socks.SOCKS5 if parsed.scheme == 'socks5' else socks.SOCKS4
                proxy = (stype, parsed.hostname, parsed.port)
                bot_logger.info(f'Using {parsed.scheme.upper()} proxy: {parsed.hostname}:{parsed.port}')
            elif parsed.scheme == 'http':
                proxy = (socks.HTTP, parsed.hostname, parsed.port)
                bot_logger.info(f'Using HTTP proxy: {parsed.hostname}:{parsed.port}')
        except ImportError:
            bot_logger.warning("PySocks not installed — proxy ignored. Install with: pip install PySocks")

    bot = TelegramClient('xhsfwbot_telethon', api_id, api_hash, proxy=proxy)

    # Telegraph account (shared, re-created if needed)
    telegraph_account = Telegraph()
    gemini_client = genai.Client()

    # ── Helper: send admin log ────────────────────────────────────────────────

    async def _send_log_to_admin(caption: str) -> None:
        if admin_id:
            try:
                await bot.send_file(
                    admin_id, logging_file,
                    caption=caption, parse_mode='html',
                    silent=True,
                )
            except Exception as e:
                bot_logger.error(f"Failed to send log to admin: {e}")

    # ── /start ─────────────────────────────────────────────────────────────────

    @bot.on(events.NewMessage(pattern=r'^/start(\s|$)'))
    async def start_handler(event: events.NewMessage.Event) -> None:
        try:
            await event.respond("I'm xhsfwbot, please send me a xhs link!\n/help for more info.")
        except Exception as e:
            bot_logger.error(f"Failed to send start message: {e}")
        raise events.StopPropagation

    # ── /help ──────────────────────────────────────────────────────────────────

    @bot.on(events.NewMessage(pattern=r'^/help(\s|$)'))
    async def help_handler(event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        bot_logger.debug(f"Help requested by user {user_id}")
        try:
            if help_config.get('type') == 'text':
                await event.respond(help_config['text'])
            elif help_config.get('type') == 'forward':
                from_chat = help_config['from_chat_id']
                msg_id = help_config['message_id']
                original = await bot.get_messages(from_chat, ids=msg_id)
                if original:
                    # Re-send without forward attribution (copy behaviour)
                    # Skip MessageMediaWebPage — can't be sent as file
                    if original.media and original.media.__class__.__name__ == 'MessageMediaWebPage':
                        await bot.send_message(event.chat_id, original.message or '', formatting_entities=original.entities, silent=True)
                    elif original.media:
                        await bot.send_file(
                            event.chat_id, original.media,
                            caption=original.message or '',
                            formatting_entities=original.entities, silent=True,
                        )
                    else:
                        await bot.send_message(event.chat_id, original.message or '', formatting_entities=original.entities, silent=True)
                else:
                    await event.respond("Help message is no longer available.")
            else:
                await event.respond(
                    "No help message has been configured yet. "
                    "Ask the administrator to set one with /sethelp."
                )
        except Exception as e:
            bot_logger.error(f"Failed to send help message: {e}")
        raise events.StopPropagation

    # ── /sethelp ───────────────────────────────────────────────────────────────

    @bot.on(events.NewMessage(pattern=r'^/sethelp(\s|$)'))
    async def sethelp_handler(event: events.NewMessage.Event) -> None:
        global help_config
        if not admin_id or event.sender_id != admin_id:
            await event.respond("Sorry, only the administrator can use this command.")
            raise events.StopPropagation

        reply = await event.get_reply_message()
        if reply:
            help_config = {
                'type': 'forward',
                'from_chat_id': reply.chat_id,
                'message_id': reply.id,
            }
            save_help_config(help_config)
            bot_logger.info(f"Admin {event.sender_id} set help to message {reply.id} from {reply.chat_id}")
            await event.respond(
                "✅ Help message updated — the replied message will be sent when users call /help."
            )
        else:
            # Extract text after /sethelp
            args_text = event.text.split(maxsplit=1)[1] if len(event.text.split(maxsplit=1)) > 1 else ''
            if args_text:
                help_config = {'type': 'text', 'text': args_text}
                save_help_config(help_config)
                bot_logger.info(f"Admin {event.sender_id} set help to custom text")
                await event.respond(f"✅ Help message updated to:\n\n{args_text}")
            else:
                await event.respond(
                    "ℹ️ Usage:\n"
                    "• /sethelp <text> — set help message to the given text\n"
                    "• Reply to any message + /sethelp — use that message as help content\n"
                    "• /clearhelp — remove custom help and revert to default"
                )
        raise events.StopPropagation

    # ── /clearhelp ─────────────────────────────────────────────────────────────

    @bot.on(events.NewMessage(pattern=r'^/clearhelp(\s|$)'))
    async def clearhelp_handler(event: events.NewMessage.Event) -> None:
        global help_config
        if not admin_id or event.sender_id != admin_id:
            await event.respond("Sorry, only the administrator can use this command.")
            raise events.StopPropagation
        help_config = {}
        save_help_config(help_config)
        bot_logger.info(f"Admin {event.sender_id} cleared the custom help message")
        await event.respond(
            "✅ Custom help message cleared. /help will now use the default message (if configured)."
        )
        raise events.StopPropagation

    # ── Reaction handler (🤔 AI summary / 👨‍💻 resend as file / 👀 send live photos) ──

    def _check_reaction(update, emoticon: str) -> bool:
        """Check if a specific emoticon is among the NEW reactions."""
        return any(
            (isinstance(r, ReactionEmoji) and r.emoticon == emoticon) or
            (hasattr(r, 'reaction') and isinstance(r.reaction, ReactionEmoji) and r.reaction.emoticon == emoticon)
            for r in (update.new_reactions or [])
        )

    async def _update_summary_msg(
        bot_client: TelegramClient, chat_id: int,
        data: dict[str, Any], msg_file_path: str,
    ) -> None:
        """Update the progress/summary message footer to reflect current reaction status."""
        progress_msg_id = data.get('progress_msg_id')
        if not progress_msg_id:
            return
        flags = data.get('flags', {})
        reactions_used = data.get('reactions_used', {})
        has_live = data.get('has_live_photos', False)
        ai_summary = data.get('ai_summary', '')
        try:
            msg = await bot_client.get_messages(chat_id, ids=progress_msg_id)
            if not msg or not msg.message:
                return
            text = msg.message
            # Strip old footer (AI summary + flags + reactions)
            # AI summary marker comes first if present
            ai_marker = '\n✨ AI Summary'
            idx = text.find(ai_marker)
            if idx != -1:
                text = text[:idx]
            else:
                marker = '\n🏷 Flags'
                idx = text.find(marker)
                if idx != -1:
                    text = text[:idx]
            footer = _build_summary_footer(
                send_as_file=flags.get('send_as_file', False),
                include_live_videos=flags.get('include_live_videos', False),
                use_xsec=flags.get('use_xsec', False),
                has_live_photos=has_live,
                reactions_used=reactions_used,
                ai_summary=ai_summary,
                has_anchor_comments=data.get('has_anchor_comments', False),
                anchor_comments_sent=data.get('anchor_comments_sent', False),
                has_xsec_token=data.get('has_xsec_token', False),
                media_too_large=data.get('total_media_bytes', 0) > AI_SUMMARY_MAX_MEDIA_BYTES,
            )
            await msg.edit(text + footer, parse_mode='html')
        except MessageNotModifiedError:
            pass
        except Exception as e:
            bot_logger.debug(f"Failed to update summary footer: {e}")

    @bot.on(events.Raw(UpdateBotMessageReaction))
    async def reaction_handler(update) -> None:  # type: ignore[name-defined]
        bot_logger.debug(f"Received reaction update: {update}")

        has_thinking = _check_reaction(update, '🤔')
        has_file = _check_reaction(update, '👨\u200d💻')
        has_eyes = _check_reaction(update, '👀')

        if not has_thinking and not has_file and not has_eyes:
            return

        try:
            chat_id = get_peer_id(update.peer)
            msg_id = update.msg_id
            user_id = get_peer_id(update.actor) if update.actor else None
        except Exception as e:
            bot_logger.error(f"Failed to parse reaction update: {e}")
            return

        msg_file_path = os.path.join('data', f'{chat_id}.{msg_id}.json')
        if not os.path.exists(msg_file_path):
            bot_logger.debug(f"No data file for {chat_id}.{msg_id}, ignoring reaction")
            return

        # Load data and check if reaction was already used
        with open(msg_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        reactions_used = data.get('reactions_used', {})

        # ── 👨‍💻 Resend media as files ─────────────────────────────────────────
        if has_file:
            bot_logger.info(f"User {user_id} reacted 👨‍💻 to message {msg_id} in {chat_id}")
            if data.get('flags', {}).get('send_as_file'):
                bot_logger.debug("Files already sent via -f flag, rejecting 👨‍💻")
                await _set_reaction(bot, update.peer, msg_id, '🤷‍♂️')
                return
            if reactions_used.get('file'):
                bot_logger.debug("File reaction already used, ignoring")
                return
            await _set_reaction(bot, update.peer, msg_id, '👌')
            try:
                images_list = data.get('images_list', [])
                video_url = data.get('video_url', '')

                # Collect all media URLs with proper naming:
                # photo_1, live_1, photo_2, photo_3 (no live), ...
                all_urls: list[dict[str, str]] = []
                photo_num = 0
                pending_live: dict[str, str] | None = None
                for img in images_list:
                    if img.get('live'):
                        # Live entry comes before its photo; hold it
                        pending_live = {'url': img['url']}
                    else:
                        photo_num += 1
                        all_urls.append({'url': img['url'], 'name': f'photo_{photo_num}.jpg'})
                        if pending_live:
                            all_urls.append({'url': pending_live['url'], 'name': f'live_{photo_num}.mp4'})
                            pending_live = None
                if video_url:
                    all_urls.append({'url': video_url, 'name': 'video_1.mp4'})

                if not all_urls:
                    bot_logger.debug("No media URLs found for 👨‍💻 reaction")
                    return

                total = len(all_urls)
                bar = _make_progress_bar(0)
                prog_msg = await bot.send_message(
                    chat_id,
                    f'📥 Downloading files (0/{total})...\n{bar}',
                    reply_to=msg_id, silent=True,
                )

                all_files: list[BytesIO] = []
                total_bytes = 0
                dl_start = time.monotonic()
                dl_last_edit = time.monotonic()
                for idx, item in enumerate(all_urls):
                    resp = requests.get(item['url'])
                    bio = BytesIO(resp.content)
                    bio.name = item['name']
                    all_files.append(bio)
                    total_bytes += len(resp.content)

                    # Update download progress (throttled to every 2s, always update on last)
                    now = time.monotonic()
                    if now - dl_last_edit >= 2 or idx == total - 1:
                        dl_last_edit = now
                        pct = (idx + 1) / total
                        bar = _make_progress_bar(pct)
                        dl_mb = total_bytes / (1024 * 1024)
                        try:
                            await prog_msg.edit(
                                f'📥 Downloading files ({idx + 1}/{total})...\n{bar}  {dl_mb:.1f} MB'
                            )
                        except MessageNotModifiedError:
                            pass

                dl_elapsed = time.monotonic() - dl_start
                total_mb = total_bytes / (1024 * 1024)

                # Upload phase
                try:
                    await prog_msg.edit(
                        f'📤 Uploading {total} file(s) ({total_mb:.1f} MB)...\n{_make_progress_bar(0)}'
                    )
                except MessageNotModifiedError:
                    pass

                ul_start = time.monotonic()
                ul_last_edit = time.monotonic()

                # Track cumulative upload progress across batches
                batches = list(range(0, len(all_files), 10))
                files_uploaded = 0

                for batch_idx, i in enumerate(batches):
                    batch = all_files[i:i + 10]
                    batch_size = sum(b.getbuffer().nbytes for b in batch)

                    async def _file_react_upload_progress(current: int, total_up: int) -> None:
                        nonlocal ul_last_edit
                        now = time.monotonic()
                        if now - ul_last_edit < 2:
                            return
                        ul_last_edit = now
                        # Approximate overall progress
                        done_bytes = sum(b.getbuffer().nbytes for b in all_files[:files_uploaded])
                        overall_pct = (done_bytes + current) / total_bytes if total_bytes else 0
                        bar = _make_progress_bar(overall_pct)
                        cur_mb = (done_bytes + current) / (1024 * 1024)
                        try:
                            await prog_msg.edit(
                                f'📤 Uploading {total} file(s) ({total_mb:.1f} MB)...\n{bar}  {cur_mb:.1f}/{total_mb:.1f} MB'
                            )
                        except MessageNotModifiedError:
                            pass

                    await bot.send_file(
                        chat_id, batch,
                        reply_to=msg_id, silent=True,
                        force_document=True,
                        progress_callback=_file_react_upload_progress,
                    )
                    files_uploaded += len(batch)

                ul_elapsed = time.monotonic() - ul_start
                bot_logger.info(f"Sent {len(all_files)} file(s) via 👨‍💻 reaction")

                # Delete progress message
                try:
                    await prog_msg.delete()
                except Exception:
                    pass

                # Mark reaction as used and update summary
                data['reactions_used']['file'] = True
                with open(msg_file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)
                await _update_summary_msg(bot, chat_id, data, msg_file_path)
            except Exception as e:
                bot_logger.error(f"Failed to resend media as files (👨‍💻): {e}\n{traceback.format_exc()}")
            return

        # ── 👀 Send missing live photos as videos ─────────────────────────────
        if has_eyes:
            bot_logger.info(f"User {user_id} reacted 👀 to message {msg_id} in {chat_id}")
            if data.get('flags', {}).get('include_live_videos'):
                bot_logger.debug("Live photos already included via -l flag, rejecting 👀")
                await _set_reaction(bot, update.peer, msg_id, '🤷‍♂️')
                return
            if reactions_used.get('eyes'):
                bot_logger.debug("Eyes reaction already used, ignoring")
                return
            try:
                images_list = data.get('images_list', [])

                live_urls = [img['url'] for img in images_list if img.get('live')]
                if not live_urls:
                    bot_logger.debug("No live photo URLs found for 👀 reaction")
                    await _set_reaction(bot, update.peer, msg_id, '🤷‍♂️')
                    return

                await _set_reaction(bot, update.peer, msg_id, '👌')

                live_files: list[BytesIO] = []
                for idx, url in enumerate(live_urls):
                    resp = requests.get(url)
                    bio = BytesIO(resp.content)
                    bio.name = f'live_{idx + 1}.mp4'
                    live_files.append(bio)

                for i in range(0, len(live_files), 10):
                    batch = live_files[i:i + 10]
                    await bot.send_file(
                        chat_id, batch,
                        reply_to=msg_id, silent=True,
                    )
                bot_logger.info(f"Sent {len(live_files)} live photo video(s) via 👀 reaction")

                # Mark reaction as used and update summary
                data['reactions_used']['eyes'] = True
                with open(msg_file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)
                await _update_summary_msg(bot, chat_id, data, msg_file_path)
            except Exception as e:
                bot_logger.error(f"Failed to send live photos (👀): {e}\n{traceback.format_exc()}")
            return

        # ── 🤔 AI summary ────────────────────────────────────────────────────
        bot_logger.info(f"User {user_id} reacted 🤔 to message {msg_id} in {chat_id}")
        if reactions_used.get('thinking'):
            bot_logger.debug("Thinking reaction already used, ignoring")
            return

        # Reject if media is too large for AI processing
        if data.get('total_media_bytes', 0) > AI_SUMMARY_MAX_MEDIA_BYTES:
            bot_logger.info("Media too large for AI summary, rejecting 🤔 reaction")
            await _set_reaction(bot, update.peer, msg_id, '🤷‍♂️')
            return

        # Set 👾 reaction
        await _set_reaction(bot, update.peer, msg_id, '👾')

        # Mark reaction as used before running
        data['reactions_used']['thinking'] = True
        try:
            with open(msg_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            bot_logger.debug(f"Failed to pre-save thinking reaction state: {e}")

        # Run AI summary — edits the progress/summary message in-place
        await _run_ai_summary(bot, gemini_client, chat_id, msg_id, msg_file_path, data)

    # ── Note processing (main message handler) ────────────────────────────────

    async def _note2feed_internal(event: events.NewMessage.Event) -> None:
        chat_id = event.chat_id
        msg_id = event.id
        # event.text already returns the caption for photo/video messages in Telethon
        message_text = event.text or ''

        if 'xhslink.com' not in message_text and 'xiaohongshu.com' not in message_text and not event.message.photo:
            return

        # QR code decoding from photo
        if event.message.photo:
            try:
                photo_bytes: bytes = await bot.download_media(event.message.photo, file=bytes)  # type: ignore[arg-type]
                image = Image.open(BytesIO(photo_bytes))
                decoded_objects: list[Any] = decode(image)  # pyright: ignore
                for obj in decoded_objects:
                    if obj.type == 'QRCODE':
                        qr_data = obj.data.decode('utf-8')
                        bot_logger.info(f"QR Code detected: {qr_data}")
                        message_text += f' {qr_data} '
            except Exception as e:
                bot_logger.error(f"Failed to decode QR code: {e}")

        url_info = get_url_info(message_text)
        if not url_info['success']:
            return

        noteId = str(url_info['noteId'])
        xsec_token = str(url_info['xsec_token'])
        anchorCommentId = str(url_info['anchorCommentId'])
        bot_logger.info(
            f'Note ID: {noteId}, xsec_token: {xsec_token if xsec_token else "None"}, '
            f'anchorCommentId: {anchorCommentId if anchorCommentId else "None"}'
        )

        # React with 👌
        try:
            await _set_reaction(bot, await event.get_input_chat(), msg_id, '👌')
            await asyncio.sleep(0.2)
        except Exception as e:
            bot_logger.debug(f"Failed to set OK reaction: {e}")

        async with bot.action(chat_id, 'typing'):
            await asyncio.sleep(0.2)

        bot_logger.debug('try open note on device')
        open_note(noteId, anchorCommentId=anchorCommentId)
        await asyncio.sleep(0.75)

        note_data: dict[str, Any] = {}
        comment_list_data: dict[str, Any] = {'data': {}}

        try:
            note_data = requests.get(f"https://{FLASK_SERVER_NAME}/get_note/{noteId}").json()
            with open(os.path.join('data', f'note_data-{noteId}.json'), 'w', encoding='utf-8') as f:
                json.dump(note_data, f, indent=4, ensure_ascii=False)
            times = 0
            while True:
                times += 1
                try:
                    comment_list_data = requests.get(f"https://{FLASK_SERVER_NAME}/get_comment_list/{noteId}").json()
                    with open(os.path.join('data', f'comment_list_data-{noteId}.json'), 'w', encoding='utf-8') as f:
                        json.dump(comment_list_data, f, indent=4, ensure_ascii=False)
                    bot_logger.debug('got comment list data')
                    break
                except Exception:
                    if times <= 3:
                        await asyncio.sleep(0.1)
                    else:
                        raise Exception('error when getting comment list data')
        except Exception:
            bot_logger.error(traceback.format_exc())
        finally:
            if not note_data or 'data' not in note_data:
                try:
                    await _set_reaction(bot, await event.get_input_chat(), msg_id, '😢')
                except Exception:
                    pass
                return

        if note_data['data']['data'][0]['note_list'][0]['model_type'] == 'error':
            bot_logger.warning(f"Note data not available\n{note_data['data']}")
            try:
                await _set_reaction(bot, await event.get_input_chat(), msg_id, '😢')
            except Exception:
                pass
            return

        try:
            try:
                await telegraph_account.get_account_info()  # type: ignore
            except Exception:
                await telegraph_account.create_account(short_name='@xhsfwbot')  # type: ignore

            # Parse flags: -x, -l, -f (or combined like -xl, -xlf, -fxl, etc.)
            flag_chars = set()
            for m in re.finditer(r'(?<!\S)-([xlf]+)(?!\S)', message_text):
                flag_chars.update(m.group(1))
            use_xsec = ('x' in flag_chars) and xsec_token
            note = Note(
                note_data['data'],
                comment_list_data=comment_list_data['data'],
                live=True,
                telegraph=True,
                telegraph_account=telegraph_account,
                anchorCommentId=anchorCommentId,
                xsec_token=xsec_token if use_xsec else '',
            )
            await note.initialize()

            try:
                send_as_file = 'f' in flag_chars
                include_live_videos = 'l' in flag_chars

                # Create early progress bar
                progress_msg = await bot.send_message(
                    chat_id,
                    f'📝 Creating Telegraph page...\n{_make_progress_bar(0)}',
                    reply_to=msg_id, silent=True,
                )
                tg_start = time.monotonic()

                async with bot.action(chat_id, 'typing'):
                    if not hasattr(note, 'telegraph_url') or not note.telegraph_url:
                        await note.to_telegraph()

                tg_elapsed = time.monotonic() - tg_start
                try:
                    await progress_msg.edit(
                        f'✅ Telegraph created ({tg_elapsed:.1f}s)\n📋 Preparing message...'
                    )
                except MessageNotModifiedError:
                    pass

                await note.to_telethon_message(preview=False)

                try:
                    await progress_msg.edit(
                        f'✅ Telegraph created ({tg_elapsed:.1f}s)\n📦 Sending media...'
                    )
                except MessageNotModifiedError:
                    pass

                await note.send_as_telethon_message(
                    bot, chat_id, reply_to=msg_id,
                    send_as_file=send_as_file,
                    include_live_videos=include_live_videos,
                    progress_msg=progress_msg,
                    use_xsec=use_xsec,
                    has_xsec_token=bool(xsec_token),
                )

                # Delete user's original message
                try:
                    await bot.delete_messages(chat_id, msg_id)
                except Exception as e:
                    bot_logger.debug(f"Failed to delete user message: {e}")

            except Exception:
                bot_logger.error(traceback.format_exc())
                try:
                    await _set_reaction(bot, await event.get_input_chat(), msg_id, '😢')
                except Exception:
                    pass

        except Exception as e:
            bot_logger.error(f"Error in note2feed: {e}\n{traceback.format_exc()}")
            try:
                await _set_reaction(bot, await event.get_input_chat(), msg_id, '😢')
            except Exception:
                pass

    async def _process_note_request(event: events.NewMessage.Event) -> None:
        user_id = event.sender_id
        bot_logger.debug(f"Queuing note request from user {user_id}, semaphore={processing_semaphore._value}")
        async with processing_semaphore:
            try:
                await _note2feed_internal(event)
            except Exception as e:
                bot_logger.error(f"Error processing note for user {user_id}: {e}")

    @bot.on(events.NewMessage(incoming=True))
    async def note2feed_handler(event: events.NewMessage.Event) -> None:
        # Ignore commands
        if event.text and event.text.startswith('/'):
            return
        # Accept if:
        #  - message contains xhslink.com or xiaohongshu.com
        #  - or is a photo (QR code)
        text = event.text or ''
        if (
            'xhslink.com' in text or 'xiaohongshu.com' in text
            or event.message.photo
        ):
            asyncio.create_task(_process_note_request(event))

    # ── /note command ──────────────────────────────────────────────────────────

    @bot.on(events.NewMessage(pattern=r'^/note(\s+\S+)?'))
    async def note_command(event: events.NewMessage.Event) -> None:
        asyncio.create_task(_process_note_request(event))
        raise events.StopPropagation

    # ── Inline query ───────────────────────────────────────────────────────────

    async def _inline_note2feed_internal(event: events.InlineQuery.Event) -> None:
        inline_start = time.monotonic()
        message_text = event.text
        if not message_text:
            return
        if 'xhslink.com' not in message_text and 'xiaohongshu.com' not in message_text:
            return

        url_info = get_url_info(message_text)
        if not url_info['success']:
            return

        noteId = str(url_info['noteId'])
        xsec_token = str(url_info['xsec_token'])
        anchorCommentId = str(url_info['anchorCommentId'])
        bot_logger.info(
            f'Inline Note ID: {noteId}, xsec_token: {xsec_token if xsec_token else "None"}, '
            f'anchorCommentId: {anchorCommentId if anchorCommentId else "None"}'
        )

        bot_logger.debug('try open note on device (inline)')
        open_note(noteId, anchorCommentId=anchorCommentId)
        await asyncio.sleep(1.0)

        note_data: dict[str, Any] = {}

        try:
            note_data = requests.get(f"https://{FLASK_SERVER_NAME}/get_note/{noteId}").json()
            with open(os.path.join('data', f'note_data-{noteId}.json'), 'w', encoding='utf-8') as f:
                json.dump(note_data, f, indent=4, ensure_ascii=False)
        except Exception:
            bot_logger.error(traceback.format_exc())
        finally:
            if not note_data or 'data' not in note_data:
                return

        if note_data['data']['data'][0]['note_list'][0]['model_type'] == 'error':
            bot_logger.warning(f"Inline note data not available\n{note_data['data']}")
            return

        try:
            try:
                await telegraph_account.get_account_info()  # type: ignore
            except Exception:
                await telegraph_account.create_account(short_name='@xhsfwbot')  # type: ignore

            note = Note(
                note_data['data'],
                comment_list_data={'data': {}},
                live=True,
                telegraph=True,
                telegraph_account=telegraph_account,
                anchorCommentId=anchorCommentId,
            )
            await note.initialize()
            telegraph_url = note.telegraph_url if hasattr(note, 'telegraph_url') else await note.to_telegraph()

            name_esc = tg_msg_escape_html(note.user['name'])
            uid = note.user['id']
            tag_str = f'\n{tg_msg_escape_html(note.tag_string)}' if note.tags else ''
            title_part = tg_msg_escape_html(note.title) if note.title else 'Note Source'

            msg_text = (
                f'📰 <a href="{telegraph_url}">View via Telegraph</a>\n\n'
                f'📕 <a href="{note.url}">{title_part}</a>{tag_str}\n\n'
                f'👤 <a href="https://www.xiaohongshu.com/user/profile/{uid}">@{name_esc}</a>'
            )

            thumb = (
                InputWebDocument(url=note.thumbnail, size=0, mime_type='image/jpeg', attributes=[])
                if note.thumbnail else None
            )

            result = event.builder.article(
                title=note.title or 'Note',
                description='Telegraph URL with xiaohongshu.com URL',
                text=msg_text,
                parse_mode='html',
                link_preview=True,
                thumb=thumb,
            )

            elapsed = time.monotonic() - inline_start
            if elapsed > 25:
                bot_logger.warning(f"Inline query took {elapsed:.1f}s, likely expired – skipping answer")
                return

            try:
                await event.answer([result])
            except QueryIdInvalidError:
                bot_logger.warning(f"Inline query expired after {elapsed:.1f}s – answer discarded")

        except Exception as e:
            bot_logger.error(f"Error in inline_note2feed: {e}\n{traceback.format_exc()}")

    @bot.on(events.InlineQuery)
    async def inline_handler(event: events.InlineQuery.Event) -> None:
        async with processing_semaphore:
            try:
                await _inline_note2feed_internal(event)
            except Exception as e:
                bot_logger.error(f"Error in inline handler: {e}")

    # ── Run ────────────────────────────────────────────────────────────────────

    async def _main() -> None:
        await bot.start(bot_token=bot_token)
        me = await bot.get_me()
        bot_logger.info(f"Bot started as @{me.username} (id={me.id}) using Telethon (MTProto)")
        bark_notify("xhsfwbot tries to start polling (Telethon).")
        await bot.run_until_disconnected()

    asyncio.run(_main())


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    try:
        telegraph_account = Telegraph()
        client = genai.Client()
        run_telegram_bot()
    except Exception as e:
        bot_logger.error(f"Fatal error: {e}\n{traceback.format_exc()}")
        restart_script()
