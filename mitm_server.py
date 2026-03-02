import re
import requests
from mitmproxy.tools.main import mitmdump # type: ignore
from mitmproxy import http, ctx
from urllib.parse import parse_qs, urlparse
from typing import Any
import os
from dotenv import load_dotenv
load_dotenv()
FLASK_SERVER_NAME = '127.0.0.1'
FLASK_SERVER_PORT = os.getenv('FLASK_SERVER_PORT', '5001')

def set_request(note_id:str, url: str, data: dict[str, Any], type: str) -> dict[str, Any]:
    requests.post(
        f"http://{FLASK_SERVER_NAME}:{FLASK_SERVER_PORT}/set_{type}",
        json={"note_id": note_id, "url": url, "data": data}
    )
    return {"note_id": note_id, "url": url, "data": data}

class ImageFeedFilter:
    def __init__(self, callback: Any):
        self.callback = callback
        self.url_pattern = re.compile(r"https://edith.xiaohongshu.com/api/sns/v\d+/note/imagefeed")
        self.type = 'note'

    def get_note_id(self, url: str) -> str:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        note_id = query_params.get('note_id', [None])[0]
        if note_id is None:
            raise ValueError("note_id not found in URL")
        return note_id

    def response(self, flow: http.HTTPFlow) -> None:
        if re.findall(self.url_pattern, flow.request.pretty_url):
            data = flow.response
            if data is not None:
                json_data = data.json()
            else:
                json_data = {}
            self.callback(
                note_id=self.get_note_id(flow.request.pretty_url),
                url=flow.request.pretty_url,
                data=json_data,
                type=self.type
            )

class CommentListFilter(ImageFeedFilter):
    def __init__(self, callback: Any):
        super().__init__(callback)
        self.url_pattern = re.compile(r'https?://edith.xiaohongshu.com/api/sns/v\d+/note/comment/list')
        self.type = 'comment_list'

    def response(self, flow: http.HTTPFlow) -> None:
        if re.findall(self.url_pattern, flow.request.pretty_url):
            data = flow.response
            if data is not None:
                json_data = data.json()
            else:
                json_data = {}
            self.callback(
                note_id=self.get_note_id(flow.request.pretty_url),
                url=flow.request.pretty_url,
                data=json_data,
                type=self.type
            )


class BlockURLs:
    def __init__(self, block_pattern_list: list[str]):
        self.block_pattern_list = block_pattern_list
    
    def response(self, flow: http.HTTPFlow) -> None:
        if [True for pattern in self.block_pattern_list if re.findall(pattern, flow.request.pretty_url)]:
            if not flow.response:
                return
            flow.response.status_code = 345
            flow.response.content = b"{'fuckxhs': true}"
            view = ctx.master.addons.get("view") # type: ignore
            if view is not None and view.store_count() >= 10: # type: ignore
                view.clear() # type: ignore

def get_block_pattern_list() -> list[str]:
    return [
        r'https?://fe-static.xhscdn.com/data/formula-static/hammer/patch/\S*',
        r'https?://cdn.xiaohongshu.com/webview/\S*',
        r'https?://infra-webview-s1.xhscdn.com/webview/\S*',
        r'https?://apm-fe.xiaohongshu.com/api/data/\S*',
        r'https?://apm-native.xiaohongshu.com/api/collect/?\S*',
        r'https?://lng.xiaohongshu.com/api/collect/?\S*',
        r'https?://edith.xiaohongshu.com/api/sns/celestial/connect/config\S*',
        r'https?://edith.xiaohongshu.com/api/im/users/filterUser/stranger',
        r'https?://t\d.xiaohongshu.com/api/collect/?\S*',
        r'https?://edith.xiaohongshu.com/api/sns/v\d/note/metrics_report',
        r'https?://edith.xiaohongshu.com/api/sns/v\d/system_service/flag_exp\S*',
        r'https?://edith.xiaohongshu.com/api/sns/v\d/system_service/config\S*',
        r'https?://sns-avatar-qc.xhscdn.com/avatar\S*',
        r'https?://edith.xiaohongshu.com/api/sns/v\d/user/signoff/flow',
        r'https?://rec.xiaohongshu.com/api/sns/v\d/followings/reddot',
        r'https?://gslb.xiaohongshu.com/api/gslb/v\d/domainNew\S*',
        r'https?://edith-seb.xiaohongshu.com/api/sns/v\d/system_service/config\S*',
        r'https?://sns-na-i\d.xhscdn.com/?\S*',
        r'https?://sns-avatar-qc.xhscdn.com/user_banner\S*',
        r'https?://www.xiaohongshu.com/api/sns/v\d/hey\S*',
        r'https?://edith.xiaohongshu.com/api/sns/v\d/note/detailfeed/preload\S*',
        r'https?://sns-na-i\d.xhscdn.com/?\S*',
        r'https?://edith.xiaohongshu.com/api/media/v\d/upload/permit\S*',
        r'https?://sns-na-i\d.xhscdn.com/notes_pre_post\S*',
        r'https?://infra-app-log-\d*.cos.ap-shanghai.myqcloud.com/xhslog\S*',
        r'https?://edith.xiaohongshu.com/api/sns/v\d/note/video_played',
        r'https?://edith.xiaohongshu.com/api/sns/v\d/note/widgets',
        r'https?://ros-upload.xiaohongshu.com/bad_frame\S*',
        r'https?://infra-app-log-\d*.cos.accelerate.myqcloud.com/xhslog\S*',
        r'https?://mall.xiaohongshu.com/api/store/guide/components/shop_entrance\S*',
        r'https?://edith.xiaohongshu.com/api/sns/v\d/system_service/launch',
        r'https?://open.kuaishouzt.com/rest/log/open/sdk/collect\S*',
        r'https?://ci.xiaohongshu.com/icons/user\S*',
        r'https?://picasso-static-bak.xhscdn.com/fe-platform\S*',
        r'https?://edith.xiaohongshu.com/api/sns/v1/system/service/ui/config\S*',
        r'https?://apm-fe.xiaohongshu.com/api/data\S*',
        r'https?://ci.xiaohongshu.com/1040g00831lni0o1j520g4bnb0m4mho3oa1dtrao\S*',
        r'https?://edith.xiaohongshu.com/api/sns/user_cache/follow/rotate\S*',
        r'https?://edith.xiaohongshu.com/api/sns/v1/im/get_recent_chats\S*',
        r'https?://as.xiaohongshu.com/api/v1/profile/android\S*',
        r'https?://edith.xiaohongshu.com/api/sns/v\d/message/detect\S*',
        r'https?://fe-platform-i\d.xhscdn.com/platform\S*',
        r'https?://fe-video-qc.xhscdn.com/fe-platform\S*',
        r'https?://spider-tracker.xiaohongshu.com/api/spider\S*',
        # r'https?://edith.xiaohongshu.com/api/sns/v\d/note/collection/list\S*',
        # r'https?://edith.xiaohongshu.com/api/sns/v\d/user/collect_filter',
        # r'https?://edith.xiaohongshu.com/api/sns/v\d/note/user/posted\S*',
    ]

addons: list[Any] = [
    ImageFeedFilter(set_request),
    CommentListFilter(set_request),
    BlockURLs(get_block_pattern_list()),
]

def run_mitm():
    mitmdump(args=["-s", "xhsfeedbot.py", "--mode", "regular@8082", "--listen-host", "0.0.0.0"])


if __name__ == "__main__":
    run_mitm()