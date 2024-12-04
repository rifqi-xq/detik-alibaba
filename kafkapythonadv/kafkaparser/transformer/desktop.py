import json
from typing import Any, Dict, Union


class DesktopDoc:
    def __init__(self, data: Dict[str, Any]):
        self.unique_visitor = data.get("dtma", [])
        self.detik_id = data.get("D_TS", [])
        self.session_notif = data.get("session-notif", [])
        self.ga = data.get("ga", [])
        self.dtmac = data.get("dtmac", [])
        self.dtmac_sub = data.get("dtmacsub", [])
        self.dtmf = data.get("dtmf", [])
        self.domain = data.get("dtmhn", [])
        self.kanal_id = data.get("kanalid", [])
        self.article_id = data.get("articleid", [])
        self.site_id = data.get("siteid", [])
        self.title = data.get("dtmdt", [])
        self.url = data.get("dtmp", [])
        self.referer = data.get("dtmr", [])
        self.header_referer = data.get("header-referer", [])
        self.keywords = data.get("keywords", [])
        self.token_push_notification = data.get("token-push-notification", [])
        self.created_date = data.get("createddate_ori", [])
        self.entry_time = data.get("entry-time", [])
        self.publish_date = data.get("publishdate", [])
        self.x_real_ip = data.get("header-x-forwarded-for", [])
        self.user_agent = data.get("useragent", [])
        self.custom_site_id = data.get("custom_siteid", [])
        self.custom_page_type = data.get("custom_pagetype", [])
        self.custom_page_number = data.get("custom_pagenumber", [])
        self.custom_page_size = data.get("custom_pagesize", [])


def build_desktop_doc_from_byte_slice(raw_data: bytes) -> Union[DesktopDoc, None]:
    try:
        data_dict = json.loads(raw_data)
        desktop_doc = DesktopDoc(data_dict)
        return desktop_doc
    except json.JSONDecodeError as e:
        print(f"Failed to parse rawData: {e}, rawData: {raw_data}")
        return None
