import json
from typing import Any, Dict, Union
import logging


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

        # logging.info(f"""(6b) Desktop Doc
        #     unique_visitor: {self.unique_visitor},
        #     detik_id: {self.detik_id},
        #     session_notif: {self.session_notif},
        #     ga: {self.ga},
        #     dtmac: {self.dtmac},
        #     dtmac_sub: {self.dtmac_sub},
        #     dtmf: {self.dtmf},
        #     domain: {self.domain},
        #     kanal_id: {self.kanal_id},
        #     article_id: {self.article_id},
        #     site_id: {self.site_id},
        #     title: {self.title},
        #     url: {self.url},
        #     referer: {self.referer},
        #     header_referer: {self.header_referer},
        #     keywords: {self.keywords},
        #     token_push_notification: {self.token_push_notification},
        #     created_date: {self.created_date},
        #     entry_time: {self.entry_time},
        #     publish_date: {self.publish_date},
        #     x_real_ip: {self.x_real_ip},
        #     user_agent: {self.user_agent},
        #     custom_site_id: {self.custom_site_id},
        #     custom_page_type: {self.custom_page_type},
        #     custom_page_number: {self.custom_page_number},
        #     custom_page_size: {self.custom_page_size}
        #     """)

        
    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


def build_desktop_doc_from_byte_slice(raw_data: bytes) -> Union[DesktopDoc, None]:
    try:
        
        # logging.info(f"raw data → desktop: {raw_data}")
        data_dict = json.loads(raw_data)
        desktop_transformed = DesktopDoc(data_dict)
        # logging.info(f"desktop transformed → article: {desktop_transformed}")
        
        return desktop_transformed
    except Exception as e:
        logging.error(f"Error building desktop doc: {e}")

