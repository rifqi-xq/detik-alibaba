import json
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

class ArticleDoc:
    def __init__(self, data: Dict[str, Any]):
        self.unique_visitor = data.get("uniqueVisitor", "")
        self.detik_id = data.get("detikId", "")
        self.session_notif = data.get("sessionNotif", "")
        self.ga_id = data.get("gaId", "")
        self.token_id = data.get("tokenId", "")
        self.dtmac = data.get("dtmac", "")
        self.dtmac_sub = data.get("dtmacSub", "")
        self.dtmf = data.get("dtmf", "")
        self.domain = data.get("domain", "")
        self.kanal_id = data.get("kanalId", "")
        self.article_id = data.get("articleId", "")
        self.site_id = data.get("siteId", "")
        self.title = data.get("title", "")
        self.url = data.get("url", "")
        self.referer = data.get("referer", "")
        self.header_referer = data.get("headerReferer", "")
        self.keywords = data.get("keywords", "")
        self.created_date = self.parse_datetime(data.get("createdDate"))
        self.publish_date = self.parse_datetime(data.get("publishDate"))
        self.entery_date = self.parse_datetime(data.get("enteryDate"))
        self.custom_page_type = data.get("customPageType", "")
        self.custom_page_number = data.get("customPageNumber", "")
        self.custom_page_size = data.get("customPageSize", "")
        self.logged_time = self.parse_datetime(data.get("loggedTime"))
        self.service_version = data.get("serviceVersion", "")
        self.service_git_commit = data.get("serviceGitcommit", "")

    @staticmethod
    def parse_datetime(value: Optional[Any]) -> Optional[datetime]:
        if isinstance(value, int):
            return datetime.fromtimestamp(value)
        return None

def extract_article_byte_slice_from_desktop_doc(raw_data: Any) -> Tuple[Optional[List[bytes]], Optional[List[Exception]]]:
    try:
        # Assuming raw_data is a dictionary-like object derived from DesktopDoc
        desktop_doc = raw_data  # No type conversion as we rely on Python's duck typing

        entry_time = int(desktop_doc.get("entryTime", "0"))  # Default to 0 if parsing fails
        article_doc = ArticleDoc({
            "uniqueVisitor": desktop_doc.get("uniqueVisitor", ""),
            "detikId": desktop_doc.get("detikId", ""),
            "sessionNotif": desktop_doc.get("sessionNotif", ""),
            "gaId": desktop_doc.get("ga", ""),
            "tokenId": desktop_doc.get("tokenPushNotification", ""),
            "dtmac": desktop_doc.get("dtmac", ""),
            "dtmacSub": desktop_doc.get("dtmacSub", ""),
            "dtmf": desktop_doc.get("dtmf", ""),
            "domain": desktop_doc.get("domain", ""),
            "kanalId": desktop_doc.get("kanalId", ""),
            "articleId": desktop_doc.get("articleId", ""),
            "siteId": desktop_doc.get("siteId", ""),
            "title": desktop_doc.get("title", ""),
            "url": desktop_doc.get("url", ""),
            "referer": desktop_doc.get("referer", ""),
            "headerReferer": desktop_doc.get("headerReferer", ""),
            "keywords": desktop_doc.get("keywords", ""),
            "createdDate": desktop_doc.get("createdDate", 0),
            "publishDate": desktop_doc.get("publishDate", 0),
            "enteryDate": entry_time,
            "customPageType": desktop_doc.get("customPageType", ""),
            "customPageNumber": desktop_doc.get("customPageNumber", ""),
            "customPageSize": desktop_doc.get("customPageSize", ""),
            "loggedTime": int(datetime.now().timestamp()),
            "serviceVersion": "1.0.0",  # Example version placeholder
            "serviceGitcommit": "abcdefg"  # Example git commit placeholder
        })

        data_slice = json.dumps(article_doc.__dict__).encode("utf-8")
        return [data_slice], None

    except Exception as e:
        return None, [e]

def extract_article_byte_slice_from_apps_doc(raw_data: Any) -> Tuple[Optional[List[bytes]], Optional[List[Exception]]]:
    try:
        apps_doc = raw_data  # Assuming raw_data is a dictionary-like object derived from AppsDoc

        doc_slice = []
        error_slice = []

        logged_time = int(apps_doc.get("header", {}).get("loggedTime", "0"))
        entry_time = int(apps_doc.get("header", {}).get("entryTime", "0"))

        for session in apps_doc.get("sessions", []):
            for row in session.get("screenView", []):
                try:
                    domain = ""
                    if "dtmp" in row:
                        parsed_url = row.get("dtmp", "")
                        domain = parsed_url.split('/')[2] if '/' in parsed_url else ""

                    article = {
                        "uniqueVisitor": apps_doc.get("deviceID", ""),
                        "detikId": row.get("detikID", "-"),
                        "tokenId": row.get("tokenID", "-"),
                        "gaId": "-",
                        "dtmac": row.get("accountType", ""),
                        "dtmacSub": "apps",
                        "domain": domain,
                        "dtmf": apps_doc.get("deviceVendorID", ""),
                        "kanalId": row.get("kanalid", ""),
                        "articleId": row.get("articleID", ""),
                        "siteId": "",
                        "title": row.get("dtmdt", ""),
                        "url": row.get("dtmp", ""),
                        "keywords": row.get("keywords", ""),
                        "createdDate": row.get("createddate", 0),
                        "publishDate": row.get("publisheddate", 0),
                        "customPageNumber": row.get("customPageNumber", ""),
                        "customPageSize": row.get("customPageSize", ""),
                        "customPageType": row.get("customPageType", ""),
                        "loggedTime": logged_time,
                        "enteryDate": entry_time,
                        "serviceVersion": "1.0.0",  # Example version placeholder
                        "serviceGitcommit": "abcdefg"  # Example git commit placeholder
                    }

                    js = json.dumps(article).encode("utf-8")
                    doc_slice.append(js)

                except Exception as e:
                    error_slice.append(e)

        return doc_slice if doc_slice else None, error_slice if error_slice else None

    except Exception as e:
        return None, [e]
