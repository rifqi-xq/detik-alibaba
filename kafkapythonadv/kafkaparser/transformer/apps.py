import json
from typing import List, Optional, Dict, Any


class AppsHeader:
    def __init__(self, data: Dict[str, Any]):
        self.x_forwarded_host = data.get("X_Forwarded_Host", "")
        self.x_forwarded_server = data.get("x_Forwarded_Server", "")
        self.content_type = data.get("Content_Type", "")
        self.content_length = data.get("Content_Length", "")
        self.content_encoding = data.get("Content_Encoding", "")
        self.x_real_ip = data.get("X_Real_Ip", "")
        self.connection = data.get("Connection", "")
        self.accept_encoding = data.get("Accept_Encoding", "")
        self.logged_time = data.get("Logged_Time", "")
        self.entry_time = data.get("Entry_Time", "")
        self.x_forwarded_for = data.get("X_Forwarded_For", "")
        self.user_agent = data.get("User_Agent", "")

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


class AppsScreenView:
    def __init__(self, data: Dict[str, Any]):
        self.start = data.get("start", 0)
        self.end = data.get("end", 0)
        self.dtmp = data.get("dtmp", "")
        self.publisheddate = data.get("publisheddate", 0)
        self.article_id = data.get("articleid", "")
        self.screen_view = data.get("screen_view", "")
        self.createddate = data.get("createddate", 0)
        self.kanalid = data.get("kanalid", "")
        self.dtmdt = data.get("dtmdt", "")
        self.account_type = data.get("account_type", "")
        self.detik_id = data.get("detik_id", "")
        self.token_id = data.get("token-push-notification", "")
        self.keywords = data.get("keywords", "")
        self.custom_page_type = data.get("custom_page_type", "")
        self.custom_page_number = data.get("custom_page_number", "")
        self.custom_page_size = data.get("custom_page_size", "")

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


class AppsEvent:
    def __init__(self, data: Dict[str, Any]):
        self.label = data.get("label", "")
        self.category = data.get("category", "")
        self.action = data.get("action", "")

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


class AppsSession:
    def __init__(self, data: Dict[str, Any]):
        self.session_start = data.get("session_start", 0)
        self.session_end = data.get("session_end", 0)
        self.event = [AppsEvent(event) for event in data.get("event", [])]
        self.screen_view = [
            AppsScreenView(view) for view in data.get("screen_view", [])
        ]

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


class AppsDoc:
    def __init__(self, data: Dict[str, Any]):
        self.device_id = data.get("device_id", "")
        self.device_vendor_id = data.get("device_vendor_id", "")
        self.device_name = data.get("device_name", "")
        self.device_brand = data.get("device_brand", "")
        self.os_version = data.get("os_version", "")
        self.sdk_version = data.get("sdk_version", "")
        self.screen_resolution = data.get("screen_resolution", "")
        self.app_version = data.get("app_version", "")
        self.sessions = [AppsSession(session) for session in data.get("sessions", [])]
        self.header = AppsHeader(data.get("header", {}))

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)


def build_apps_doc_from_byte_slice(raw_data: bytes) -> Optional[AppsDoc]:
    try:
        # Parse raw_data as JSON
        print("Parsing AppsDoc...")
        parsed_data = json.loads(raw_data)
        return AppsDoc(parsed_data)
    except json.JSONDecodeError as e:
        print(f"Failed to parse rawData: {e}")
        return None
