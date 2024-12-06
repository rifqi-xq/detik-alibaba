import json
from typing import Any, List, Optional, Tuple, Dict
import logging
import time

class VisitorDoc:
    def __init__(self, data: Dict[str, str]):
        self.unique_visitor = data.get("uniqueVisitor", "")
        self.detik_id = data.get("detikId", "")
        self.ga_id = data.get("gaId", "")
        self.token_id = data.get("tokenId", "")
        self.dtmac = data.get("dtmac", "")
        self.dtmf = data.get("dtmf", "")
        self.dtmac_sub = data.get("dtmacSub", "")
        self.logged_time = data.get("loggedTime", "")
        self.entery_date = data.get("enteryDate", "")
        self.user_agent = data.get("userAgent", "")
        self.x_real_ip = data.get("xRealIp", "")
        self.session_notif = data.get("sessionNotif", "")
        self.service_version = data.get("serviceVersion", "")
        self.service_git_commit = data.get("serviceGitcommit", "")


def parse_unixto_datetime(unix_time: int) -> str:
    """
    Convert Unix timestamp to ISO 8601 formatted datetime string.
    """
    from datetime import datetime

    try:
        return datetime.utcfromtimestamp(unix_time).isoformat()
    except ValueError:
        return ""


def extract_visitor_byte_slice_from_desktop_doc(
    raw_data: Any,
) -> Tuple[Optional[List[bytes]], Optional[List[Exception]]]:
    """
    Extract visitor data from desktop document source and return as serialized JSON byte slices.
    """
    try:
        
        logging.info(f"(6c) Visitor Time: {time.time()}")
        
        # Extract EntryTime and handle errors
        entry_time = "".join(raw_data.get("entry_time", "0"))
        logging.info(f"(6d) Entry time Time: {entry_time}")
        
        try:
            entry_time = int(entry_time)
        except ValueError:
            entry_time = 0

        # Extract XRealIP
        x_real_ip_list = raw_data.get("x_real_ip", [])
        x_real_ip = x_real_ip_list[0].split(",") if x_real_ip_list else []

        # logging.info(parse_unixto_datetime(int(time.time())))

        visitor = VisitorDoc(
            {
                "uniqueVisitor": raw_data.get("unique_visitor", ""),
                "detikId": raw_data.get("detik_id", ""),
                "gaId": raw_data.get("ga", ""),
                "tokenId": raw_data.get("token_push_notification", ""),
                "dtmac": raw_data.get("dtmac", ""),
                "dtmacSub": raw_data.get("dtmac_sub", ""),
                "dtmf": raw_data.get("dtmf", ""),
                "xRealIp": x_real_ip[0] if x_real_ip else "",
                "sessionNotif": raw_data.get("session_notif", ""),
                "userAgent": raw_data.get("user_agent", ""),
                "loggedTime": parse_unixto_datetime(int(time.time())),
                "enteryDate": parse_unixto_datetime(entry_time),
                "serviceVersion": raw_data.get("service_version", "unknown"),
                "serviceGitcommit": raw_data.get("service_git_commit", "unknown"),
            }
        )

        data_slice = json.dumps(visitor.__dict__).encode("utf-8")
        
        return [data_slice], None
    except Exception as e:
        logging.error(f"Error extracting visitor from desktop doc: {e}")


def extract_visitor_byte_slice_from_apps_doc(
    raw_data: Any,
) -> Tuple[Optional[List[bytes]], Optional[List[Exception]]]:
    """
    Extract visitor data from apps document source and return as serialized JSON byte slices.
    """
    try:
        doc_slices = []
        error_slices = []

        # Parse LoggedTime and EntryTime
        header = raw_data.get("header", {})
        try:
            logged_time = int(header.get("logged_time", "0"))
        except ValueError:
            logged_time = 0

        try:
            entry_time = int(header.get("entry_time", "0"))
        except ValueError:
            entry_time = 0

        for session in raw_data.get("sessions", []):
            for row in session.get("screen_view", []):
                # Prepare visitor data
                detik_id = (
                    row.get("detik_id", "-") if row.get("detik_id", "-") != "-" else "-"
                )
                token_id = (
                    row.get("token_id", "-") if row.get("token_id", "-") != "-" else "-"
                )

                x_real_ip = header.get("x_forwarded_for", "").split(",")[0]

                visitor = VisitorDoc(
                    {
                        "uniqueVisitor": raw_data.get("device_id", ""),
                        "detikId": detik_id,
                        "tokenId": token_id,
                        "gaId": "-",
                        "dtmac": row.get("account_type", ""),
                        "dtmacSub": "apps",
                        "dtmf": raw_data.get("device_vendor_id", ""),
                        "xRealIp": x_real_ip,
                        "userAgent": header.get("user_agent", ""),
                        "loggedTime": parse_unixto_datetime(logged_time),
                        "enteryDate": parse_unixto_datetime(entry_time),
                        "serviceVersion": raw_data.get("service_version", "unknown"),
                        "serviceGitcommit": raw_data.get(
                            "service_git_commit", "unknown"
                        ),
                    }
                )

                try:
                    doc_slices.append(json.dumps(visitor).encode("utf-8"))
                except Exception as e:
                    error_slices.append(e)

        return doc_slices if doc_slices else None, (
            error_slices if error_slices else None
        )
    except Exception as e:
        logging.error(f"Error extracting visitor from apps doc: {e}")
