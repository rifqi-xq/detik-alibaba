from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from confluent_kafka import Producer, KafkaException
from typing import Union
import setting

app = FastAPI()

# Kafka configuration
conf = setting.kafka_setting
producer = Producer(
    {
        "bootstrap.servers": conf["bootstrap_servers"],
        "sasl.mechanisms": "PLAIN",
        "ssl.ca.location": conf["ca_location"],
        "security.protocol": "SASL_SSL",
        "ssl.endpoint.identification.algorithm": "none",
        "sasl.username": conf["sasl_plain_username"],
        "sasl.password": conf["sasl_plain_password"],
    }
)
TOPIC_NAME = conf["topic_name"]

# Data Model (Optional if you dynamically generate data in the GET request)
class RawDesktopData(BaseModel):
    article_dewasa: str  # Example: "dewasatidak"
    article_hoax: str  # Example: "default"
    article_id: str  # Example: "241005066"
    author: str  # Example: "Dinda Ayu"
    content_type: str  # Example: "videonews"
    created_date: int  # Example: 1728109800000
    created_date_ori: int  # Example: 1728108964000
    created_date_str: str  # Example: "2024-10-05 13:16:04"
    custom_pagetype: str  # Example: "video"
    dtma: str  # Example: "146380193.308930295.1675049473.1731549892.1731558486.481"
    dtmac: str  # Example: "acc-tv"
    dtmacsub: str  # Example: "mobile"
    dtmb: str  # Example: "146380193.5.10.1731558681"
    dtmdt: str  # Example: "Video Embed 241005066 - 20Detik"
    dtmf: str  # Example: "-"
    dtmhn: str  # Example: "20.detik.com"
    dtmn: str  # Example: "1518459520"
    dtmp: str  # Example: "/embed/241005066"
    dtmr: str  # Example: "https://www.detik.com/"
    dtmwv: str  # Example: "4.0"
    entry_time: int  # Example: 1731558681354
    ga: str  # Example: "GA1.2.194900304.1675049473"
    header_accept: str  # Example: "image/avif,image/webp,image/apng..."
    header_accept_encoding: str  # Example: "gzip, deflate, br, zstd"
    header_accept_language: str  # Example: "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7"
    header_cache_control: str  # Example: "no-cache"
    header_connection: str  # Example: "Keep-Alive"
    header_cookie: str  # Long string of cookie data
    header_pragma: str  # Example: "no-cache"
    header_priority: str  # Example: "i"
    header_referer: str  # Example: "https://20.detik.com/"
    header_sec_ch_ua: str  # Example: "\"Chromium\";v=\"130\", \"Android WebView\"..."
    header_sec_ch_ua_mobile: str  # Example: "?1"
    header_sec_ch_ua_platform: str  # Example: "\"Android\""
    header_sec_fetch_dest: str  # Example: "image"
    header_sec_fetch_mode: str  # Example: "no-cors"
    header_sec_fetch_site: str  # Example: "same-site"
    header_user_agent: str  # User-agent string
    header_via: str  # Example: "1.1 google"
    header_x_cloud_trace_context: str  # Example: "baf4786926d3b2816060ec2a6e926b2e..."
    header_x_forwarded_for: str  # Example: "114.4.213.215, 35.241.10.124"
    header_x_forwarded_proto: str  # Example: "https"
    header_x_requested_with: str  # Example: "org.detikcom.rss"
    id_fokus: str  # Example: ""
    kanal_id: str  # Example: "221213591"
    keywords: str  # Example: "what comes after love,drama korea what comes after love..."
    publish_date: int  # Example: 1728109800000
    publish_date_str: str  # Example: "2024-10-05 13:30:00"
    thumbnail_url: str  # Example: "https://cdnv.detik.com/videoservice/..."
    user_agent: str  # Example: "Mozilla/5.0 (Linux; Android 14; ..."
    video_present: str  # Example: "No"

class RawAppData(BaseModel):
    app_version: str  # Example: "6.5.12"
    device_brand: str  # Example: "samsung"
    device_id: str  # Example: "detikcom-c77808b154490499"
    device_name: str  # Example: "SM-A750GN"
    device_vendor_id: str  # Example: "c77808b154490499"
    os_version: str  # Example: "10"
    screen_resolution: str  # Example: "1080x2220"
    sdk_version: str  # Example: "1.1"
    user_agent: str  # Example: "Dalvik/2.1.0 (Linux; U; Android 10; SM-A750GN Build/QP1A.190711.020)"
    
    class Header:
        accept_encoding: str  # Example: "gzip"
        connection: str  # Example: "Keep-Alive"
        content_encoding: str  # Example: "gzip"
        content_length: str  # Example: "275"
        content_type: str  # Example: "application/octet-stream"
        entry_time: int  # Example: 1731560448601
        logged_time: int  # Example: 1731560448601
        user_agent: str  # Example: "Dalvik/2.1.0 (Linux; U; Android 10; SM-A750GN Build/QP1A.190711.020)"
        via: str  # Example: "1.1 google"
        x_cloud_trace_context: str  # Example: "754d140446f5a08d5e2c20aa19ed67da/7446109000015744094"
        x_forwarded_for: str  # Example: "112.215.65.43, 35.241.10.124"
        x_forwarded_proto: str  # Example: "https"
    
    class Session:
        session_start: int  # Example: 1731559489361
        session_end: int  # Example: 1731560089361
        
        class ScreenView:
            screen_view: str  # Example: "detikcom_wp/Berita_Terbaru"
            start: int  # Example: 1731560434599
            end: int  # Example: 1731560446483
            created_date: int  # Example: 0
            published_date: int  # Example: 0
            account_type: str  # Example: "acc-wpdetikcom"
        
        screen_views: list  # List of `ScreenView` objects

    header: Header  # Header object
    sessions: list  # List of `Session` objects


@app.post("/stream-data")
async def post_stream_data(data: Union[RawDesktopData, RawAppData]):
    """
    Endpoint to receive streaming data from the client.
    """
    try:
        record = data.dict()
        producer.produce(
            TOPIC_NAME,
            key=None,
            value=json.dumps(record).encode("utf-8"),
        )
        producer.flush()
        return {"message": "Data sent to Kafka successfully", "data": record}
    except KafkaException as e:
        raise HTTPException(status_code=500, detail=f"Kafka error: {e}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error sending data: {e}")


@app.get("/health")
async def health_check():
    """
    Health-check endpoint to confirm the app is running.
    """
    return {"status": "ok"}