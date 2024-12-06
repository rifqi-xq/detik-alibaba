import requests
import time
import random
from datetime import datetime
import json

# API endpoint
api_url = "http://8.215.82.122:8000/stream-data"


# Simulate RawAppData
def generate_raw_app_data():
    return {
        "data": {
            "app_version": "6.5.12",
            "device_brand": "samsung",
            "device_id": "detikcom-c77808b154490499",
            "device_name": "SM-A750GN",
            "device_vendor_id": "c77808b154490499",
            "header": {
                "Accept_Encoding": "gzip",
                "Connection": "Keep-Alive",
                "Content_Encoding": "gzip",
                "Content_Length": "275",
                "Content_Type": "application/octet-stream",
                "Entry_Time": "1731560448601",
                "Logged_Time": "1731560448601",
                "User_Agent": "Dalvik/2.1.0 (Linux; U; Android 10; SM-A750GN Build/QP1A.190711.020)",
                "Via": "1.1 google",
                "X_Cloud_Trace_Context": "754d140446f5a08d5e2c20aa19ed67da/7446109000015744094",
                "X_Forwarded_For": "112.215.65.43, 35.241.10.124",
                "X_Forwarded_Proto": "https",
            },
            "os_version": "10",
            "screen_resolution": "1080x2220",
            "sdk_version": "1.1",
            "sessions": [
                {
                    "session_start": 1731559489361,
                    "session_end": 1731560089361,
                    "screen_view": [
                        {
                            "screen_view": "detikcom_wp/Berita_Terbaru",
                            "start": 1731560434599,
                            "end": 1731560446483,
                            "createddate": 0,
                            "publisheddate": 0,
                            "account_type": "acc-wpdetikcom",
                        }
                    ],
                }
            ],
            "user_agent": "Dalvik/2.1.0 (Linux; U; Android 10; SM-A750GN Build/QP1A.190711.020)",
        }
    }


# Simulate RawDesktopData
def generate_raw_desktop_data():
    return {
        "data": {
            "articledewasa": "dewasatidak",
            "articlehoax": ["default"],
            "articleid": ["241005066"],
            "author": ["Dinda Ayu"],
            "contenttype": ["videonews"],
            "createddate": ["1728109800000"],
            "createddate_ori": ["1728108964000"],
            "createddate_str": ["2024-10-05 13:16:04"],
            "custom_pagetype": ["video"],
            "dtma": ["146380193.308930295.1675049473.1731549892.1731558486.481"],
            "dtmac": ["acc-tv"],
            "dtmacsub": ["mobile"],
            "dtmb": ["146380193.5.10.1731558681"],
            "dtmdt": ["Video Embed 241005066 - 20Detik"],
            "dtmf": ["-"],
            "dtmhn": ["20.detik.com"],
            "dtmn": ["1518459520"],
            "dtmp": ["/embed/241005066"],
            "dtmr": ["https://www.detik.com/"],
            "dtmwv": ["4.0"],
            "entry-time": ["1731558681354"],
            "ga": ["GA1.2.194900304.1675049473"],
            "header-accept": [
                "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"
            ],
            "header-accept-encoding": ["gzip, deflate, br, zstd"],
            "header-accept-language": ["id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7"],
            "header-cache-control": ["no-cache"],
            "header-connection": ["Keep-Alive"],
            "header-cookie": [
                "dtklucx=genm_5cfd5ebe255b0d163ebc71c31219f072; __dtmc=146380193; _fbp=fb.1.1675049473150.303444958; _cc_id=766256fe2c73ec493b6ce35ebeac341f; page_load_uuid=cf96c624-072a-45d7-9a53-52347ed9f7e3; gz_page_depth=%7B%22last%22%3A%22https%3A%2F%2Finet.detik.com%2Fconsumer%2Fd-6549082%2F4-fakta-galaxy-s23-ultra-performa-dan-kamera-juara%22%2C%22depth%22%3A2%7D; __auc=7009ddae1862b39ea21ec6dd508; _sharedID=c5cea608-b44e-43c8-97ac-9e206c245a55; _sharedID_cst=zix7LPQsHA%3D%3D; _ga_ZB365DT921=GS1.1.1711750369.4.1.1711750513.59.0.0; _ga_SQ24F7Q7YW=GS1.1.1714021930.35.0.1714021930.0.0.0; _ga_K2ECMCJBFQ=GS1.1.1714021930.35.0.1714021931.0.0.0; _ga_Y30K3QCN0N=GS1.1.1717778465.2.0.1717778465.60.0.0; _gcl_au=1.1.30634182.1724148376; _au_1d=AU1D-0100-001711750393-7TX4IUO3-1OX1; _ga_M7E3P87KRC=GS1.1.1729438063.1.0.1729438063.60.0.1168502466; ___iat_vis=1730A1FAB21C17F1.f185a3cf802a03f04fb812528ab5f3f8.1729438064288.7907a48a89ddc7ee9079468941e65ead.AMIAIEZZOZ.11111111.1-0.0; _pubcid=7eb2a15e-e435-43a0-851f-45fb17267ee7; _pubcid_cst=zix7LPQsHA%3D%3D; cto_bidid=v9hSV180OUZ6ZVA2TktlaTNINDBNMHhxSzMzME8yV2VBTHJub0E3aE8lMkZjQ1o5VXVQMkNCSmJ1WTZWNmJIVUN5VXJvWTFIZDIlMkZjU28yWFFtRmRkTmxFRk5obFElM0QlM0Q; __gpi=UID=00000f36b05b57db:T=1728191703:RT=1731105460:S=ALNI_MZBnF7BXfHO-gGTGpbWlmKmxR3Rtg; __eoi=ID=afda5ae8b02efd21:T=1728191703:RT=1731105460:S=AA-AfjZ44PKKgGBkWHenCi4boKSZ; _gid=GA1.2.941578010.1731466687; __dtma=146380193.308930295.1675049473.1731549892.1731558486.481; __gads=ID=2aaa56d5b3d3a9a3:T=1725960402:RT=1731558490:S=ALNI_MajgKloKAll8Ekek0jKrOtuDSJ-lw; _clck=d9034b%7C2%7Cfqv%7C0%7C1624; _clsk=7zria%7C1731558548142%7C1%7C0%7Cl.clarity.ms%2Fcollect; panoramaId_expiry=1731644948846; FCNEC=%5B%5B%22AKsRol8NFhn6SsWWZIQNGbHg0-QjXetIGAxcYGGDjSVcY3MiUdXtG26Pl1Eu00ngMFXo3nFxf7BXg-Zl_Uf75HA12YhhCZz5nVSASsPwLeRkLfWRzF1H3lo3kOMFic97ZmiiyPH-t6ZgFJ6gjOAOHnjavqnc2xp_Dw%3D%3D%22%5D%5D; cto_bundle=I9dz3194SCUyRkFwaTBOQ1R0QTZQSFp6TEFKcVhzQnFJRlFBeTNWVDVqMkJoS0klMkZ4JTJCWTNsRnJPV0pHYW1pQzA2QnV3djZUNEo4eTc5T1lwWWFzR2R5ZHpHVmhzNHBnVU9VWm5MSHRLcGxLZEd1dVptemViR1YyS3ZSYnVwd2tPdUtFY0R4Tg; _ga_CY42M5S751=GS1.1.1731558485.482.1.1731558608.60.0.0; _ga=GA1.2.194900304.1675049473; __dtmids=241112093,240913052,240630096,240918088,241114060,241016045,230124109,241114067,241012045,241005066; __dtmb=146380193.5.10.1731558681"
            ],
            "header-pragma": ["no-cache"],
            "header-priority": ["i"],
            "header-referer": ["https://20.detik.com/"],
            "header-sec-ch-ua": [
                '"Chromium";v="130", "Android WebView";v="130", "Not?A_Brand";v="99"'
            ],
            "header-sec-ch-ua-mobile": ["?1"],
            "header-sec-ch-ua-platform": ['"Android"'],
            "header-sec-fetch-dest": ["image"],
            "header-sec-fetch-mode": ["no-cors"],
            "header-sec-fetch-site": ["same-site"],
            "header-user-agent": [
                "Mozilla/5.0 (Linux; Android 14; 2203129G Build/UKQ1.231003.002; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/130.0.6723.86 Mobile Safari/537.36 Mobile/detiknetwork/detikcom/android 6.5.12"
            ],
            "header-via": ["1.1 google"],
            "header-x-cloud-trace-context": [
                "baf4786926d3b2816060ec2a6e926b2e/15348832425871903929"
            ],
            "header-x-forwarded-for": ["114.4.213.215, 35.241.10.124"],
            "header-x-forwarded-proto": ["https"],
            "header-x-requested-with": ["org.detikcom.rss"],
            "idfokus": [""],
            "kanalid": ["221213591"],
            "keywords": [
                "what comes after love,drama korea what comes after love,drama korea,drakor,lee se young,sakaguchi kentaro,hong jong hyun,nakamura anne"
            ],
            "publishdate": ["1728109800000"],
            "publishdate_str": ["2024-10-05 13:30:00"],
            "thumbnailUrl": [
                "https://cdnv.detik.com/videoservice/AdminTV/2024/10/05/What_Comes_After_Love-20241005131615-custom.jpg?w=650\u0026q=80"
            ],
            "useragent": [
                "Mozilla/5.0 (Linux; Android 14; 2203129G Build/UKQ1.231003.002; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/130.0.6723.86 Mobile Safari/537.36 Mobile/detiknetwork/detikcom/android 6.5.12"
            ],
            "videopresent": ["No"],
        }
    }


def send_streaming_data():
    while True:
        # Randomly send either RawDesktopData or RawAppData
        data_type = random.choice(["RawDesktopData", "RawAppData"])
        data_payload = (
            generate_raw_desktop_data()
            if data_type == "RawDesktopData"
            else generate_raw_app_data()
        )
        try:
            # Serialize the payload to JSON and add it as a query parameter
            response = requests.post(api_url, json=data_payload)

            if response.status_code == 200:
                print(f"Data sent successfully ({data_type}): {(str(data_payload))[:40]}")
            else:
                print(
                    f"Failed to send data ({data_type}): {response.status_code} - {response.text}"
                )
        except requests.ConnectionError:
            print("Failed to connect to the API server.")
            continue

        # Simulate 1 second delay for data sending
        time.sleep(1)


if __name__ == "__main__":
    send_streaming_data()
