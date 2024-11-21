import os

kafka_setting = {
    'bootstrap_servers': os.environ['BOOTSTRAP_SERVERS'],
    'topic_name': os.environ['TOPIC_NAME'],
    'group_name': os.environ['GROUP_NAME'],
    'sasl_plain_username': os.environ['SASL_USERNAME'],
    'sasl_plain_password': os.environ['SASL_PASSWORD'],
    'ca_location': os.environ['CA_LOCATION']
}

oss_setting = {
    'oss_access_key_id': os.environ['OSS_ACCESS_KEY_ID'],
    'oss_access_key_secret': os.environ['OSS_ACCESS_KEY_SECRET'],
    'oss_endpoint': os.environ['OSS_ENDPOINT'],
    'oss_bucket_name': os.environ['OSS_BUCKET_NAME']
}