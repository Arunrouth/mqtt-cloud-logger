import json
import requests
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt

# ---------------------------------------------------
# Supabase config
# ---------------------------------------------------
SUPABASE_URL = "https://sisahycsodsqgutavqrt.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNpc2FoeWNzb2RzcWd1dGF2cXJ0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjQ1ODczNTEsImV4cCI6MjA4MDE2MzM1MX0.6NasowiITzJCLt3SAmrHuJiyzTEauVbxASaEqIdo8tc"

TABLE_NAME = "mqtt_messages"
SUPABASE_REST_URL = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"

HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

# ---------------------------------------------------
# MQTT config – FILL THESE
# ---------------------------------------------------
MQTT_HOST = "ncr.cub.mqtt.innoqlate.com"          # e.g. 192.168.1.50 or broker.emqx.io
MQTT_PORT = 1883                      # default MQTT port
MQTT_USER = "innv3-ncr-cub"
MQTT_PASS = "$t^28j9f9up3!YG%"

TOPICS = [
    "AI/NCR/CUB/114189/EDGE/RECORD",
    "AI/NCR/CUB/151262/EDGE/RECORD",
    "AI/NCR/CUB/112061/EDGE/RECORD",
    "AI/NCR/CUB/112328/EDGE/RECORD",
    "AI/NCR/CUB/112098/EDGE/RECORD",
]

# Track last save time for 1-hour rule
last_saved = {t: None for t in TOPICS}


# ---------------------------------------------------
# Function: insert one row into Supabase
# ---------------------------------------------------
def save_to_supabase(topic: str, payload: dict):
    data = {
        "topic": topic,
        "message": payload,
        "created_at": datetime.utcnow().isoformat()
    }

    resp = requests.post(SUPABASE_REST_URL, headers=HEADERS, data=json.dumps(data))
    if resp.status_code in (200, 201, 204):
        print("✔ Saved to Supabase")
    else:
        print("❌ Supabase error:", resp.status_code, resp.text)


# ---------------------------------------------------
# MQTT callback
# ---------------------------------------------------
def on_message(client, userdata, msg):
    topic = msg.topic
    now = datetime.now()

    print(f"📩 MQTT Message Received: {topic}")

    # 1-hour rule per topic
    if topic in last_saved and last_saved[topic] is not None:
        if now - last_saved[topic] < timedelta(hours=1):
            print("⏳ Skipped (less than 1 hour for this topic)")
            return

    try:
        payload_text = msg.payload.decode()
        payload = json.loads(payload_text)  # your JSON camera data
    except Exception as e:
        print("❌ Could not parse JSON, saving raw text:", e)
        payload = {"raw": msg.payload.decode(errors="ignore")}

    try:
        save_to_supabase(topic, payload)
        last_saved[topic] = now
    except Exception as e:
        print("❌ Error saving to Supabase:", e)


# ---------------------------------------------------
# MQTT client setup
# ---------------------------------------------------
client = mqtt.Client()  # deprecation warning is safe, can ignore
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.on_message = on_message

print("🔗 Connecting to MQTT broker...")
client.connect(MQTT_HOST, MQTT_PORT, 60)

for t in TOPICS:
    client.subscribe(t)
    print("📡 Subscribed:", t)

print("🚀 Listening for MQTT messages (1 per hour per topic)...")
client.loop_forever()
