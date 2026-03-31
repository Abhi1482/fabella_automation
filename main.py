import requests
import gspread
import os
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, unquote
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# 🔑 CONFIG (ENV VARIABLES)
# =========================

SHOP = os.environ["SHOP"]
ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]

ACCESS_TOKEN_META = os.environ["META_ACCESS_TOKEN"]
AD_ACCOUNT_FABELLA = os.environ["AD_ACCOUNT_FABELLA"]
AD_ACCOUNT_SR = os.environ["AD_ACCOUNT_SR"]

# =========================
# 📅 DATE (YESTERDAY)
# =========================

yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# =========================
# 📡 META SPEND
# =========================

def get_meta_spend(ad_account_id):
    url = f"https://graph.facebook.com/v19.0/{ad_account_id}/insights"

    params = {
        "access_token": ACCESS_TOKEN_META,
        "fields": "spend",
        "time_range": json.dumps({
            "since": yesterday,
            "until": yesterday
        })
    }

    try:
        res = requests.get(url, params=params)
        data = res.json()
        return float(data["data"][0]["spend"])
    except:
        return 0

spend_FABELLA = get_meta_spend(AD_ACCOUNT_FABELLA)
spend_SR = get_meta_spend(AD_ACCOUNT_SR)

# =========================
# 📦 PRODUCT COST
# =========================

product_cost = {
    10441727803694: 250,
    10441723773230: 250,
    10441718071598: 180,
    10441712369966: 180,
    10441706111278: 180,
    10441704931630: 180,
    10441682321710: 220,
    10441672524078: 220,
    10327452877102: 120,
    10327452451118: 120,
    10067247169838: 130,
    10067246809390: 130,
    10067246448942: 90,
    10067246252334: 90,
    10067246022958: 90,
    10065951097134: 90
}

# =========================
# 📡 SHOPIFY ORDERS
# =========================

url = f"https://{SHOP}.myshopify.com/admin/api/2024-01/orders.json"

headers = {
    "X-Shopify-Access-Token": ACCESS_TOKEN
}

params = {
    "created_at_min": f"{yesterday}T00:00:00+05:30",
    "created_at_max": f"{yesterday}T23:59:59+05:30",
    "status": "any",
    "limit": 250
}

response = requests.get(url, headers=headers, params=params)

if response.status_code != 200:
    print("❌ Shopify Error:", response.text)
    exit()

orders = response.json().get("orders", [])

# =========================
# 🧠 UTM EXTRACTION
# =========================

def get_utm_from_order(order):
    utm_source = None
    note_attr = order.get("note_attributes", [])

    if isinstance(note_attr, list):
        for item in note_attr:
            if item.get("name") == "utm_source":
                utm_source = item.get("value")

    if not utm_source:
        landing = order.get("landing_site")

        if not landing:
            for item in note_attr:
                if item.get("name") == "landing_page_url":
                    landing = item.get("value")

        if landing:
            parsed_url = urlparse(landing)
            utm_source = parse_qs(parsed_url.query).get("utm_source", ["unknown"])[0]

    utm_source = unquote(utm_source) if utm_source else "unknown"

    if utm_source == "unknown":
        utm_source = "SR_facebook"

    return utm_source

# =========================
# 📊 AGGREGATION
# =========================

PER_ORDER_COST = 65
SHIPROCKET_PERCENT = 0.0295

source_data = {}

for order in orders:
    utm_source = get_utm_from_order(order)

    if utm_source not in source_data:
        source_data[utm_source] = {
            "revenue": 0,
            "orders": 0,
            "quantity": 0,
            "product_cost": 0
        }

    revenue = float(order.get("total_price", 0))
    qty = 0
    cost = 0

    for item in order.get("line_items", []):
        pid = item.get("product_id")
        q = item.get("quantity", 0)

        qty += q
        cost += q * product_cost.get(pid, 0)

    source_data[utm_source]["revenue"] += revenue
    source_data[utm_source]["orders"] += 1
    source_data[utm_source]["quantity"] += qty
    source_data[utm_source]["product_cost"] += cost

# =========================
# 📊 PROFIT
# =========================

final_rows = []

for source, data in source_data.items():
    revenue = data["revenue"]
    orders_count = data["orders"]

    logistics = orders_count * PER_ORDER_COST
    shiprocket = revenue * SHIPROCKET_PERCENT

    if source == "facebook":
        ad_spend = spend_FABELLA
    elif source == "SR_facebook":
        ad_spend = spend_SR
    elif source == "Affiliate":
        ad_spend = 0.28 * revenue
    else:
        ad_spend = 0

    profit = revenue - data["product_cost"] - logistics - shiprocket - ad_spend

    final_rows.append([
        yesterday,
        source,
        revenue,
        orders_count,
        data["quantity"],
        data["product_cost"],
        logistics,
        shiprocket,
        ad_spend,
        profit
    ])

# =========================
# 📄 GOOGLE SHEETS
# =========================

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

client = gspread.authorize(creds)
sheet = client.open("FABELLA_DATA").sheet1

for row in final_rows:
    sheet.append_row(row)

print("✅ AUTOMATION SUCCESS")