import csv
import requests
import time
import os
import sys

# ---------------- CONFIG ---------------- #

API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
if not API_TOKEN:
    print("❌ Missing environment variable: PIPEDRIVE_API_TOKEN")
    sys.exit(1)

BASE_URL = "https://api.pipedrive.com/v1"
CSV_FILE = "query_result_2026-01-03T15_36_44.573240563Z.csv"
RATE_LIMIT_DELAY = 0.12


# ---------------- API HELPERS ---------------- #

def api_get(endpoint, params=None):
    params = params or {}
    params["api_token"] = API_TOKEN
    r = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
    time.sleep(RATE_LIMIT_DELAY)
    return r.json()

def api_post(endpoint, data):
    params = {"api_token": API_TOKEN}
    r = requests.post(
        f"{BASE_URL}/{endpoint}",
        params=params,
        json=data,
        timeout=30
    )
    time.sleep(RATE_LIMIT_DELAY)
    return r.json()

def api_put(endpoint, data):
    params = {"api_token": API_TOKEN}
    r = requests.put(
        f"{BASE_URL}/{endpoint}",
        params=params,
        json=data,
        timeout=30
    )
    time.sleep(RATE_LIMIT_DELAY)
    return r.json()


# ---------------- METADATA ---------------- #

def get_label_id(label_name):
    fields = api_get("personFields").get("data", [])
    for f in fields:
        if f.get("key") == "label":
            for opt in f.get("options", []):
                if opt.get("label", "").upper() == label_name.upper():
                    return opt["id"]
    raise RuntimeError(f"Label '{label_name}' not found")

def get_field_key_by_name(field_name):
    fields = api_get("personFields").get("data", [])
    for f in fields:
        if f.get("name") == field_name:
            return f["key"]
    raise RuntimeError(f"Custom field '{field_name}' not found")


# ---------------- LOADERS ---------------- #

def load_postgres_data():
    users = {}
    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = (row.get("email") or "").strip().lower()
            if not email:
                continue
            users[email] = row
    return users

def fetch_all_people():
    people = []
    start = 0
    limit = 500

    while True:
        res = api_get("persons", {"start": start, "limit": limit})
        if not res.get("success") or not res.get("data"):
            break

        people.extend(res["data"])

        if not res.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection"):
            break

        start += limit

    return people

def build_email_index(people):
    index = {}
    for p in people:
        for e in p.get("email", []):
            val = e.get("value") if isinstance(e, dict) else e
            if val:
                index[val.strip().lower()] = p
    return index


# ---------------- MAIN ---------------- #

def main():
    print("🚀 Starting Pipedrive enrichment (Render)…")

    IN_DB_LABEL_ID = get_label_id("IN DATABASE")

    USER_ID_FIELD = get_field_key_by_name("User ID")
    ALL_ORGS_FIELD = get_field_key_by_name("All LIGR Organizations")

    postgres = load_postgres_data()
    people = fetch_all_people()
    email_index = build_email_index(people)

    created = updated = errors = 0

    for email, row in postgres.items():
        person = email_index.get(email)

        payload = {
            "name": row.get("full_name"),
            USER_ID_FIELD: row.get("user_id"),
            ALL_ORGS_FIELD: row.get("all_ligr_organizations"),
            "label": IN_DB_LABEL_ID
        }

        if person:
            res = api_put(f"persons/{person['id']}", payload)
            if res.get("success"):
                updated += 1
            else:
                errors += 1
        else:
            payload["email"] = email
            res = api_post("persons", payload)
            if res.get("success"):
                created += 1
            else:
                errors += 1

    print("\n===== COMPLETE =====")
    print(f"Created: {created}")
    print(f"Updated: {updated}")
    print(f"Errors:  {errors}")


if __name__ == "__main__":
    main()
