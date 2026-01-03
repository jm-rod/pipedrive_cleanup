#!/usr/bin/env python3
"""
Pipedrive Contact Tagger & Enricher
- Tags contacts based on email match to Postgres database
- Adds User ID and All LIGR Organizations from Postgres

Tags (DB Status):
- "in database" = email found in Postgres
- "not in database" = email not found in Postgres  
- "unknown" = no email on the Pipedrive contact

Additional fields populated for matches:
- User ID = Postgres user ID
- All LIGR Organizations = Combined org list from Postgres
"""

import requests
import time
import csv
import os
from datetime import datetime

# Configuration
API_TOKEN = os.environ.get("PIPEDRIVE_API_TOKEN")
BASE_URL = "https://api.pipedrive.com/v1"
CSV_FILE = "query_result_2026-01-03T04_42_13_247228952Z.csv"
RATE_LIMIT_DELAY = 0.12  # 100 requests per 10 seconds


def api_get(endpoint, params=None):
    """Make GET request to Pipedrive API"""
    if params is None:
        params = {}
    params['api_token'] = API_TOKEN
    
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params)
    time.sleep(RATE_LIMIT_DELAY)
    return response.json()


def api_put(endpoint, data):
    """Make PUT request to Pipedrive API"""
    params = {'api_token': API_TOKEN}
    response = requests.put(f"{BASE_URL}/{endpoint}", params=params, json=data)
    time.sleep(RATE_LIMIT_DELAY)
    return response.json()


def api_post(endpoint, data):
    """Make POST request to Pipedrive API"""
    params = {'api_token': API_TOKEN}
    response = requests.post(f"{BASE_URL}/{endpoint}", params=params, json=data)
    time.sleep(RATE_LIMIT_DELAY)
    return response.json()


def test_connection():
    """Test API connection"""
    result = api_get("users/me")
    if result.get('success'):
        print(f"  Connected as: {result['data']['name']}")
        return True
    else:
        print(f"  Connection failed: {result}")
        return False


def load_postgres_data():
    """Load user data from Postgres CSV export"""
    users = {}  # email -> {user_id, full_name, all_organizations}
    
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row.get('email', '').strip().lower()
            if not email:
                continue  # Skip rows with missing email
            
            users[email] = {
                'user_id': row.get('user_id', '').strip(),
                'full_name': row.get('full_name', '').strip(),
                'all_organizations': row.get('all_organizations', '').strip()
            }
    
    return users


def fetch_all_persons():
    """Fetch all persons from Pipedrive"""
    persons = []
    start = 0
    limit = 500
    
    while True:
        result = api_get("persons", {"start": start, "limit": limit})
        
        if not result.get('success') or not result.get('data'):
            break
            
        persons.extend(result['data'])
        print(f"  Fetched {len(persons)} persons...", end='\r')
        
        if not result.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection'):
            break
            
        start += limit
    
    print(f"  Fetched {len(persons)} persons total    ")
    return persons


def get_or_create_field(field_name, field_type, options=None):
    """Get or create a person field"""
    # Check existing fields
    result = api_get("personFields")
    
    if result.get('success') and result.get('data'):
        for field in result['data']:
            if field['name'] == field_name:
                print(f"  Found existing '{field_name}' field: {field['key']}")
                return field['key']
    
    # Create new field
    print(f"  Creating '{field_name}' field...")
    field_data = {
        "name": field_name,
        "field_type": field_type
    }
    if options:
        field_data["options"] = options
    
    result = api_post("personFields", field_data)
    
    if result.get('success'):
        print(f"  Created field: {result['data']['key']}")
        return result['data']['key']
    else:
        print(f"  Failed to create field: {result}")
        return None


def get_enum_option_id(field_key, label):
    """Get the option ID for an enum field value"""
    result = api_get(f"personFields/{field_key}")
    
    if result.get('success') and result['data'].get('options'):
        for option in result['data']['options']:
            if option['label'].lower() == label.lower():
                return option['id']
    
    return None


def get_person_email(person):
    """Extract primary email from person record"""
    emails = person.get('email', [])
    if not emails:
        return None
    
    if isinstance(emails, list):
        for e in emails:
            if isinstance(e, dict):
                return e.get('value', '').strip().lower()
            elif isinstance(e, str):
                return e.strip().lower()
    elif isinstance(emails, str):
        return emails.strip().lower()
    
    return None


def process_persons(persons, postgres_data, field_keys, option_ids):
    """Process all persons - tag and enrich"""
    stats = {
        'in_database': 0,
        'not_in_database': 0,
        'unknown': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0
    }
    
    total = len(persons)
    
    for i, person in enumerate(persons):
        person_id = person['id']
        email = get_person_email(person)
        
        # Build update payload
        update_data = {}
        
        # Determine DB status tag
        if not email:
            tag = 'unknown'
            option_id = option_ids['unknown']
        elif email in postgres_data:
            tag = 'in_database'
            option_id = option_ids['in database']
            
            # Add User ID and All Organizations for matches
            pg_user = postgres_data[email]
            if pg_user['user_id']:
                update_data[field_keys['user_id']] = pg_user['user_id']
            if pg_user['all_organizations']:
                update_data[field_keys['all_orgs']] = pg_user['all_organizations']
        else:
            tag = 'not_in_database'
            option_id = option_ids['not in database']
        
        # Always set DB status
        update_data[field_keys['db_status']] = option_id
        
        # Check if update needed (skip if no changes)
        current_status = person.get(field_keys['db_status'])
        current_user_id = person.get(field_keys['user_id'])
        current_orgs = person.get(field_keys['all_orgs'])
        
        needs_update = (
            current_status != option_id or
            (tag == 'in_database' and (
                current_user_id != update_data.get(field_keys['user_id']) or
                current_orgs != update_data.get(field_keys['all_orgs'])
            ))
        )
        
        if not needs_update:
            stats['skipped'] += 1
        else:
            # Update person
            result = api_put(f"persons/{person_id}", update_data)
            
            if result.get('success'):
                stats[tag] += 1
                stats['updated'] += 1
            else:
                stats['errors'] += 1
                print(f"\n  Error updating person {person_id}: {result.get('error', 'Unknown error')}")
        
        # Progress
        processed = i + 1
        print(f"  Progress: {processed}/{total} ({processed*100//total}%) | ✓ DB:{stats['in_database']} | ✗ Not:{stats['not_in_database']} | ? Unknown:{stats['unknown']} | Skip:{stats['skipped']} | Err:{stats['errors']}", end='\r')
    
    print()
    return stats


def main():
    print("=" * 60)
    print("PIPEDRIVE CONTACT TAGGER & ENRICHER")
    print("=" * 60)
    
    # Test connection
    print("\n🔌 Testing API connection...")
    if not test_connection():
        return
    
    # Load Postgres data
    print(f"\n📂 Loading Postgres data from {CSV_FILE}...")
    postgres_data = load_postgres_data()
    print(f"  Loaded {len(postgres_data)} unique users")
    
    # Fetch Pipedrive persons
    print("\n📡 Fetching Pipedrive persons...")
    persons = fetch_all_persons()
    
    # Setup fields
    print("\n🔧 Setting up custom fields...")
    
    db_status_key = get_or_create_field(
        "DB Status", 
        "enum",
        options=[
            {"label": "in database"},
            {"label": "not in database"},
            {"label": "unknown"}
        ]
    )
    
    user_id_key = get_or_create_field("User ID", "varchar")
    all_orgs_key = get_or_create_field("All LIGR Organizations", "text")
    
    if not all([db_status_key, user_id_key, all_orgs_key]):
        print("  Failed to setup fields, exiting")
        return
    
    field_keys = {
        'db_status': db_status_key,
        'user_id': user_id_key,
        'all_orgs': all_orgs_key
    }
    
    # Get option IDs for DB Status
    print("\n🏷️  Getting DB Status options...")
    option_ids = {
        'in database': get_enum_option_id(db_status_key, 'in database'),
        'not in database': get_enum_option_id(db_status_key, 'not in database'),
        'unknown': get_enum_option_id(db_status_key, 'unknown')
    }
    print(f"  Options: {option_ids}")
    
    if None in option_ids.values():
        print("  Missing option IDs, exiting")
        return
    
    # Process persons
    print("\n👥 Processing persons...")
    stats = process_persons(persons, postgres_data, field_keys, option_ids)
    
    # Summary
    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"  ✓ In database:     {stats['in_database']}")
    print(f"  ✗ Not in database: {stats['not_in_database']}")
    print(f"  ? Unknown:         {stats['unknown']}")
    print(f"  ⏭ Skipped:         {stats['skipped']}")
    print(f"  🔄 Total updated:   {stats['updated']}")
    print(f"  ❌ Errors:          {stats['errors']}")


if __name__ == "__main__":
    main()
