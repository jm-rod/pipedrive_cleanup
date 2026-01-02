"""
Pipedrive Data Sync Script
--------------------------
Uses Postgres data as source of truth to:
1. Create missing organizations in Pipedrive
2. Link persons to correct organizations
3. Populate "All LIGR Organizations" custom field for multi-org users
4. Tag Pipedrive-only contacts as "not in db"

Run: python pipedrive_sync.py
"""

import csv
import requests
import time
import json
from datetime import datetime
from collections import defaultdict

# =============================================================================
# CONFIGURATION
# =============================================================================

import os
API_TOKEN = os.environ.get("PIPEDRIVE_API_TOKEN")
BASE_URL = "https://api.pipedrive.com/v1"
CSV_PATH = "query_result_2026-01-01T07_09_46_49449815Z.csv"

# Rate limiting - Pipedrive allows 100 requests per 10 seconds
REQUEST_DELAY = 0.12  # seconds between requests

# Custom field names
ALL_ORGS_FIELD_NAME = "All LIGR Organizations"
DB_STATUS_FIELD_NAME = "DB Status"

# Dry run mode - set to False to make actual changes
DRY_RUN = False

# =============================================================================
# API HELPERS
# =============================================================================

request_count = 0

def api_get(endpoint, params=None):
    """Make a GET request to Pipedrive API"""
    global request_count
    if params is None:
        params = {}
    params['api_token'] = API_TOKEN
    response = requests.get(f"{BASE_URL}/{endpoint}", params=params)
    request_count += 1
    time.sleep(REQUEST_DELAY)
    return response.json()

def api_post(endpoint, data):
    """Make a POST request to Pipedrive API"""
    global request_count
    params = {'api_token': API_TOKEN}
    response = requests.post(f"{BASE_URL}/{endpoint}", params=params, json=data)
    request_count += 1
    time.sleep(REQUEST_DELAY)
    return response.json()

def api_put(endpoint, data):
    """Make a PUT request to Pipedrive API"""
    global request_count
    params = {'api_token': API_TOKEN}
    response = requests.put(f"{BASE_URL}/{endpoint}", params=params, json=data)
    request_count += 1
    time.sleep(REQUEST_DELAY)
    return response.json()

def fetch_all_paginated(endpoint, entity_name="items"):
    """Fetch all records from a paginated Pipedrive endpoint"""
    all_items = []
    start = 0
    limit = 500
    
    while True:
        result = api_get(endpoint, {'start': start, 'limit': limit})
        
        if not result.get('success') or not result.get('data'):
            break
            
        all_items.extend(result['data'])
        print(f"  Fetched {len(all_items)} {entity_name}...", end='\r')
        
        pagination = result.get('additional_data', {}).get('pagination', {})
        if not pagination.get('more_items_in_collection'):
            break
        
        start = pagination.get('next_start', start + limit)
    
    print(f"  Fetched {len(all_items)} {entity_name} total    ")
    return all_items

# =============================================================================
# DATA LOADING
# =============================================================================

def load_postgres_data(csv_path):
    """Load and process Postgres CSV export"""
    print(f"\n📂 Loading Postgres data from {csv_path}...")
    
    # User lookup: email -> {name, user_id, orgs: [{name, id}, ...]}
    users = {}
    # Org lookup: org_name_lower -> {name, id}
    orgs = {}
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        row_count = 0
        
        for row in reader:
            row_count += 1
            email = row['Email'].lower().strip()
            org_name = row['Organization Name'].strip()
            org_id = int(row['Organization ID'])
            user_id = int(row['User ID'])
            full_name = row['Full Name'].strip()
            
            # Add to orgs lookup
            org_key = org_name.lower()
            if org_key not in orgs:
                orgs[org_key] = {'name': org_name, 'id': org_id}
            
            # Add to users lookup
            if email not in users:
                users[email] = {
                    'name': full_name,
                    'user_id': user_id,
                    'orgs': []
                }
            
            # Add org if not already in user's list
            org_exists = any(o['id'] == org_id for o in users[email]['orgs'])
            if not org_exists:
                users[email]['orgs'].append({'name': org_name, 'id': org_id})
    
    print(f"  Loaded {row_count} rows")
    print(f"  Unique users: {len(users)}")
    print(f"  Unique organizations: {len(orgs)}")
    
    # Stats on multi-org users
    multi_org_users = sum(1 for u in users.values() if len(u['orgs']) > 1)
    print(f"  Users with multiple orgs: {multi_org_users}")
    
    return users, orgs

def load_pipedrive_data():
    """Fetch all persons and organizations from Pipedrive"""
    print("\n📡 Fetching Pipedrive data...")
    
    print("  Organizations:")
    pd_orgs = fetch_all_paginated('organizations', 'organizations')
    
    print("  Persons:")
    pd_persons = fetch_all_paginated('persons', 'persons')
    
    # Build lookups
    org_by_name = {}
    org_by_id = {}
    for org in pd_orgs:
        name_lower = org['name'].lower().strip()
        org_by_name[name_lower] = org
        org_by_id[org['id']] = org
    
    person_by_email = {}
    for person in pd_persons:
        emails = person.get('email', [])
        if emails:
            for email_obj in emails:
                if isinstance(email_obj, dict):
                    email = email_obj.get('value', '').lower().strip()
                else:
                    email = str(email_obj).lower().strip()
                if email:
                    person_by_email[email] = person
    
    print(f"\n  Pipedrive organizations: {len(org_by_name)}")
    print(f"  Pipedrive persons (with email): {len(person_by_email)}")
    
    return org_by_name, org_by_id, person_by_email, pd_persons

# =============================================================================
# CUSTOM FIELDS
# =============================================================================

def get_or_create_custom_fields():
    """Get or create the custom fields we need"""
    print("\n🔧 Setting up custom fields...")
    
    # Get existing person fields
    result = api_get('personFields')
    person_fields = result.get('data', []) if result.get('success') else []
    
    all_orgs_field_key = None
    db_status_field_key = None
    
    for field in person_fields:
        if field['name'] == ALL_ORGS_FIELD_NAME:
            all_orgs_field_key = field['key']
            print(f"  Found existing '{ALL_ORGS_FIELD_NAME}' field: {all_orgs_field_key}")
        if field['name'] == DB_STATUS_FIELD_NAME:
            db_status_field_key = field['key']
            print(f"  Found existing '{DB_STATUS_FIELD_NAME}' field: {db_status_field_key}")
    
    # Create "All LIGR Organizations" field if needed
    if not all_orgs_field_key:
        if DRY_RUN:
            print(f"  [DRY RUN] Would create '{ALL_ORGS_FIELD_NAME}' field")
            all_orgs_field_key = "DRY_RUN_ALL_ORGS_KEY"
        else:
            result = api_post('personFields', {
                'name': ALL_ORGS_FIELD_NAME,
                'field_type': 'text'
            })
            if result.get('success'):
                all_orgs_field_key = result['data']['key']
                print(f"  Created '{ALL_ORGS_FIELD_NAME}' field: {all_orgs_field_key}")
            else:
                print(f"  ❌ Failed to create field: {result}")
    
    # Create "DB Status" field if needed
    if not db_status_field_key:
        if DRY_RUN:
            print(f"  [DRY RUN] Would create '{DB_STATUS_FIELD_NAME}' field")
            db_status_field_key = "DRY_RUN_DB_STATUS_KEY"
        else:
            result = api_post('personFields', {
                'name': DB_STATUS_FIELD_NAME,
                'field_type': 'varchar'
            })
            if result.get('success'):
                db_status_field_key = result['data']['key']
                print(f"  Created '{DB_STATUS_FIELD_NAME}' field: {db_status_field_key}")
            else:
                print(f"  ❌ Failed to create field: {result}")
    
    return all_orgs_field_key, db_status_field_key

# =============================================================================
# SYNC LOGIC
# =============================================================================

def create_missing_orgs(postgres_orgs, pd_org_by_name):
    """Create organizations that exist in Postgres but not in Pipedrive"""
    print("\n🏢 Creating missing organizations...")
    
    postgres_org_names = set(postgres_orgs.keys())
    pipedrive_org_names = set(pd_org_by_name.keys())
    
    missing_orgs = postgres_org_names - pipedrive_org_names
    print(f"  Organizations to create: {len(missing_orgs)}")
    
    created = []
    failed = []
    
    for i, org_key in enumerate(missing_orgs):
        org_data = postgres_orgs[org_key]
        
        if DRY_RUN:
            if i < 10:
                print(f"  [DRY RUN] Would create: {org_data['name']}")
            elif i == 10:
                print(f"  ... and {len(missing_orgs) - 10} more")
            # Add to lookup for dry run
            pd_org_by_name[org_key] = {
                'id': f"DRY_RUN_{org_data['id']}",
                'name': org_data['name']
            }
            created.append(org_data)
        else:
            result = api_post('organizations', {'name': org_data['name']})
            
            if result.get('success'):
                new_org = result['data']
                pd_org_by_name[org_key] = new_org
                created.append({'name': org_data['name'], 'pd_id': new_org['id']})
                if (i + 1) % 50 == 0:
                    print(f"  Progress: {i + 1}/{len(missing_orgs)}")
            else:
                failed.append({'name': org_data['name'], 'error': result})
                print(f"  ❌ Failed: {org_data['name']}")
    
    print(f"  ✅ Created: {len(created)}")
    if failed:
        print(f"  ❌ Failed: {len(failed)}")
    
    return created, failed

def sync_persons(postgres_users, pd_person_by_email, pd_org_by_name, pd_org_by_id, 
                 all_orgs_field_key, db_status_field_key):
    """Sync person records - update org links and custom fields"""
    print("\n👥 Syncing persons...")
    
    postgres_emails = set(postgres_users.keys())
    pipedrive_emails = set(pd_person_by_email.keys())
    
    in_both = postgres_emails & pipedrive_emails
    only_pipedrive = pipedrive_emails - postgres_emails
    
    print(f"  Persons in both systems: {len(in_both)}")
    print(f"  Persons only in Pipedrive (to tag): {len(only_pipedrive)}")
    
    # Track changes for logging
    changes = []
    
    # Process persons in both systems
    print("\n  Processing persons in Postgres...")
    updated = 0
    skipped = 0
    failed = 0
    
    for i, email in enumerate(in_both):
        postgres_user = postgres_users[email]
        pd_person = pd_person_by_email[email]
        
        # Build the "All LIGR Organizations" value
        all_orgs_value = ", ".join([
            f"{org['name']} ({org['id']})" 
            for org in postgres_user['orgs']
        ])
        
        # Get current Pipedrive org
        current_org_id = pd_person.get('org_id')
        current_org_name = None
        if current_org_id and current_org_id in pd_org_by_id:
            current_org_name = pd_org_by_id[current_org_id]['name']
        
        # Determine if current org matches any Postgres org
        postgres_org_names_lower = [o['name'].lower() for o in postgres_user['orgs']]
        current_matches = current_org_name and current_org_name.lower() in postgres_org_names_lower
        
        # Determine new org
        if current_matches:
            # Keep current org
            new_org_id = current_org_id
            new_org_name = current_org_name
            org_action = "kept"
        else:
            # Use first Postgres org
            primary_org = postgres_user['orgs'][0]
            primary_org_key = primary_org['name'].lower()
            
            if primary_org_key in pd_org_by_name:
                pd_org = pd_org_by_name[primary_org_key]
                new_org_id = pd_org['id']
                new_org_name = pd_org['name']
                org_action = "updated"
            else:
                # Org not in Pipedrive (shouldn't happen after create step)
                new_org_id = current_org_id
                new_org_name = current_org_name
                org_action = "skipped (org not found)"
        
        # Build update data
        update_data = {}
        
        # Update org if changed
        if new_org_id != current_org_id and new_org_id is not None:
            update_data['org_id'] = new_org_id
        
        # Always update the all orgs field
        if all_orgs_field_key and not all_orgs_field_key.startswith("DRY_RUN"):
            update_data[all_orgs_field_key] = all_orgs_value
        
        # Record change
        change_record = {
            'email': email,
            'name': pd_person.get('name'),
            'pd_person_id': pd_person['id'],
            'current_org': current_org_name,
            'new_org': new_org_name,
            'org_action': org_action,
            'all_orgs': all_orgs_value,
            'status': 'pending'
        }
        
        if update_data:
            if DRY_RUN:
                change_record['status'] = 'dry_run'
                if i < 10:
                    print(f"  [DRY RUN] {email}: org {org_action} -> {new_org_name}")
                elif i == 10:
                    print(f"  ... processing {len(in_both)} total")
                updated += 1
            else:
                result = api_put(f"persons/{pd_person['id']}", update_data)
                if result.get('success'):
                    change_record['status'] = 'success'
                    updated += 1
                else:
                    change_record['status'] = 'failed'
                    change_record['error'] = str(result)
                    failed += 1
                
                if (i + 1) % 100 == 0:
                    print(f"  Progress: {i + 1}/{len(in_both)}")
        else:
            change_record['status'] = 'skipped'
            skipped += 1
        
        changes.append(change_record)
    
    print(f"\n  ✅ Updated: {updated}")
    print(f"  ⏭️  Skipped: {skipped}")
    if failed:
        print(f"  ❌ Failed: {failed}")
    
    # Tag Pipedrive-only persons
    print(f"\n  Tagging {len(only_pipedrive)} Pipedrive-only contacts as 'not in db'...")
    tagged = 0
    tag_failed = 0
    
    for i, email in enumerate(only_pipedrive):
        pd_person = pd_person_by_email[email]
        
        change_record = {
            'email': email,
            'name': pd_person.get('name'),
            'pd_person_id': pd_person['id'],
            'current_org': pd_org_by_id.get(pd_person.get('org_id'), {}).get('name'),
            'new_org': None,
            'org_action': 'tagged_not_in_db',
            'all_orgs': None,
            'status': 'pending'
        }
        
        if DRY_RUN:
            change_record['status'] = 'dry_run'
            if i < 10:
                print(f"  [DRY RUN] Would tag: {email}")
            elif i == 10:
                print(f"  ... and {len(only_pipedrive) - 10} more")
            tagged += 1
        else:
            if db_status_field_key:
                result = api_put(f"persons/{pd_person['id']}", {
                    db_status_field_key: 'not in db'
                })
                if result.get('success'):
                    change_record['status'] = 'success'
                    tagged += 1
                else:
                    change_record['status'] = 'failed'
                    change_record['error'] = str(result)
                    tag_failed += 1
                
                if (i + 1) % 100 == 0:
                    print(f"  Progress: {i + 1}/{len(only_pipedrive)}")
        
        changes.append(change_record)
    
    print(f"\n  ✅ Tagged: {tagged}")
    if tag_failed:
        print(f"  ❌ Failed: {tag_failed}")
    
    return changes

# =============================================================================
# LOGGING
# =============================================================================

def write_changelog(changes, created_orgs, failed_orgs):
    """Write a CSV log of all changes"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"pipedrive_sync_log_{timestamp}.csv"
    
    print(f"\n📝 Writing changelog to {filename}...")
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'email', 'name', 'pd_person_id', 'current_org', 'new_org', 
            'org_action', 'all_orgs', 'status', 'error'
        ])
        writer.writeheader()
        
        for change in changes:
            writer.writerow({
                'email': change.get('email'),
                'name': change.get('name'),
                'pd_person_id': change.get('pd_person_id'),
                'current_org': change.get('current_org'),
                'new_org': change.get('new_org'),
                'org_action': change.get('org_action'),
                'all_orgs': change.get('all_orgs'),
                'status': change.get('status'),
                'error': change.get('error', '')
            })
    
    print(f"  Written {len(changes)} records")
    
    # Also write org creation log
    org_filename = f"pipedrive_orgs_created_{timestamp}.csv"
    with open(org_filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['name', 'pd_id', 'status'])
        writer.writeheader()
        for org in created_orgs:
            writer.writerow({'name': org.get('name'), 'pd_id': org.get('pd_id', 'N/A'), 'status': 'created'})
        for org in failed_orgs:
            writer.writerow({'name': org.get('name'), 'pd_id': '', 'status': 'failed'})
    
    print(f"  Written org log: {org_filename}")
    
    return filename, org_filename

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("PIPEDRIVE DATA SYNC")
    print("=" * 60)
    print(f"\nDRY RUN MODE: {DRY_RUN}")
    if DRY_RUN:
        print("⚠️  No changes will be made. Set DRY_RUN = False to execute.")
    
    # Test connection
    print("\n🔌 Testing API connection...")
    result = api_get("users/me")
    if result.get('success'):
        print(f"  Connected as: {result['data']['name']}")
    else:
        print(f"  ❌ Connection failed: {result}")
        return
    
    # Load data
    postgres_users, postgres_orgs = load_postgres_data(CSV_PATH)
    pd_org_by_name, pd_org_by_id, pd_person_by_email, _ = load_pipedrive_data()
    
    # Setup custom fields
    all_orgs_field_key, db_status_field_key = get_or_create_custom_fields()
    
    # Create missing orgs
    created_orgs, failed_orgs = create_missing_orgs(postgres_orgs, pd_org_by_name)
    
    # Sync persons
    changes = sync_persons(
        postgres_users, pd_person_by_email, pd_org_by_name, pd_org_by_id,
        all_orgs_field_key, db_status_field_key
    )
    
    # Write changelog
    log_file, org_log_file = write_changelog(changes, created_orgs, failed_orgs)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"\nAPI requests made: {request_count}")
    print(f"Log files: {log_file}, {org_log_file}")
    
    if DRY_RUN:
        print("\n⚠️  DRY RUN - No changes were made.")
        print("   Review the log files, then set DRY_RUN = False and run again.")
    else:
        print("\n✅ Sync complete!")

if __name__ == "__main__":
    main()
