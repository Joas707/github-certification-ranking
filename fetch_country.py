#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch GitHub Certifications for a single country
Includes both verified (GitHub org) and unverified (Microsoft external) badges
"""

import csv
import json
import os
import sys
import requests
from datetime import datetime

def fetch_github_external_badges(user_id):
    """Fetch GitHub external badges (Microsoft-issued) for a user"""
    url = f"https://www.credly.com/api/v1/users/{user_id}/external_badges/open_badges/public?page=1&page_size=48"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Filter only GitHub badges issued by Microsoft
        github_badges = []
        for badge in data.get('data', []):
            external_badge = badge.get('external_badge', {})
            badge_name = external_badge.get('badge_name', '')
            issuer_name = external_badge.get('issuer_name', '')
            
            # Check if it's a GitHub certification issued by Microsoft
            if issuer_name == 'Microsoft' and 'GitHub' in badge_name:
                github_badges.append(badge)
        
        return len(github_badges)
    except Exception:
        # If external badges endpoint fails, return 0 (user may have no external badges)
        return 0

def fetch_country_data(country):
    """Fetch all data for a country"""
    base_url = f"https://www.credly.com/api/v1/directory?organization_id=63074953-290b-4dce-86ce-ea04b4187219&sort=alphabetical&filter%5Blocation_name%5D={country.replace(' ', '%20')}&page="
    
    all_users = []
    page = 1
    
    print(f"Fetching data for {country}...")
    
    while True:
        url = f"{base_url}{page}&format=json"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            users = data.get('data', [])
            if not users:
                break
            
            # For each user, add external badges count
            for user in users:
                user_id = user.get('id')
                if user_id:
                    external_count = fetch_github_external_badges(user_id)
                    user['badge_count'] = user.get('badge_count', 0) + external_count
            
            all_users.extend(users)
            print(f"  Page {page}: {len(users)} users")
            page += 1
            
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break
    
    return all_users

def save_to_csv(country, users, output_dir='datasource'):
    """Save users to CSV file"""
    os.makedirs(output_dir, exist_ok=True)
    
    file_suffix = country.lower().replace(' ', '-')
    output_file = f"{output_dir}/github-certs-{file_suffix}.csv"
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['first_name', 'middle_name', 'last_name', 'badge_count'])
        
        for user in users:
            writer.writerow([
                user.get('first_name', ''),
                user.get('middle_name', ''),
                user.get('last_name', ''),
                user.get('badge_count', 0)
            ])
    
    print(f"\nSaved to {output_file}")
    return output_file

def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("Usage: ./fetch_country.py <country_name>")
        print("Example: ./fetch_country.py Brazil")
        print("         ./fetch_country.py \"United States\"")
        sys.exit(1)
    
    country = sys.argv[1]
    
    print("=" * 80)
    print(f"Fetching GitHub certifications for: {country}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    users = fetch_country_data(country)
    
    # Always save CSV, even if empty (to maintain consistency)
    save_to_csv(country, users)
    print()
    print("=" * 80)
    print(f"✅ Success! Downloaded {len(users)} users")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    sys.exit(0)

if __name__ == "__main__":
    main()
