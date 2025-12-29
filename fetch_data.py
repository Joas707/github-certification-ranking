#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch GitHub Certifications data for all countries
Runs cert-github.sh in parallel for all countries in CONTINENT_MAP
"""

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Import CONTINENT_MAP from generate_rankings
from generate_rankings import CONTINENT_MAP

def get_all_countries():
    """Extract unique countries from CONTINENT_MAP"""
    countries = set()
    for country in CONTINENT_MAP.keys():
        # Convert to title case for proper country names
        country_name = country.title()
        countries.add(country_name)
    return sorted(countries)

def fetch_country_data(country):
    """Fetch data for a single country using cert-github.sh"""
    # Larger countries need more time
    large_countries = ['Brazil', 'India', 'United States', 'China', 'Germany', 
                       'United Kingdom', 'France', 'Canada', 'Japan']
    timeout = 900 if country in large_countries else 120  # 15 minutes for large countries
    
    try:
        result = subprocess.run(
            ['./cert-github.sh', country],
            timeout=timeout,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            return (country, 'success', None)
        else:
            return (country, 'failed', f"Exit code: {result.returncode}")
    except subprocess.TimeoutExpired:
        return (country, 'failed', f'Timeout ({timeout}s)')
    except Exception as e:
        return (country, 'failed', str(e))

def main():
    """Main execution"""
    print("=" * 80)
    print("GitHub Certifications Data Fetcher")
    print("=" * 80)
    print()
    
    # Get all countries
    countries = get_all_countries()
    total_countries = len(countries)
    
    print(f"📋 Found {total_countries} countries to process")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Process countries in parallel
    max_workers = 10  # Maximum concurrent downloads
    success_count = 0
    failed_countries = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_country = {
            executor.submit(fetch_country_data, country): country 
            for country in countries
        }
        
        # Process results as they complete
        for i, future in enumerate(as_completed(future_to_country), 1):
            country, status, error = future.result()
            
            if status == 'success':
                print(f"✓ [{i}/{total_countries}] Success: {country}")
                success_count += 1
            else:
                print(f"✗ [{i}/{total_countries}] Failed: {country} ({error})")
                failed_countries.append(country)
    
    print()
    print("=" * 80)
    print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✅ Success: {success_count}/{total_countries}")
    print(f"❌ Failed: {len(failed_countries)}/{total_countries}")
    
    if failed_countries:
        print()
        print("Failed countries:")
        for country in failed_countries:
            print(f"  - {country}")
    
    print("=" * 80)
    
    # Return non-zero exit code if any failures
    sys.exit(0 if len(failed_countries) == 0 else 1)

if __name__ == "__main__":
    main()
