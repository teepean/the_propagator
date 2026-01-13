#!/usr/bin/env python3
"""
Dump all available data from a Geni profile via API.
"""

import json
import time
from datetime import datetime
from geni_client import GeniClient


def dump_all_data(profile_id: str, output_file: str = None):
    """Download all available data for a profile."""

    client = GeniClient()

    if not client.access_token:
        print("Not authenticated. Please run authentication first.")
        return

    profile_id = client.normalize_profile_id(profile_id)

    data = {
        "dump_timestamp": datetime.now().isoformat(),
        "profile_id": profile_id,
        "endpoints": {}
    }

    endpoints = [
        # Profile endpoints
        (f"{profile_id}", "profile"),
        (f"{profile_id}/immediate-family", "immediate_family"),
        (f"{profile_id}/ancestors?generations=20", "ancestors_20gen"),
        (f"{profile_id}/documents", "documents"),
        (f"{profile_id}/photos", "photos"),
        (f"{profile_id}/videos", "videos"),

        # User endpoints (for the authenticated user)
        ("user", "user"),
        ("user/metadata", "user_metadata"),
        ("user/followed-profiles", "followed_profiles"),
        ("user/followed-projects", "followed_projects"),
        ("user/followed-documents", "followed_documents"),
        ("user/followed-surnames", "followed_surnames"),
        ("user/managed-profiles", "managed_profiles"),
        ("user/uploaded-documents", "uploaded_documents"),
        ("user/uploaded-photos", "uploaded_photos"),
        ("user/uploaded-videos", "uploaded_videos"),
        ("user/my-albums", "my_albums"),
        ("user/my-labels", "my_labels"),
        ("user/max-family", "max_family"),

        # Stats
        ("stats/world-family-tree", "world_tree_stats"),
    ]

    for endpoint, name in endpoints:
        print(f"Fetching {name}...")
        try:
            time.sleep(2)  # Rate limiting
            result = client._make_request(endpoint)
            data["endpoints"][name] = {
                "status": "success",
                "endpoint": endpoint,
                "data": result
            }
            print(f"  OK - {type(result).__name__}")
        except Exception as e:
            error_msg = str(e)
            data["endpoints"][name] = {
                "status": "error",
                "endpoint": endpoint,
                "error": error_msg
            }
            print(f"  Error: {error_msg[:60]}")

    # Generate output filename if not provided
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"geni_dump_{profile_id.replace('profile-', '')}_{timestamp}.json"

    # Save to file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nData saved to: {output_file}")

    # Summary
    success = sum(1 for v in data["endpoints"].values() if v["status"] == "success")
    failed = sum(1 for v in data["endpoints"].values() if v["status"] == "error")
    print(f"Summary: {success} successful, {failed} failed")

    return data


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        profile_id = sys.argv[1]
    else:
        profile_id = input("Enter profile ID or GUID: ").strip()

    dump_all_data(profile_id)
