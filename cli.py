#!/usr/bin/env python3
"""
Y-DNA Propagator CLI

Command-line interface for the Y-DNA Propagator application.
"""

import argparse
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path

from propagator import YDNAPropagator
from database import Database
from haplotree import Haplotree, print_comparison


def generate_tree_filename(profile: dict, prefix: str = "tree") -> str:
    """Generate unique filename based on profile name and ID."""
    name = get_name(profile)
    # Clean name for filename (remove special chars, replace spaces with underscore)
    clean_name = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')

    profile_id = profile.get("id") or profile.get("geni_id", "unknown")
    # Extract just the numeric part if it's like "profile-15611"
    if profile_id.startswith("profile-"):
        profile_id = profile_id.replace("profile-", "")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    return f"{prefix}_{clean_name}_{profile_id}_{timestamp}.csv"


def get_name(profile: dict) -> str:
    """Get display name from profile, trying various fields."""
    if profile.get("display_name"):
        return profile["display_name"]
    if profile.get("name"):
        return profile["name"]
    parts = []
    if profile.get("first_name"):
        parts.append(profile["first_name"])
    if profile.get("last_name"):
        parts.append(profile["last_name"])
    if parts:
        return " ".join(parts)
    return "Unknown"


def cmd_auth(args):
    """Authenticate with Geni API."""
    propagator = YDNAPropagator(args.config)

    if args.code:
        success = propagator.authenticate(args.code)
    else:
        success = propagator.authenticate()

    propagator.close()
    return 0 if success else 1


def cmd_profile(args):
    """Fetch and display a profile."""
    propagator = YDNAPropagator(args.config)

    if not propagator.authenticate():
        print("Authentication failed.")
        return 1

    profile = propagator.fetch_and_save_profile(args.profile_id)

    if profile:
        print(f"\nProfile: {profile.get('display_name', profile.get('name', 'Unknown'))}")
        print(f"  ID: {profile.get('id', profile.get('geni_id'))}")
        print(f"  Gender: {profile.get('gender', 'Unknown')}")

        birth = profile.get("birth", {})
        if birth:
            print(f"  Birth: {birth.get('date', {}).get('formatted_date', 'Unknown')}")
            if birth.get("location"):
                print(f"  Birth place: {birth.get('location', {}).get('place_name', 'Unknown')}")

        death = profile.get("death", {})
        if death:
            print(f"  Death: {death.get('date', {}).get('formatted_date', 'Unknown')}")

        if args.json:
            print(f"\nRaw JSON:\n{json.dumps(profile, indent=2)}")
    else:
        print(f"Profile {args.profile_id} not found.")

    propagator.close()
    return 0


def cmd_family(args):
    """Fetch and display immediate family."""
    propagator = YDNAPropagator(args.config)

    if not propagator.authenticate():
        print("Authentication failed.")
        return 1

    family = propagator.fetch_immediate_family(args.profile_id)

    if family:
        focus = family.get("focus", {})
        print(f"\nFamily of: {focus.get('display_name', focus.get('name', 'Unknown'))}")

        parents = family.get("parents", [])
        if parents:
            print("\nParents:")
            for p in parents:
                gender_marker = "(M)" if p.get("gender") == "male" else "(F)"
                print(f"  {gender_marker} {get_name(p)} - {p.get('id')}")

        partners = family.get("partners", [])
        if partners:
            print("\nPartners:")
            for p in partners:
                print(f"  {get_name(p)} - {p.get('id')}")

        children = family.get("children", [])
        if children:
            print("\nChildren:")
            for c in children:
                gender_marker = "(M)" if c.get("gender") == "male" else "(F)"
                print(f"  {gender_marker} {get_name(c)} - {c.get('id')}")

    propagator.close()
    return 0


def cmd_ancestors(args):
    """Traverse and display paternal ancestors."""
    propagator = YDNAPropagator(args.config)

    if not propagator.authenticate():
        print("Authentication failed.")
        return 1

    ancestors = propagator.traverse_paternal_line_up(
        args.profile_id,
        max_generations=args.generations
    )

    if ancestors:
        print(f"\nFound {len(ancestors)} paternal ancestors")

        if args.export:
            export_profiles_csv(ancestors, args.export)
            print(f"Exported to {args.export}")

    propagator.close()
    return 0


def cmd_descendants(args):
    """Traverse and display paternal descendants."""
    propagator = YDNAPropagator(args.config)

    if not propagator.authenticate():
        print("Authentication failed.")
        return 1

    descendants = propagator.traverse_paternal_line_down(
        args.profile_id,
        max_generations=args.generations
    )

    if descendants:
        print(f"\nFound {len(descendants)} paternal descendants")

        if args.export:
            profiles = [d["profile"] for d in descendants]
            export_profiles_csv(profiles, args.export)
            print(f"Exported to {args.export}")

    propagator.close()
    return 0


def cmd_propagate(args):
    """Propagate a haplogroup along paternal lines."""
    propagator = YDNAPropagator(args.config)

    if not propagator.authenticate():
        print("Authentication failed.")
        return 1

    stats = propagator.propagate_haplogroup(
        args.profile_id,
        args.haplogroup,
        source=args.source,
        propagate_up=not args.down_only,
        propagate_down=not args.up_only
    )

    if args.export:
        # Export all profiles with this haplogroup
        profiles = propagator.db.get_profiles_by_haplogroup(args.haplogroup)
        export_profiles_csv(profiles, args.export)
        print(f"Exported {len(profiles)} profiles to {args.export}")

    propagator.close()
    return 0


def cmd_full_tree(args):
    """Propagate haplogroup from oldest ancestor to ALL descendants."""
    propagator = YDNAPropagator(args.config)

    if not propagator.authenticate():
        print("Authentication failed.")
        return 1

    stats = propagator.propagate_full_tree(
        args.profile_id,
        args.haplogroup,
        source=args.source,
        resume=args.resume
    )

    # Always export - generate unique filename if not specified
    profiles = propagator.db.get_profiles_by_haplogroup(args.haplogroup)

    if args.export:
        export_file = args.export
    else:
        # Auto-generate filename from root ancestor
        root_profile = propagator.db.get_profile(stats["root_profile_id"])
        if root_profile:
            export_file = generate_tree_filename(root_profile, prefix=f"tree_{args.haplogroup}")
        else:
            export_file = f"tree_{args.haplogroup}_{stats['root_profile_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    export_profiles_csv(profiles, export_file)
    print(f"Exported {len(profiles)} profiles to {export_file}")

    propagator.close()
    return 0


def cmd_import_haplogroups(args):
    """Import haplogroups from a CSV file."""
    propagator = YDNAPropagator(args.config)

    if not propagator.authenticate():
        print("Authentication failed.")
        return 1

    with open(args.csv_file, "r") as f:
        reader = csv.DictReader(f)
        count = 0

        for row in reader:
            profile_id = row.get("geni_profile_id") or row.get("profile_id")
            haplogroup = row.get("haplogroup")
            source = row.get("source", "imported")

            if profile_id and haplogroup:
                # Fetch the profile first
                propagator.fetch_and_save_profile(profile_id)

                # Add the haplogroup
                propagator.db.add_haplogroup(
                    profile_id,
                    haplogroup,
                    source,
                    is_tested=True,
                    confidence="confirmed"
                )
                count += 1
                print(f"Imported: {profile_id} = {haplogroup}")

    print(f"\nImported {count} haplogroups")
    propagator.close()
    return 0


def cmd_build_tree(args):
    """Build a complete paternal tree."""
    propagator = YDNAPropagator(args.config)

    if not propagator.authenticate():
        print("Authentication failed.")
        return 1

    tree = propagator.build_paternal_tree(
        args.profile_id,
        haplogroup=args.haplogroup,
        tree_name=args.name
    )

    print(f"\nTree built:")
    print(f"  Name: {tree['tree_name']}")
    print(f"  Root: {tree['root_profile_id']}")
    print(f"  Ancestors: {tree['total_ancestors']}")
    print(f"  Descendants: {tree['total_descendants']}")

    if args.export:
        # Export all profiles in the tree
        profiles = []

        # Get root and ancestors
        root = propagator.db.get_profile(tree['root_profile_id'])
        if root:
            profiles.append(root)

        descendants = propagator.db.get_paternal_descendants(tree['root_profile_id'])
        profiles.extend([d["profile"] for d in descendants])

        export_profiles_csv(profiles, args.export)
        print(f"Exported {len(profiles)} profiles to {args.export}")

    propagator.close()
    return 0


def cmd_stats(args):
    """Display database statistics."""
    db = Database(args.database)
    stats = db.get_statistics()

    print("\nDatabase Statistics:")
    print(f"  Total profiles: {stats['total_profiles']}")
    print(f"  Male profiles: {stats['male_profiles']}")
    print(f"  Paternal links: {stats['paternal_links']}")
    print(f"  Profiles with haplogroup: {stats['profiles_with_haplogroup']}")
    print(f"  Tested haplogroups: {stats['tested_haplogroups']}")
    print(f"  Unique haplogroups: {stats['unique_haplogroups']}")
    print(f"  Paternal trees: {stats['paternal_trees']}")

    db.close()
    return 0


def cmd_search(args):
    """Search for profiles."""
    propagator = YDNAPropagator(args.config)

    if not propagator.authenticate():
        print("Authentication failed.")
        return 1

    results = propagator.client.search_profiles(names=args.name)

    if "results" in results:
        profiles = results["results"]
        print(f"\nFound {len(profiles)} profiles:")
        for p in profiles:
            gender_marker = "(M)" if p.get("gender") == "male" else "(F)"
            birth = p.get("birth", {})
            birth_year = ""
            if birth and birth.get("date"):
                birth_year = f" b.{birth['date'].get('year', '?')}"
            print(f"  {gender_marker} {p.get('name', 'Unknown')}{birth_year} - {p.get('id')}")
    else:
        print("No results found.")

    propagator.close()
    return 0


def cmd_export(args):
    """Export profiles with a specific haplogroup."""
    db = Database(args.database)

    profiles = db.get_profiles_by_haplogroup(args.haplogroup)

    if profiles:
        export_profiles_csv(profiles, args.output)
        print(f"Exported {len(profiles)} profiles to {args.output}")
    else:
        print(f"No profiles found with haplogroup {args.haplogroup}")

    db.close()
    return 0


def cmd_compare(args):
    """Compare two haplogroups to find their relationship."""
    tree = Haplotree(args.haplotree)
    print_comparison(args.haplogroup1, args.haplogroup2, tree)
    return 0


def cmd_haplotree_info(args):
    """Show information about a haplogroup from the FTDNA haplotree."""
    tree = Haplotree(args.haplotree)

    node = tree.find_by_name(args.haplogroup)
    if not node:
        print(f"Haplogroup {args.haplogroup} not found in FTDNA haplotree")
        return 1

    print(f"\n{'='*60}")
    print(f"Haplogroup: {args.haplogroup}")
    print(f"{'='*60}")

    print(f"  Root: {node.get('root', 'Unknown')}")
    print(f"  Sub-branches: {node.get('subBranches', 0)}")
    print(f"  BigY count: {node.get('bigYCount', 0)}")

    # Get ancestry path
    path = tree.get_ancestry_path(args.haplogroup)
    print(f"\nAncestry ({len(path)} levels):")
    for i, (_, name) in enumerate(path[:20]):
        print(f"  {i}: {name}")
    if len(path) > 20:
        print(f"  ... ({len(path) - 20} more)")

    # Show defining SNPs
    variants = node.get('variants', [])
    if variants and args.snps:
        print(f"\nDefining SNPs ({len(variants)}):")
        for v in variants[:10]:
            print(f"  {v.get('variant')}: pos {v.get('position')} ({v.get('ancestral')} -> {v.get('derived')})")
        if len(variants) > 10:
            print(f"  ... ({len(variants) - 10} more)")

    return 0


def export_profiles_csv(profiles: list, filename: str):
    """Export profiles to CSV file."""
    if not profiles:
        return

    fieldnames = [
        "geni_id", "display_name", "first_name", "last_name",
        "gender", "birth_date", "birth_place", "death_date", "death_place",
        "haplogroup", "haplogroup_source"
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for p in profiles:
            row = {
                "geni_id": p.get("geni_id") or p.get("id"),
                "display_name": p.get("display_name") or p.get("name"),
                "first_name": p.get("first_name"),
                "last_name": p.get("last_name"),
                "gender": p.get("gender"),
                "birth_date": p.get("birth_date"),
                "birth_place": p.get("birth_place"),
                "death_date": p.get("death_date"),
                "death_place": p.get("death_place"),
                "haplogroup": p.get("haplogroup"),
                "haplogroup_source": p.get("haplogroup_source")
            }
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(
        description="Y-DNA Propagator - Extend Y-DNA propagation along paternal lines using Geni.com data"
    )
    parser.add_argument("--config", default="config.json", help="Path to config file")
    parser.add_argument("--database", default="ydna_propagator.db", help="Path to database file")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Auth command
    auth_parser = subparsers.add_parser("auth", help="Authenticate with Geni API")
    auth_parser.add_argument("--code", help="Authorization code (if already obtained)")
    auth_parser.set_defaults(func=cmd_auth)

    # Profile command
    profile_parser = subparsers.add_parser("profile", help="Fetch and display a profile")
    profile_parser.add_argument("profile_id", help="Geni profile ID")
    profile_parser.add_argument("--json", action="store_true", help="Show raw JSON")
    profile_parser.set_defaults(func=cmd_profile)

    # Family command
    family_parser = subparsers.add_parser("family", help="Fetch and display immediate family")
    family_parser.add_argument("profile_id", help="Geni profile ID")
    family_parser.set_defaults(func=cmd_family)

    # Ancestors command
    ancestors_parser = subparsers.add_parser("ancestors", help="Traverse paternal ancestors")
    ancestors_parser.add_argument("profile_id", help="Starting profile ID")
    ancestors_parser.add_argument("--generations", "-g", type=int, default=50, help="Max generations")
    ancestors_parser.add_argument("--export", "-e", help="Export to CSV file")
    ancestors_parser.set_defaults(func=cmd_ancestors)

    # Descendants command
    descendants_parser = subparsers.add_parser("descendants", help="Traverse paternal descendants")
    descendants_parser.add_argument("profile_id", help="Starting profile ID")
    descendants_parser.add_argument("--generations", "-g", type=int, default=50, help="Max generations")
    descendants_parser.add_argument("--export", "-e", help="Export to CSV file")
    descendants_parser.set_defaults(func=cmd_descendants)

    # Propagate command
    propagate_parser = subparsers.add_parser("propagate", help="Propagate haplogroup along paternal lines")
    propagate_parser.add_argument("profile_id", help="Source profile with known haplogroup")
    propagate_parser.add_argument("haplogroup", help="Y-DNA haplogroup (e.g., R-M269)")
    propagate_parser.add_argument("--source", "-s", default="FTDNA", help="Source of haplogroup data")
    propagate_parser.add_argument("--up-only", action="store_true", help="Only propagate to ancestors")
    propagate_parser.add_argument("--down-only", action="store_true", help="Only propagate to descendants")
    propagate_parser.add_argument("--export", "-e", help="Export results to CSV file")
    propagate_parser.set_defaults(func=cmd_propagate)

    # Full tree propagation command
    fulltree_parser = subparsers.add_parser("full-tree", help="Propagate from oldest ancestor to ALL descendants")
    fulltree_parser.add_argument("profile_id", help="Any profile in the paternal line")
    fulltree_parser.add_argument("haplogroup", help="Y-DNA haplogroup (e.g., R-M269)")
    fulltree_parser.add_argument("--source", "-s", default="FTDNA", help="Source of haplogroup data")
    fulltree_parser.add_argument("--resume", "-r", action="store_true", help="Resume interrupted propagation")
    fulltree_parser.add_argument("--export", "-e", help="Export results to CSV file")
    fulltree_parser.set_defaults(func=cmd_full_tree)

    # Import haplogroups command
    import_parser = subparsers.add_parser("import", help="Import haplogroups from CSV")
    import_parser.add_argument("csv_file", help="CSV file with profile_id,haplogroup,source columns")
    import_parser.set_defaults(func=cmd_import_haplogroups)

    # Build tree command
    tree_parser = subparsers.add_parser("tree", help="Build a complete paternal tree")
    tree_parser.add_argument("profile_id", help="Starting profile ID")
    tree_parser.add_argument("--haplogroup", help="Haplogroup for the tree")
    tree_parser.add_argument("--name", help="Name for the tree")
    tree_parser.add_argument("--export", "-e", help="Export to CSV file")
    tree_parser.set_defaults(func=cmd_build_tree)

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Display database statistics")
    stats_parser.set_defaults(func=cmd_stats)

    # Search command
    search_parser = subparsers.add_parser("search", help="Search for profiles")
    search_parser.add_argument("name", help="Name to search for")
    search_parser.set_defaults(func=cmd_search)

    # Export command
    export_parser = subparsers.add_parser("export", help="Export profiles by haplogroup")
    export_parser.add_argument("haplogroup", help="Haplogroup to export")
    export_parser.add_argument("output", help="Output CSV file")
    export_parser.set_defaults(func=cmd_export)

    # Compare haplogroups command
    compare_parser = subparsers.add_parser("compare", help="Compare two haplogroups to find their relationship")
    compare_parser.add_argument("haplogroup1", help="First haplogroup (e.g., R-A11110)")
    compare_parser.add_argument("haplogroup2", help="Second haplogroup (e.g., R-BY117398)")
    compare_parser.add_argument("--haplotree", default="ftdna_haplotree.json", help="Path to FTDNA haplotree JSON")
    compare_parser.set_defaults(func=cmd_compare)

    # Haplotree info command
    info_parser = subparsers.add_parser("info", help="Show information about a haplogroup")
    info_parser.add_argument("haplogroup", help="Haplogroup name (e.g., R-M269)")
    info_parser.add_argument("--snps", action="store_true", help="Show defining SNPs")
    info_parser.add_argument("--haplotree", default="ftdna_haplotree.json", help="Path to FTDNA haplotree JSON")
    info_parser.set_defaults(func=cmd_haplotree_info)

    # Run interactive command
    run_parser = subparsers.add_parser("run", help="Interactive mode - prompts for profile and haplogroup")
    run_parser.set_defaults(func=cmd_run_interactive)

    args = parser.parse_args()

    if not args.command:
        # Default to interactive mode
        args.command = "run"
        args.config = "config.json"
        args.database = "ydna_propagator.db"
        return cmd_run_interactive(args)

    return args.func(args)


def cmd_run_interactive(args):
    """Interactive mode - prompts for profile ID and haplogroup."""
    print("=" * 60)
    print("Y-DNA Propagator - Interactive Mode")
    print("=" * 60)
    print()

    propagator = YDNAPropagator(args.config)

    if not propagator.authenticate():
        print("Authentication failed. Please check your credentials.")
        return 1

    # Get current user info
    try:
        user = propagator.client.get_user()
        print(f"Authenticated as: {user.get('name', 'Unknown')}")
        print()
    except Exception:
        pass

    # Prompt for profile ID
    print("Enter the Geni profile ID of the person with known Y-DNA.")
    print("(Copy the number from the profile URL, e.g., 6000000040364004409)")
    print()
    profile_id = input("Profile ID: ").strip()

    if not profile_id:
        print("No profile ID provided. Exiting.")
        propagator.close()
        return 1

    # Normalize profile ID using client's method (handles GUIDs)
    profile_id = propagator.client.normalize_profile_id(profile_id)

    # Fetch and display the profile
    print()
    print(f"Fetching profile {profile_id}...")
    try:
        profile = propagator.client.get_profile(profile_id)
        profile_name = profile.get("display_name") or profile.get("name") or "Unknown"
        gender = profile.get("gender", "unknown")
        print(f"Found: {profile_name} ({gender})")

        if gender != "male":
            print("Warning: Y-DNA is only inherited through the male line.")
            print("This profile is not male. Continue anyway? (y/n)")
            if input().strip().lower() != 'y':
                propagator.close()
                return 1
    except Exception as e:
        print(f"Error fetching profile: {e}")
        propagator.close()
        return 1

    # Prompt for haplogroup
    print()
    print("Enter the Y-DNA haplogroup (e.g., R-BY117398, I-M253, N-M231):")
    haplogroup = input("Haplogroup: ").strip()

    if not haplogroup:
        print("No haplogroup provided. Exiting.")
        propagator.close()
        return 1

    # Prompt for source
    print()
    print("Enter the source of the haplogroup data (default: FTDNA):")
    source = input("Source [FTDNA]: ").strip() or "FTDNA"

    # Check if this haplogroup has previous progress
    explored_count = propagator.db.get_explored_count(haplogroup)
    resume = False

    if explored_count > 0:
        print()
        print(f"Found {explored_count} previously explored profiles for {haplogroup}.")
        print("Do you want to resume from where you left off? (y/n)")
        resume = input().strip().lower() == 'y'

    # Confirm and run
    print()
    print("=" * 60)
    print("Ready to propagate:")
    print(f"  Profile: {profile_name} ({profile_id})")
    print(f"  Haplogroup: {haplogroup}")
    print(f"  Source: {source}")
    if resume:
        print(f"  Mode: RESUME (skipping {explored_count} explored profiles)")
    print("=" * 60)
    print()
    print("This will:")
    print("  1. Find the oldest paternal ancestor")
    print("  2. Propagate the haplogroup to ALL male descendants")
    if resume:
        print("  3. Skip profiles already explored")
    print("  3. Save results to a CSV file")
    print()
    print("Continue? (y/n)")

    if input().strip().lower() != 'y':
        print("Cancelled.")
        propagator.close()
        return 0

    # Run full tree propagation
    print()
    stats = propagator.propagate_full_tree(profile_id, haplogroup, source=source, resume=resume)

    # Export results
    profiles = propagator.db.get_profiles_by_haplogroup(haplogroup)
    root_profile = propagator.db.get_profile(stats["root_profile_id"])

    if root_profile:
        export_file = generate_tree_filename(root_profile, prefix=f"tree_{haplogroup}")
    else:
        export_file = f"tree_{haplogroup}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    export_profiles_csv(profiles, export_file)

    print()
    print("=" * 60)
    print("Complete!")
    print(f"  Profiles with {haplogroup}: {len(profiles)}")
    print(f"  Exported to: {export_file}")
    print("=" * 60)

    propagator.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
