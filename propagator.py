"""
Y-DNA Propagator Engine

Core logic for traversing paternal lines and propagating Y-DNA haplogroups.
"""

import json
import time
from typing import Optional

from geni_client import GeniClient
from database import Database


def get_name(profile: dict) -> str:
    """Get display name from profile."""
    if profile.get("display_name"):
        return profile["display_name"]
    if profile.get("name"):
        return profile["name"]
    parts = []
    if profile.get("first_name"):
        parts.append(profile["first_name"])
    if profile.get("last_name"):
        parts.append(profile["last_name"])
    return " ".join(parts) if parts else "Unknown"


class YDNAPropagator:
    """Engine for traversing paternal lines and propagating Y-DNA haplogroups."""

    def __init__(self, config_path: str = "config.json"):
        with open(config_path, "r") as f:
            self.config = json.load(f)

        self.client = GeniClient(config_path)
        self.db = Database(self.config["database"]["path"])
        self.max_gen_up = self.config["propagation"]["max_generations_up"]
        self.max_gen_down = self.config["propagation"]["max_generations_down"]

        # Rate limiting - geni.com has strict limits
        self.request_delay = self.config.get("rate_limit", {}).get("delay", 2.0)
        self.last_request_time = 0

    def _rate_limit(self):
        """Ensure we don't exceed API rate limits."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_delay:
            wait = self.request_delay - elapsed
            print(f"    [Rate limit: waiting {wait:.1f}s]", flush=True)
            time.sleep(wait)
        self.last_request_time = time.time()

    def authenticate(self, auth_code: str = None) -> bool:
        """Authenticate with Geni API."""
        return self.client.authenticate(auth_code)

    def fetch_and_save_profile(self, profile_id: str) -> Optional[dict]:
        """
        Fetch a profile from Geni and save to local database.

        Returns the saved profile data.
        """
        # Check if we already have this profile
        existing = self.db.get_profile(profile_id)
        if existing:
            return existing

        self._rate_limit()

        try:
            print(f"    [Fetching profile: {profile_id}]", flush=True)
            profile_data = self.client.get_profile(profile_id)
            self.db.save_profile(profile_data)
            name = get_name(profile_data)
            print(f"    [Saved: {name}]", flush=True)
            return profile_data
        except Exception as e:
            print(f"    [Error fetching profile {profile_id}: {e}]")
            return None

    def fetch_immediate_family(self, profile_id: str) -> dict:
        """
        Fetch immediate family and save all profiles/unions to database.

        Returns dict with parsed family relationships.
        """
        print(f"    [Fetching family: {profile_id}]", flush=True)
        self._rate_limit()

        try:
            family_data = self.client.get_immediate_family(profile_id)
        except Exception as e:
            print(f"    [Error fetching family for {profile_id}: {e}]")
            return {}

        nodes = family_data.get("nodes", {})
        focus = family_data.get("focus", {})

        # Save the focus profile
        if focus:
            self.db.save_profile(focus)

        # Parse and save all nodes
        profiles = {}
        unions = {}

        for node_id, node_data in nodes.items():
            if node_id.startswith("profile-"):
                self.db.save_profile(node_data)
                profiles[node_id] = node_data
            elif node_id.startswith("union-"):
                self.db.save_union(node_data)
                unions[node_id] = node_data

        # Extract family relationships from edges
        result = {
            "focus": focus,
            "parents": [],
            "children": [],
            "partners": [],
            "siblings": []
        }

        focus_id = focus.get("id") if focus else profile_id
        if not focus_id.startswith("profile-"):
            focus_id = f"profile-{focus_id}"

        # Get focus profile's edges to find which unions they belong to
        focus_node = profiles.get(focus_id, {})
        focus_edges = focus_node.get("edges", {})

        # Find unions where focus is a child (to find parents)
        # Find unions where focus is a partner (to find children)
        for union_id, edge_info in focus_edges.items():
            if not union_id.startswith("union-"):
                continue

            union_node = unions.get(union_id, {})
            union_edges = union_node.get("edges", {})

            if edge_info.get("rel") == "child":
                # Focus is a child in this union - find the partners (parents)
                for profile_id_in_union, rel_info in union_edges.items():
                    if profile_id_in_union.startswith("profile-") and rel_info.get("rel") == "partner":
                        if profile_id_in_union in profiles:
                            result["parents"].append(profiles[profile_id_in_union])

            elif edge_info.get("rel") == "partner":
                # Focus is a partner in this union - find children and other partners
                for profile_id_in_union, rel_info in union_edges.items():
                    if not profile_id_in_union.startswith("profile-"):
                        continue
                    if profile_id_in_union == focus_id:
                        continue

                    if rel_info.get("rel") == "child":
                        if profile_id_in_union in profiles:
                            result["children"].append(profiles[profile_id_in_union])
                    elif rel_info.get("rel") == "partner":
                        if profile_id_in_union in profiles:
                            result["partners"].append(profiles[profile_id_in_union])

        return result

    def _extract_id(self, ref) -> str:
        """Extract profile ID from various reference formats."""
        if isinstance(ref, str):
            if ref.startswith("profile-"):
                return ref
            elif "/" in ref:
                # URL format
                return ref.split("/")[-1]
            else:
                return f"profile-{ref}"
        elif isinstance(ref, dict):
            return ref.get("id", "")
        return ""

    def get_father(self, profile_id: str) -> Optional[dict]:
        """
        Get the father of a profile.

        First checks local DB, then fetches from Geni if needed.
        """
        # Check local DB first
        father = self.db.get_father(profile_id)
        if father:
            return father

        # Fetch from Geni
        family = self.fetch_immediate_family(profile_id)
        parents = family.get("parents", [])
        focus = family.get("focus", {})

        # Get the actual child ID from the focus (API normalizes IDs)
        actual_child_id = focus.get("id") if focus else profile_id

        # Ensure child profile is saved first
        if focus:
            self.db.save_profile(focus)

        # Find the male parent
        for parent in parents:
            if parent.get("gender") == "male":
                parent_id = parent.get("id")
                # Save parent profile before creating link
                self.db.save_profile(parent)
                # Create paternal link using actual IDs
                self.db.add_paternal_link(parent_id, actual_child_id)
                return parent

        return None

    def get_sons(self, profile_id: str, force_fetch: bool = False) -> list:
        """
        Get all sons of a profile.

        Args:
            profile_id: The Geni profile ID
            force_fetch: If True, always fetch from Geni (for full tree discovery)

        First checks local DB, then fetches from Geni if needed.
        """
        # Check local DB first (unless force_fetch)
        if not force_fetch:
            sons = self.db.get_sons(profile_id)
            if sons:
                return sons

        # Fetch from Geni
        family = self.fetch_immediate_family(profile_id)
        children = family.get("children", [])
        focus = family.get("focus", {})

        # Get actual parent ID from API response
        actual_parent_id = focus.get("id") if focus else profile_id

        # Ensure parent profile exists in DB
        if focus:
            self.db.save_profile(focus)

        # Find male children
        sons = []
        for child in children:
            if child.get("gender") == "male":
                child_id = child.get("id")
                # Ensure child profile is saved before creating link
                self.db.save_profile(child)
                # Create paternal link using actual IDs
                self.db.add_paternal_link(actual_parent_id, child_id)
                sons.append(child)

        return sons

    def traverse_paternal_line_up(self, start_profile_id: str,
                                   max_generations: int = None,
                                   callback=None) -> list:
        """
        Traverse paternal line upward (ancestors).

        Args:
            start_profile_id: Starting profile Geni ID
            max_generations: Maximum generations to traverse
            callback: Optional function called for each profile found

        Returns:
            List of ancestor profiles in order (father, grandfather, ...)
        """
        if max_generations is None:
            max_generations = self.max_gen_up

        ancestors = []
        current_id = start_profile_id
        generation = 0

        print(f"\nTraversing paternal line UP from {start_profile_id}")

        while generation < max_generations:
            father = self.get_father(current_id)

            if not father:
                print(f"  Generation {generation}: No father found, end of line")
                break

            father_id = father.get("id") or father.get("geni_id")
            father_name = get_name(father)

            print(f"  Generation {generation + 1}: {father_name} ({father_id})")
            ancestors.append(father)

            if callback:
                callback(father, generation + 1, "up")

            current_id = father_id
            generation += 1

        print(f"Found {len(ancestors)} paternal ancestors")
        return ancestors

    def traverse_paternal_line_down(self, start_profile_id: str,
                                     max_generations: int = None,
                                     callback=None) -> list:
        """
        Traverse paternal line downward (descendants).

        Args:
            start_profile_id: Starting profile Geni ID
            max_generations: Maximum generations to traverse
            callback: Optional function called for each profile found

        Returns:
            List of descendant dicts with profile and generation info
        """
        if max_generations is None:
            max_generations = self.max_gen_down

        descendants = []

        print(f"\nTraversing paternal line DOWN from {start_profile_id}")

        def traverse(current_id: str, generation: int, path: list):
            if generation > max_generations:
                return

            sons = self.get_sons(current_id)

            for son in sons:
                son_id = son.get("id") or son.get("geni_id")
                son_name = get_name(son)

                indent = "  " * generation
                print(f"{indent}Generation {generation}: {son_name} ({son_id})")

                descendants.append({
                    "profile": son,
                    "generation": generation,
                    "path": path + [son_id]
                })

                if callback:
                    callback(son, generation, "down")

                # Recursively traverse this son's line
                traverse(son_id, generation + 1, path + [son_id])

        traverse(start_profile_id, 1, [start_profile_id])

        print(f"Found {len(descendants)} paternal descendants")
        return descendants

    def propagate_haplogroup(self, source_profile_id: str, haplogroup: str,
                              source: str = "FTDNA", propagate_up: bool = True,
                              propagate_down: bool = True) -> dict:
        """
        Propagate a known Y-DNA haplogroup along paternal lines.

        Args:
            source_profile_id: Profile ID with tested/known haplogroup
            haplogroup: The Y-DNA haplogroup (e.g., "R-M269", "I-M253")
            source: Source of the haplogroup data (FTDNA, YFull, etc.)
            propagate_up: Whether to propagate to ancestors
            propagate_down: Whether to propagate to descendants

        Returns:
            Dict with propagation statistics
        """
        stats = {
            "source_profile": source_profile_id,
            "haplogroup": haplogroup,
            "ancestors_propagated": 0,
            "descendants_propagated": 0,
            "conflicts": []
        }

        # First, ensure the source profile is in the database
        self.fetch_and_save_profile(source_profile_id)

        # Add the tested haplogroup to the source
        self.db.add_haplogroup(
            source_profile_id,
            haplogroup,
            source,
            is_tested=True,
            confidence="confirmed"
        )

        print(f"\n{'='*60}")
        print(f"Propagating haplogroup {haplogroup} from {source_profile_id}")
        print(f"{'='*60}")

        def propagation_callback(profile, generation, direction):
            profile_id = profile.get("id") or profile.get("geni_id")

            # Check for existing haplogroup
            existing = self.db.get_haplogroup(profile_id)

            if existing:
                if existing["haplogroup"] != haplogroup:
                    conflict = {
                        "profile_id": profile_id,
                        "existing_haplogroup": existing["haplogroup"],
                        "new_haplogroup": haplogroup,
                        "generation": generation,
                        "direction": direction
                    }
                    stats["conflicts"].append(conflict)
                    print(f"    CONFLICT: {profile_id} has {existing['haplogroup']}, not {haplogroup}")
                return

            # Propagate the haplogroup
            self.db.add_haplogroup(
                profile_id,
                haplogroup,
                f"propagated_{source}",
                is_tested=False,
                propagated_from=source_profile_id,
                confidence="propagated"
            )

            if direction == "up":
                stats["ancestors_propagated"] += 1
            else:
                stats["descendants_propagated"] += 1

        if propagate_up:
            self.traverse_paternal_line_up(source_profile_id, callback=propagation_callback)

        if propagate_down:
            self.traverse_paternal_line_down(source_profile_id, callback=propagation_callback)

        print(f"\n{'='*60}")
        print(f"Propagation complete:")
        print(f"  Ancestors propagated: {stats['ancestors_propagated']}")
        print(f"  Descendants propagated: {stats['descendants_propagated']}")
        print(f"  Conflicts found: {len(stats['conflicts'])}")
        print(f"{'='*60}")

        return stats

    def build_paternal_tree(self, root_profile_id: str,
                            haplogroup: str = None,
                            tree_name: str = None) -> dict:
        """
        Build a complete paternal tree starting from a root profile.

        Traverses both up and down to create a full tree structure.
        """
        # First go up to find the most distant ancestor
        ancestors = self.traverse_paternal_line_up(root_profile_id)

        if ancestors:
            # The last ancestor is the most distant known
            tree_root = ancestors[-1]
            tree_root_id = tree_root.get("id") or tree_root.get("geni_id")
        else:
            tree_root_id = root_profile_id

        # Now traverse down from the root to get all descendants
        descendants = self.traverse_paternal_line_down(tree_root_id)

        # Create tree record
        if not tree_name:
            root_profile = self.db.get_profile(tree_root_id)
            tree_name = f"Paternal tree of {root_profile.get('display_name', 'Unknown')}"

        tree_id = self.db.create_paternal_tree(
            name=tree_name,
            root_profile_id=tree_root_id,
            haplogroup=haplogroup
        )

        return {
            "tree_id": tree_id,
            "tree_name": tree_name,
            "root_profile_id": tree_root_id,
            "total_ancestors": len(ancestors),
            "total_descendants": len(descendants),
            "haplogroup": haplogroup
        }

    def propagate_full_tree(self, start_profile_id: str, haplogroup: str,
                            source: str = "FTDNA", resume: bool = False) -> dict:
        """
        Propagate haplogroup from furthest ancestor to ALL male descendants.

        1. Finds the oldest paternal ancestor
        2. Recursively propagates to ALL sons at each generation
        3. Returns statistics on the full tree

        Args:
            start_profile_id: Any profile in the paternal line
            haplogroup: Y-DNA haplogroup to propagate
            source: Source of the haplogroup data
            resume: If True, skip profiles already explored for this haplogroup

        Returns:
            Dict with propagation statistics
        """
        stats = {
            "haplogroup": haplogroup,
            "root_profile_id": None,
            "total_propagated": 0,
            "skipped_explored": 0,
            "generations": 0,
            "conflicts": [],
            "resumed": resume
        }

        print(f"\n{'='*60}")
        if resume:
            explored_count = self.db.get_explored_count(haplogroup)
            print(f"RESUMING tree propagation of {haplogroup}")
            print(f"Already explored: {explored_count} profiles")
        else:
            print(f"Full tree propagation of {haplogroup}")
        print(f"{'='*60}")

        # First, find the oldest ancestor
        print(f"\nFinding oldest paternal ancestor from {start_profile_id}...")
        ancestors = self.traverse_paternal_line_up(start_profile_id)

        if ancestors:
            root_profile = ancestors[-1]
            root_id = root_profile.get("id") or root_profile.get("geni_id")
        else:
            root_profile = self.fetch_and_save_profile(start_profile_id)
            # Use actual ID from API, not the input ID
            root_id = root_profile.get("id") if root_profile else start_profile_id

        stats["root_profile_id"] = root_id
        root_name = get_name(root_profile) if root_profile else "Unknown"
        print(f"\nOldest ancestor: {root_name} ({root_id})")

        # Propagate to the root
        self._assign_haplogroup(root_id, haplogroup, source, stats)

        # Recursively propagate to ALL descendants
        print(f"\nPropagating to all male descendants...")

        def propagate_to_all_sons(profile_id: str, generation: int):
            if generation > self.max_gen_down:
                return

            # Check if already explored (for resume)
            if resume and self.db.is_explored(profile_id, haplogroup):
                # Still need to recurse through known sons from database
                sons = self.db.get_sons(profile_id)
                if sons:
                    indent = "  " * generation
                    print(f"{indent}[Skipping explored: {profile_id}, checking {len(sons)} known sons]", flush=True)
                    stats["skipped_explored"] += 1
                    for son in sons:
                        son_id = son.get("geni_id") or son.get("id")
                        # Ensure haplogroup is assigned
                        self._assign_haplogroup(son_id, haplogroup, f"propagated_{source}", stats)
                        # Recurse
                        propagate_to_all_sons(son_id, generation + 1)
                return

            # Fetch from Geni to discover ALL sons
            sons = self.get_sons(profile_id, force_fetch=True)

            # Mark this profile as explored
            self.db.mark_explored(profile_id, haplogroup)

            for son in sons:
                son_id = son.get("id") or son.get("geni_id")
                son_name = get_name(son)
                indent = "  " * generation

                # Assign haplogroup to this son
                assigned = self._assign_haplogroup(son_id, haplogroup, f"propagated_{source}", stats)
                status = "+" if assigned else "="
                print(f"{indent}{status} Gen {generation}: {son_name} ({son_id})")

                # Recurse to this son's sons
                propagate_to_all_sons(son_id, generation + 1)

            if generation > stats["generations"]:
                stats["generations"] = generation

        propagate_to_all_sons(root_id, 1)

        print(f"\n{'='*60}")
        print(f"Full tree propagation complete:")
        print(f"  Root ancestor: {root_name} ({root_id})")
        print(f"  Total profiles: {stats['total_propagated']}")
        if resume:
            print(f"  Skipped (already explored): {stats['skipped_explored']}")
        print(f"  Max generations: {stats['generations']}")
        print(f"  Conflicts: {len(stats['conflicts'])}")
        print(f"{'='*60}")

        return stats

    def _assign_haplogroup(self, profile_id: str, haplogroup: str,
                           source: str, stats: dict) -> bool:
        """Helper to assign haplogroup and track statistics."""
        existing = self.db.get_haplogroup(profile_id)

        if existing:
            if existing["haplogroup"] != haplogroup:
                stats["conflicts"].append({
                    "profile_id": profile_id,
                    "existing": existing["haplogroup"],
                    "new": haplogroup
                })
            return False

        self.db.add_haplogroup(
            profile_id,
            haplogroup,
            source,
            is_tested=(source == "FTDNA" or source == "YFull"),
            confidence="propagated" if "propagated" in source else "confirmed"
        )
        stats["total_propagated"] += 1
        return True

    def find_tree_connections(self, haplogroup1: str, haplogroup2: str) -> list:
        """
        Find potential connections between two haplogroup trees.

        Returns profiles that might connect the two lineages.
        """
        profiles1 = self.db.get_profiles_by_haplogroup(haplogroup1)
        profiles2 = self.db.get_profiles_by_haplogroup(haplogroup2)

        # For now, just return profiles with matching surnames
        surnames1 = set(p.get("last_name", "").lower() for p in profiles1 if p.get("last_name"))
        surnames2 = set(p.get("last_name", "").lower() for p in profiles2 if p.get("last_name"))

        common_surnames = surnames1.intersection(surnames2)

        connections = []
        for surname in common_surnames:
            matching1 = [p for p in profiles1 if p.get("last_name", "").lower() == surname]
            matching2 = [p for p in profiles2 if p.get("last_name", "").lower() == surname]
            connections.append({
                "surname": surname,
                "haplogroup1_profiles": matching1,
                "haplogroup2_profiles": matching2
            })

        return connections

    def get_statistics(self) -> dict:
        """Get propagator statistics."""
        return self.db.get_statistics()

    def close(self):
        """Clean up resources."""
        self.db.close()


if __name__ == "__main__":
    # Test the propagator
    propagator = YDNAPropagator()

    if propagator.authenticate():
        stats = propagator.get_statistics()
        print(f"\nDatabase statistics: {stats}")

    propagator.close()
