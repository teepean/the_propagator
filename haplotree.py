"""
FTDNA Y-DNA Haplotree Lookup

Provides functions for analyzing relationships between Y-DNA haplogroups
using FTDNA's public haplotree data.
"""

import json
from typing import Optional, Tuple


class Haplotree:
    """FTDNA Y-DNA Haplotree for finding haplogroup relationships."""

    def __init__(self, haplotree_path: str = "ftdna_haplotree.json"):
        """Load the FTDNA haplotree data."""
        self.haplotree_path = haplotree_path
        self.nodes = {}
        self.name_to_id = {}
        self._loaded = False

    def _ensure_loaded(self):
        """Lazy load the haplotree data."""
        if self._loaded:
            return

        try:
            with open(self.haplotree_path, 'r') as f:
                data = json.load(f)
            self.nodes = data.get('allNodes', {})

            # Build name lookup index
            for hid, node in self.nodes.items():
                name = node.get('name', '')
                if name:
                    self.name_to_id[name] = hid

            self._loaded = True
            print(f"Loaded {len(self.nodes)} haplogroups from FTDNA haplotree")
        except FileNotFoundError:
            print(f"Haplotree file not found: {self.haplotree_path}")
            print("Download with: wget https://www.familytreedna.com/public/y-dna-haplotree/get -O ftdna_haplotree.json")
        except json.JSONDecodeError as e:
            print(f"Error parsing haplotree JSON: {e}")

    def find_by_name(self, name: str) -> Optional[dict]:
        """Find a haplogroup by its name (e.g., 'R-M269')."""
        self._ensure_loaded()
        hid = self.name_to_id.get(name)
        if hid:
            return self.nodes.get(hid)
        return None

    def get_ancestry_path(self, name: str) -> list:
        """
        Get the full ancestry path from a haplogroup to the root.

        Returns list of (id, name) tuples from the haplogroup up to root.
        """
        self._ensure_loaded()

        hid = self.name_to_id.get(name)
        if not hid:
            return []

        path = []
        current_id = hid
        visited = set()

        while current_id and str(current_id) not in visited:
            visited.add(str(current_id))
            node = self.nodes.get(str(current_id))
            if not node:
                break
            path.append((str(current_id), node.get('name', '')))
            parent_id = node.get('parentId')
            if not parent_id or str(parent_id) == str(current_id):
                break
            current_id = str(parent_id)

        return path

    def find_common_ancestor(self, name1: str, name2: str) -> Optional[dict]:
        """
        Find the most recent common ancestor of two haplogroups.

        Returns dict with:
            - name: Common ancestor haplogroup name
            - distance1: Steps from name1 to common ancestor
            - distance2: Steps from name2 to common ancestor
            - total_distance: Total genetic distance between the two
        """
        path1 = self.get_ancestry_path(name1)
        path2 = self.get_ancestry_path(name2)

        if not path1 or not path2:
            return None

        set1 = {hid for hid, _ in path1}

        for i, (hid, name) in enumerate(path2):
            if hid in set1:
                # Find position in path1
                pos1 = next(j for j, (h, _) in enumerate(path1) if h == hid)
                return {
                    'name': name,
                    'id': hid,
                    'distance1': pos1,
                    'distance2': i,
                    'total_distance': pos1 + i
                }

        return None

    def are_related(self, name1: str, name2: str, max_distance: int = 10) -> bool:
        """
        Check if two haplogroups are closely related.

        Args:
            name1: First haplogroup name
            name2: Second haplogroup name
            max_distance: Maximum steps between them to consider related

        Returns True if they share a common ancestor within max_distance steps.
        """
        result = self.find_common_ancestor(name1, name2)
        if not result:
            return False
        return result['total_distance'] <= max_distance

    def is_downstream_of(self, child: str, ancestor: str) -> bool:
        """
        Check if a haplogroup is downstream of another.

        Args:
            child: Potential descendant haplogroup
            ancestor: Potential ancestor haplogroup

        Returns True if child is downstream of ancestor.
        """
        path = self.get_ancestry_path(child)
        ancestor_id = self.name_to_id.get(ancestor)

        if not ancestor_id:
            return False

        for hid, _ in path:
            if hid == ancestor_id:
                return True

        return False

    def get_snp_info(self, name: str) -> list:
        """
        Get the SNP variants that define a haplogroup.

        Returns list of variant dicts with position, ancestral, derived alleles.
        """
        node = self.find_by_name(name)
        if not node:
            return []
        return node.get('variants', [])

    def compare_haplogroups(self, name1: str, name2: str) -> dict:
        """
        Get a detailed comparison between two haplogroups.

        Returns dict with paths and common ancestor information.
        """
        path1 = self.get_ancestry_path(name1)
        path2 = self.get_ancestry_path(name2)
        common = self.find_common_ancestor(name1, name2)

        result = {
            'haplogroup1': name1,
            'haplogroup2': name2,
            'path1': [name for _, name in path1],
            'path2': [name for _, name in path2],
            'common_ancestor': common,
            'relationship': 'unknown'
        }

        if common:
            if common['distance1'] == 0:
                result['relationship'] = f'{name1} is ancestor of {name2}'
            elif common['distance2'] == 0:
                result['relationship'] = f'{name2} is ancestor of {name1}'
            elif common['distance1'] == 1 and common['distance2'] == 1:
                result['relationship'] = f'siblings (share parent {common["name"]})'
            else:
                result['relationship'] = f'cousins (share {common["name"]} at {common["total_distance"]} steps)'

        return result

    def get_statistics(self) -> dict:
        """Get statistics about the loaded haplotree."""
        self._ensure_loaded()

        if not self.nodes:
            return {'loaded': False}

        # Count by root haplogroup
        roots = {}
        for node in self.nodes.values():
            root = node.get('root', 'Unknown')
            roots[root] = roots.get(root, 0) + 1

        return {
            'loaded': True,
            'total_haplogroups': len(self.nodes),
            'by_root': roots
        }


def print_comparison(name1: str, name2: str, tree: Haplotree = None):
    """Print a formatted comparison between two haplogroups."""
    if tree is None:
        tree = Haplotree()

    comparison = tree.compare_haplogroups(name1, name2)

    print(f"\n{'='*60}")
    print(f"Haplogroup Comparison: {name1} vs {name2}")
    print(f"{'='*60}")

    print(f"\n{name1} ancestry ({len(comparison['path1'])} levels):")
    for i, name in enumerate(comparison['path1'][:15]):
        print(f"  {i}: {name}")
    if len(comparison['path1']) > 15:
        print(f"  ... ({len(comparison['path1']) - 15} more)")

    print(f"\n{name2} ancestry ({len(comparison['path2'])} levels):")
    for i, name in enumerate(comparison['path2'][:15]):
        print(f"  {i}: {name}")
    if len(comparison['path2']) > 15:
        print(f"  ... ({len(comparison['path2']) - 15} more)")

    common = comparison['common_ancestor']
    if common:
        print(f"\n{'='*60}")
        print(f"MOST RECENT COMMON ANCESTOR: {common['name']}")
        print(f"{'='*60}")
        print(f"  {name1} is {common['distance1']} steps from MRCA")
        print(f"  {name2} is {common['distance2']} steps from MRCA")
        print(f"  Total genetic distance: {common['total_distance']} steps")
        print(f"  Relationship: {comparison['relationship']}")
    else:
        print("\nNo common ancestor found within tree")


if __name__ == "__main__":
    # Test the haplotree
    tree = Haplotree()

    # Compare the two haplogroups from our propagation test
    print_comparison('R-A11110', 'R-BY117398', tree)

    # Show statistics
    stats = tree.get_statistics()
    print(f"\n\nHaplotree Statistics:")
    print(f"  Total haplogroups: {stats.get('total_haplogroups', 0)}")
    if 'by_root' in stats:
        print("  By root:")
        for root, count in sorted(stats['by_root'].items(), key=lambda x: -x[1])[:10]:
            print(f"    {root}: {count}")
