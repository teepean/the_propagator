"""
SQLite Database for Y-DNA Propagator

Stores profiles, paternal relationships, and haplogroup data for offline research.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional


class Database:
    """SQLite database for storing Geni profiles and Y-DNA data."""

    def __init__(self, db_path: str = "ydna_propagator.db"):
        self.db_path = db_path
        self.conn = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """Connect to the SQLite database."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        # Enable foreign keys
        self.conn.execute("PRAGMA foreign_keys = ON")

    def _create_tables(self):
        """Create database tables if they don't exist."""
        cursor = self.conn.cursor()

        # Profiles table - stores Geni profile data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                geni_id TEXT PRIMARY KEY,
                guid TEXT,
                display_name TEXT,
                first_name TEXT,
                middle_name TEXT,
                last_name TEXT,
                maiden_name TEXT,
                suffix TEXT,
                gender TEXT,
                birth_date TEXT,
                birth_place TEXT,
                death_date TEXT,
                death_place TEXT,
                is_alive INTEGER,
                occupation TEXT,
                about_me TEXT,
                raw_data TEXT,
                fetched_at TEXT,
                updated_at TEXT
            )
        """)

        # Paternal relationships table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS paternal_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                father_id TEXT NOT NULL,
                child_id TEXT NOT NULL,
                source TEXT DEFAULT 'geni',
                confidence TEXT DEFAULT 'confirmed',
                created_at TEXT,
                FOREIGN KEY (father_id) REFERENCES profiles(geni_id),
                FOREIGN KEY (child_id) REFERENCES profiles(geni_id),
                UNIQUE(father_id, child_id)
            )
        """)

        # Haplogroups table - stores Y-DNA haplogroup assignments
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS haplogroups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id TEXT NOT NULL,
                haplogroup TEXT NOT NULL,
                source TEXT NOT NULL,
                source_detail TEXT,
                is_tested INTEGER DEFAULT 0,
                is_propagated INTEGER DEFAULT 0,
                propagated_from TEXT,
                confidence TEXT DEFAULT 'high',
                notes TEXT,
                created_at TEXT,
                FOREIGN KEY (profile_id) REFERENCES profiles(geni_id),
                FOREIGN KEY (propagated_from) REFERENCES profiles(geni_id)
            )
        """)

        # Paternal trees table - groups profiles into connected paternal trees
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS paternal_trees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                root_profile_id TEXT,
                haplogroup TEXT,
                description TEXT,
                created_at TEXT,
                FOREIGN KEY (root_profile_id) REFERENCES profiles(geni_id)
            )
        """)

        # Tree membership - which profiles belong to which trees
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tree_members (
                tree_id INTEGER,
                profile_id TEXT,
                generations_from_root INTEGER,
                direction TEXT,
                PRIMARY KEY (tree_id, profile_id),
                FOREIGN KEY (tree_id) REFERENCES paternal_trees(id),
                FOREIGN KEY (profile_id) REFERENCES profiles(geni_id)
            )
        """)

        # Unions table - stores family unions from Geni
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS unions (
                geni_id TEXT PRIMARY KEY,
                partners TEXT,
                children TEXT,
                marriage_date TEXT,
                marriage_place TEXT,
                divorce_date TEXT,
                status TEXT,
                raw_data TEXT,
                fetched_at TEXT
            )
        """)

        # Create indexes for faster lookups
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_profiles_gender ON profiles(gender)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_profiles_last_name ON profiles(last_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_paternal_father ON paternal_links(father_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_paternal_child ON paternal_links(child_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_haplogroups_profile ON haplogroups(profile_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_haplogroups_haplogroup ON haplogroups(haplogroup)")

        self.conn.commit()

    def save_profile(self, profile_data: dict) -> str:
        """
        Save or update a profile from Geni API response.

        Returns the geni_id of the saved profile.
        """
        geni_id = profile_data.get("id", "")
        if not geni_id:
            return None

        # Extract event data
        birth = profile_data.get("birth", {}) or {}
        death = profile_data.get("death", {}) or {}

        cursor = self.conn.cursor()
        now = datetime.utcnow().isoformat()

        cursor.execute("""
            INSERT INTO profiles (
                geni_id, guid, display_name, first_name, middle_name, last_name,
                maiden_name, suffix, gender, birth_date, birth_place,
                death_date, death_place, is_alive, occupation, about_me,
                raw_data, fetched_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(geni_id) DO UPDATE SET
                guid = excluded.guid,
                display_name = excluded.display_name,
                first_name = excluded.first_name,
                middle_name = excluded.middle_name,
                last_name = excluded.last_name,
                maiden_name = excluded.maiden_name,
                suffix = excluded.suffix,
                gender = excluded.gender,
                birth_date = excluded.birth_date,
                birth_place = excluded.birth_place,
                death_date = excluded.death_date,
                death_place = excluded.death_place,
                is_alive = excluded.is_alive,
                occupation = excluded.occupation,
                about_me = excluded.about_me,
                raw_data = excluded.raw_data,
                updated_at = excluded.updated_at
        """, (
            geni_id,
            profile_data.get("guid"),
            profile_data.get("display_name") or profile_data.get("name"),
            profile_data.get("first_name"),
            profile_data.get("middle_name"),
            profile_data.get("last_name"),
            profile_data.get("maiden_name"),
            profile_data.get("suffix"),
            profile_data.get("gender"),
            birth.get("date", {}).get("formatted_date") if isinstance(birth.get("date"), dict) else birth.get("date"),
            birth.get("location", {}).get("place_name") if isinstance(birth.get("location"), dict) else birth.get("location"),
            death.get("date", {}).get("formatted_date") if isinstance(death.get("date"), dict) else death.get("date"),
            death.get("location", {}).get("place_name") if isinstance(death.get("location"), dict) else death.get("location"),
            1 if profile_data.get("is_alive") else 0,
            profile_data.get("occupation"),
            profile_data.get("about_me"),
            json.dumps(profile_data),
            now,
            now
        ))

        self.conn.commit()
        return geni_id

    def save_union(self, union_data: dict) -> str:
        """Save or update a union from Geni API response."""
        geni_id = union_data.get("id", "")
        if not geni_id:
            return None

        marriage = union_data.get("marriage", {}) or {}
        divorce = union_data.get("divorce", {}) or {}

        cursor = self.conn.cursor()
        now = datetime.utcnow().isoformat()

        cursor.execute("""
            INSERT INTO unions (
                geni_id, partners, children, marriage_date, marriage_place,
                divorce_date, status, raw_data, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(geni_id) DO UPDATE SET
                partners = excluded.partners,
                children = excluded.children,
                marriage_date = excluded.marriage_date,
                marriage_place = excluded.marriage_place,
                divorce_date = excluded.divorce_date,
                status = excluded.status,
                raw_data = excluded.raw_data,
                fetched_at = excluded.fetched_at
        """, (
            geni_id,
            json.dumps(union_data.get("partners", [])),
            json.dumps(union_data.get("children", [])),
            marriage.get("date", {}).get("formatted_date") if isinstance(marriage.get("date"), dict) else marriage.get("date"),
            marriage.get("location", {}).get("place_name") if isinstance(marriage.get("location"), dict) else marriage.get("location"),
            divorce.get("date", {}).get("formatted_date") if isinstance(divorce.get("date"), dict) else divorce.get("date"),
            union_data.get("status"),
            json.dumps(union_data),
            now
        ))

        self.conn.commit()
        return geni_id

    def add_paternal_link(self, father_id: str, child_id: str,
                          source: str = "geni", confidence: str = "confirmed"):
        """Add a father-child relationship."""
        cursor = self.conn.cursor()
        now = datetime.utcnow().isoformat()

        cursor.execute("""
            INSERT OR IGNORE INTO paternal_links (father_id, child_id, source, confidence, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (father_id, child_id, source, confidence, now))

        self.conn.commit()

    def add_haplogroup(self, profile_id: str, haplogroup: str, source: str,
                       is_tested: bool = False, propagated_from: str = None,
                       confidence: str = "high", notes: str = None):
        """Add a haplogroup assignment to a profile."""
        cursor = self.conn.cursor()
        now = datetime.utcnow().isoformat()

        cursor.execute("""
            INSERT INTO haplogroups (
                profile_id, haplogroup, source, is_tested, is_propagated,
                propagated_from, confidence, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            profile_id,
            haplogroup,
            source,
            1 if is_tested else 0,
            1 if propagated_from else 0,
            propagated_from,
            confidence,
            notes,
            now
        ))

        self.conn.commit()

    def get_profile(self, geni_id: str) -> Optional[dict]:
        """Get a profile by Geni ID."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM profiles WHERE geni_id = ?", (geni_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_father(self, child_id: str) -> Optional[dict]:
        """Get the father of a profile."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT p.* FROM profiles p
            JOIN paternal_links pl ON p.geni_id = pl.father_id
            WHERE pl.child_id = ?
        """, (child_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_sons(self, father_id: str) -> list:
        """Get all sons of a profile."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT p.* FROM profiles p
            JOIN paternal_links pl ON p.geni_id = pl.child_id
            WHERE pl.father_id = ?
        """, (father_id,))
        return [dict(row) for row in cursor.fetchall()]

    def get_haplogroup(self, profile_id: str) -> Optional[dict]:
        """Get haplogroup assignment for a profile."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM haplogroups WHERE profile_id = ?
            ORDER BY is_tested DESC, created_at DESC LIMIT 1
        """, (profile_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_profiles_by_haplogroup(self, haplogroup: str) -> list:
        """Get all profiles with a specific haplogroup."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT p.*, h.haplogroup, h.source as haplogroup_source, h.is_tested
            FROM profiles p
            JOIN haplogroups h ON p.geni_id = h.profile_id
            WHERE h.haplogroup LIKE ?
            ORDER BY p.last_name, p.first_name
        """, (f"{haplogroup}%",))
        return [dict(row) for row in cursor.fetchall()]

    def get_male_profiles(self) -> list:
        """Get all male profiles in the database."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM profiles WHERE gender = 'male' ORDER BY last_name, first_name")
        return [dict(row) for row in cursor.fetchall()]

    def get_profiles_without_haplogroup(self, gender: str = "male") -> list:
        """Get profiles that don't have a haplogroup assigned."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT p.* FROM profiles p
            LEFT JOIN haplogroups h ON p.geni_id = h.profile_id
            WHERE p.gender = ? AND h.id IS NULL
            ORDER BY p.last_name, p.first_name
        """, (gender,))
        return [dict(row) for row in cursor.fetchall()]

    def get_paternal_ancestors(self, profile_id: str, max_generations: int = 50) -> list:
        """
        Get paternal line ancestors (father's father's father...).

        Returns list of profiles from child to most distant ancestor.
        """
        ancestors = []
        current_id = profile_id
        generation = 0

        while generation < max_generations:
            father = self.get_father(current_id)
            if not father:
                break
            ancestors.append(father)
            current_id = father["geni_id"]
            generation += 1

        return ancestors

    def get_paternal_descendants(self, profile_id: str, max_generations: int = 50) -> list:
        """
        Get all paternal line descendants recursively.

        Returns list of dicts with profile and generation info.
        """
        descendants = []

        def traverse(current_id: str, generation: int):
            if generation > max_generations:
                return
            sons = self.get_sons(current_id)
            for son in sons:
                descendants.append({"profile": son, "generation": generation})
                traverse(son["geni_id"], generation + 1)

        traverse(profile_id, 1)
        return descendants

    def create_paternal_tree(self, name: str, root_profile_id: str,
                             haplogroup: str = None, description: str = None) -> int:
        """Create a new paternal tree record."""
        cursor = self.conn.cursor()
        now = datetime.utcnow().isoformat()

        cursor.execute("""
            INSERT INTO paternal_trees (name, root_profile_id, haplogroup, description, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (name, root_profile_id, haplogroup, description, now))

        self.conn.commit()
        return cursor.lastrowid

    def get_statistics(self) -> dict:
        """Get database statistics."""
        cursor = self.conn.cursor()

        stats = {}

        cursor.execute("SELECT COUNT(*) FROM profiles")
        stats["total_profiles"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM profiles WHERE gender = 'male'")
        stats["male_profiles"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM paternal_links")
        stats["paternal_links"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT profile_id) FROM haplogroups")
        stats["profiles_with_haplogroup"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM haplogroups WHERE is_tested = 1")
        stats["tested_haplogroups"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT haplogroup) FROM haplogroups")
        stats["unique_haplogroups"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM paternal_trees")
        stats["paternal_trees"] = cursor.fetchone()[0]

        return stats

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()


if __name__ == "__main__":
    # Test the database
    db = Database()
    print("Database initialized successfully.")
    print(f"Statistics: {db.get_statistics()}")
    db.close()
