"""
Geni API Client for Y-DNA Propagator

Handles OAuth authentication and API requests to Geni.com
"""

import json
import time
import webbrowser
from pathlib import Path
from urllib.parse import urlencode

import requests


class GeniClient:
    """Client for interacting with the Geni API."""

    def __init__(self, config_path: str = "config.json"):
        self.config = self._load_config(config_path)
        self.base_url = self.config["geni"]["base_url"]
        self.client_id = self.config["geni"]["client_id"]
        self.client_secret = self.config["geni"]["client_secret"]
        self.redirect_uri = self.config["geni"]["redirect_uri"]

        self.token_file = Path("geni_token.json")
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = 0

        self._load_token()

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from JSON file."""
        with open(config_path, "r") as f:
            return json.load(f)

    def _load_token(self):
        """Load saved token from file if it exists."""
        if self.token_file.exists():
            with open(self.token_file, "r") as f:
                token_data = json.load(f)
                self.access_token = token_data.get("access_token")
                self.refresh_token = token_data.get("refresh_token")
                self.token_expires_at = token_data.get("expires_at", 0)

    def _save_token(self):
        """Save token to file for future sessions."""
        token_data = {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.token_expires_at
        }
        with open(self.token_file, "w") as f:
            json.dump(token_data, f, indent=2)

    def get_authorization_url(self) -> str:
        """Get the URL for user authorization."""
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code"
        }
        return f"{self.base_url}/oauth/authorize?{urlencode(params)}"

    def authenticate(self, auth_code: str = None):
        """
        Authenticate with Geni API.

        If no auth_code provided, opens browser for user to authorize.
        """
        if self.access_token and time.time() < self.token_expires_at:
            print("Using existing valid token.")
            return True

        if self.refresh_token:
            if self._refresh_access_token():
                return True

        if not auth_code:
            auth_url = self.get_authorization_url()
            print(f"\nPlease authorize the app by visiting:\n{auth_url}\n")
            webbrowser.open(auth_url)
            auth_code = input("Enter the authorization code: ").strip()

        return self._exchange_code_for_token(auth_code)

    def _exchange_code_for_token(self, auth_code: str) -> bool:
        """Exchange authorization code for access token."""
        url = f"{self.base_url}/oauth/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": auth_code,
            "redirect_uri": self.redirect_uri,
            "grant_type": "authorization_code"
        }

        response = requests.post(url, data=data)

        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            self.refresh_token = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in", 3600)
            self.token_expires_at = time.time() + expires_in - 60  # 60s buffer
            self._save_token()
            print("Authentication successful!")
            return True
        else:
            print(f"Authentication failed: {response.status_code}")
            print(response.text)
            return False

    def _refresh_access_token(self) -> bool:
        """Refresh the access token using refresh token."""
        url = f"{self.base_url}/oauth/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"
        }

        response = requests.post(url, data=data)

        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            if "refresh_token" in token_data:
                self.refresh_token = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in", 3600)
            self.token_expires_at = time.time() + expires_in - 60
            self._save_token()
            print("Token refreshed successfully.")
            return True
        else:
            print("Token refresh failed, need to re-authenticate.")
            return False

    def _make_request(self, endpoint: str, params: dict = None, retries: int = 3) -> dict:
        """Make an authenticated API request with retry on rate limit."""
        if not self.access_token:
            raise Exception("Not authenticated. Call authenticate() first.")

        if time.time() >= self.token_expires_at:
            if not self._refresh_access_token():
                raise Exception("Token expired and refresh failed.")

        url = f"{self.base_url}/api/{endpoint}"

        if params is None:
            params = {}
        params["access_token"] = self.access_token

        for attempt in range(retries):
            response = requests.get(url, params=params)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limited - wait and retry
                wait_time = (attempt + 1) * 5  # 5, 10, 15 seconds
                print(f"  Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise Exception(f"API request failed: {response.status_code} - {response.text}")

        raise Exception(f"API request failed after {retries} retries: 429 - Rate limit exceeded")

    def get_profile(self, profile_id: str, fields: list = None) -> dict:
        """
        Get a profile by ID.

        Args:
            profile_id: The Geni profile ID (e.g., "profile-12345" or just "12345")
            fields: Optional list of fields to retrieve

        Returns:
            Profile data dictionary
        """
        if not profile_id.startswith("profile-"):
            profile_id = f"profile-{profile_id}"

        params = {}
        if fields:
            params["fields"] = ",".join(fields)

        return self._make_request(profile_id, params)

    def get_immediate_family(self, profile_id: str) -> dict:
        """
        Get immediate family members for a profile.

        Returns dict with 'focus' (the profile) and 'nodes' (family members and unions)
        """
        if not profile_id.startswith("profile-"):
            profile_id = f"profile-{profile_id}"

        return self._make_request(f"{profile_id}/immediate-family")

    def get_ancestors(self, profile_id: str, generations: int = 5) -> dict:
        """
        Get ancestors for a profile.

        Args:
            profile_id: The Geni profile ID
            generations: Number of generations to retrieve (max 20)

        Returns:
            Dict with 'focus' and 'nodes' containing ancestor profiles/unions
        """
        if not profile_id.startswith("profile-"):
            profile_id = f"profile-{profile_id}"

        params = {"generations": min(generations, 20)}
        return self._make_request(f"{profile_id}/ancestors", params)

    def search_profiles(self, names: str = None, **kwargs) -> dict:
        """
        Search for profiles.

        Args:
            names: Name(s) to search for
            **kwargs: Additional search parameters (first_name, last_name, etc.)

        Returns:
            Search results
        """
        params = {}
        if names:
            params["names"] = names
        params.update(kwargs)

        return self._make_request("profile/search", params)

    def get_user(self) -> dict:
        """Get the currently authenticated user's profile."""
        return self._make_request("user")


if __name__ == "__main__":
    # Test the client
    client = GeniClient()

    if client.authenticate():
        print("\nFetching user profile...")
        user = client.get_user()
        print(f"Logged in as: {user.get('name', 'Unknown')}")
