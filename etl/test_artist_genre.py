import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
from etl.refresh import get_access_token

def check_artist_genre(artist_id="7GlBOeep6PqTfFi59PTUUN"):
    """Fetch and print genre info for a given artist ID."""
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.spotify.com/v1/artists/{artist_id}"

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    print(f"Artist: {data['name']}")
    print(f"Genres: {data.get('genres', [])}")

if __name__ == "__main__":
    check_artist_genre()
