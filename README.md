# Y-DNA Propagator

A tool for extending Y-DNA haplogroup propagation along paternal lines using the Geni.com API. This application enables offline research of patrilineal descent by traversing father-son relationships and assigning Y-DNA haplogroups to male ancestors and descendants.

## Purpose

On Geni.com, Y-DNA propagation traces paternal lines through the family tree, matching and confirming relationships between men with common male ancestors. This tool extends that functionality by:

1. Traversing paternal lines beyond Geni's built-in limits
2. Propagating known Y-DNA haplogroups to ancestors and descendants
3. Building offline paternal trees for research
4. Identifying potential connections between separate paternal lineages

## Requirements

- Python 3.10+
- `requests` library

## Installation

```bash
git clone https://github.com/teepean/the_propagator.git
cd the_propagator
pip install -r requirements.txt
```

## Configuration

1. Register an application at https://www.geni.com/platform/developer/apps
2. Copy `config.example.json` to `config.json`
3. Add your application credentials:

```json
{
    "geni": {
        "base_url": "https://www.geni.com",
        "app_id": "YOUR_APP_ID",
        "client_id": "YOUR_CLIENT_ID",
        "client_secret": "YOUR_CLIENT_SECRET",
        "api_version": "1",
        "redirect_uri": "urn:ietf:wg:oauth:2.0:oob"
    },
    "database": {
        "path": "ydna_propagator.db"
    },
    "propagation": {
        "max_generations_up": 50,
        "max_generations_down": 50
    },
    "rate_limit": {
        "delay": 3.0
    }
}
```

## Authentication

Obtain an access token from the Geni API Explorer:
https://www.geni.com/platform/developer/api_explorer

Then run:
```bash
python3 cli.py auth --code YOUR_ACCESS_TOKEN
```

Or save the token directly to `geni_token.json`:
```json
{
    "access_token": "YOUR_ACCESS_TOKEN",
    "refresh_token": null,
    "expires_at": 9999999999
}
```

## Usage

### View immediate family
```bash
python3 cli.py family <profile-id>
```

### Traverse paternal ancestors
```bash
python3 cli.py ancestors <profile-id> --generations 20
```

### Traverse paternal descendants
```bash
python3 cli.py descendants <profile-id> --generations 20
```

### Propagate haplogroup along paternal line
```bash
python3 cli.py propagate <profile-id> <haplogroup> --source FTDNA
```

### Full tree propagation
Automatically finds the oldest paternal ancestor and propagates a haplogroup to all male descendants:
```bash
python3 cli.py full-tree <profile-id> <haplogroup> --source FTDNA
```

This generates a uniquely named CSV file: `tree_{haplogroup}_{ancestor_name}_{id}_{timestamp}.csv`

### Import known haplogroups from CSV
```bash
python3 cli.py import haplogroups.csv
```

CSV format:
```csv
geni_profile_id,haplogroup,source
profile-12345,R-M269,FTDNA
profile-67890,I-M253,YFull
```

### Export profiles by haplogroup
```bash
python3 cli.py export <haplogroup> output.csv
```

### View database statistics
```bash
python3 cli.py stats
```

## Database Schema

The application stores data locally in SQLite:

- **profiles**: Geni profile data (names, dates, gender)
- **paternal_links**: Father-son relationships
- **haplogroups**: Y-DNA haplogroup assignments with source tracking
- **unions**: Family union data from Geni

## Rate Limiting

The Geni API has rate limits. The application includes:
- Configurable delay between requests (default: 3 seconds)
- Automatic retry with exponential backoff on rate limit errors

Adjust the `rate_limit.delay` value in `config.json` if needed.

## License

This project is provided for genealogical research purposes.

## Acknowledgments

- Geni.com for providing the API
- Developed with assistance from Claude (Anthropic)
