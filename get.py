import os
import sys
import json
import argparse
import re
import time
from pathlib import Path
from typing import List
from datetime import datetime, timezone

import requests
import pymongo
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

# Load .env from repository root (same folder as this script)
ROOT = Path(__file__).parent
load_dotenv(dotenv_path=ROOT / ".env")

API_KEY = os.getenv("HYPESCORE_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "mobula")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "Token")

if not API_KEY:
    print("ERROR: HYPESCORE_API_KEY not found in environment or .env. Copy .env.example -> .env and set your key.", file=sys.stderr)
    sys.exit(2)

if not MONGO_URI:
    print("ERROR: MONGO_URI not found in environment or .env. Set MONGO_URI to your MongoDB connection string.", file=sys.stderr)
    sys.exit(2)

DEFAULT_ADDRESS = "FUAfBo2jgks6gB4Z4LfZkqSZgzNucisEHqnNebaRxM1P"
BASE_URL = "https://api.mobula.io/api/2/token/details"

def extract_addresses_from_env_file(env_path: Path) -> List[str]:
    text = env_path.read_text(encoding="utf-8")
    # Try common env keys first
    for key in ("ADDRESSES", "TOKENS", "TOKENS_LIST"):
        m = re.search(rf"^{key}\s*=\s*(.+)$", text, flags=re.MULTILINE | re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            val = re.sub(r"^['\"]|['\"]$", "", val)
            parts = [p.strip() for p in re.split(r"[,\n]+", val) if p.strip()]
            return parts
    # Fallback: find a long quoted comma-separated string anywhere
    m = re.search(r'"([^"]{50,})"', text, flags=re.DOTALL)
    if m:
        raw = m.group(1)
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return parts
    return []

def load_addresses() -> List[str]:
    # 1) env var ADDRESSES or TOKENS as CSV
    for key in ("ADDRESSES", "TOKENS", "TOKENS_LIST"):
        v = os.getenv(key)
        if v:
            v = v.strip().strip('"').strip("'")
            parts = [p.strip() for p in re.split(r"[,\n]+", v) if p.strip()]
            if parts:
                return parts
    # 2) parse .env file for the long quoted string you pasted
    env_path = ROOT / ".env"
    if env_path.exists():
        parts = extract_addresses_from_env_file(env_path)
        if parts:
            return parts
    # 3) fallback single default address
    return [DEFAULT_ADDRESS]

def get_token_details(address: str, timeout: int = 10):
    params = {"blockchain": "solana", "address": address}
    headers = {"Authorization": API_KEY}
    resp = requests.get(BASE_URL, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def save_result(out_path: Path, data):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def store_to_mongo(coll, address: str, data, fetched_at: datetime):
    doc = {
        "address": address,
        "fetched_at": fetched_at,  # timezone-aware UTC datetime
        "data": data
    }
    coll.insert_one(doc)

def main():
    parser = argparse.ArgumentParser(description="Fetch token details for one or many addresses (reads key from .env)")
    parser.add_argument("--address", "-a", help="Single token address to query (overrides .env addresses)")
    parser.add_argument("--out", "-o", help="Optional path to save JSON output. If directory, saves per-address files. If file, saves a mapping object.")
    parser.add_argument("--delay", "-d", type=float, default=0.25, help="Delay (seconds) between requests to avoid rate limits")
    args = parser.parse_args()

    if args.address:
        addresses = [args.address]
    else:
        addresses = load_addresses()

    addresses = [a for a in (addr.strip() for addr in addresses) if a]
    if not addresses:
        print("No addresses found to query.", file=sys.stderr)
        sys.exit(2)

    # Prepare output path handling
    out = Path(args.out) if args.out else None
    is_out_dir = False
    if out:
        is_out_dir = (out.exists() and out.is_dir()) or (args.out and args.out.endswith(("/", "\\")))
        if out and not is_out_dir and out.suffix == "":
            is_out_dir = True

    # Connect to MongoDB
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        coll = db[MONGO_COLLECTION]
    except PyMongoError as e:
        print(f"Failed to connect to MongoDB: {e}", file=sys.stderr)
        sys.exit(3)

    results = {}
    run_ts = datetime.now(timezone.utc)  # single timestamp for this run

    for i, addr in enumerate(addresses, start=1):
        try:
            print(f"[{i}/{len(addresses)}] Querying {addr} ...", flush=True)
            data = get_token_details(addr)
            results[addr] = {"ok": True}
            # store to mongo with fetched timestamp
            try:
                store_to_mongo(coll, addr, data, run_ts)
            except PyMongoError as me:
                print(f"Mongo insert failed for {addr}: {me}", file=sys.stderr)
                results[addr] = {"ok": False, "mongo_error": str(me)}
            # Save per-address file if requested and out is directory
            if out and is_out_dir:
                file_name = f"{addr}.json"
                save_result(out / file_name, data)
            time.sleep(max(0.0, args.delay))
        except requests.HTTPError as e:
            print(f"HTTP error for {addr}: {e} - {getattr(e.response, 'text', '')}", file=sys.stderr)
            results[addr] = {"error": str(e), "status_code": getattr(e.response, "status_code", None)}
        except requests.RequestException as e:
            print(f"Request failed for {addr}: {e}", file=sys.stderr)
            results[addr] = {"error": str(e)}

    # Final output handling
    pretty = json.dumps(results, indent=2)
    print(pretty)

    if out:
        if is_out_dir:
            print(f"Saved per-address JSON files to {out}")
        else:
            save_result(out, results)
            print(f"Saved combined JSON to {out}")

if __name__ == "__main__":
    main()