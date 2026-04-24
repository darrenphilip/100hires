import argparse
import json
import os
import re
import ssl
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_ENDPOINT = "https://api.supadata.ai/v1/youtube/transcript"
SCRIPT_DIR = Path(__file__).resolve().parent
YOUTUBE_VIDEO_PATTERN = re.compile(
    r"(https?://(?:www\.)?(?:youtube\.com/watch\?v=[\w\-]{6,}|youtu\.be/[\w\-]{6,}))",
    re.IGNORECASE,
)
NAME_PATTERN = re.compile(r"^\s*\d+\.\s+(.+?)\s*$")


def parse_sources_for_videos(path: Path) -> List[Tuple[str, str]]:
    """Return list of (name, video_url) from the sources markdown file."""
    pairs: List[Tuple[str, str]] = []
    current_name: Optional[str] = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        name_match = NAME_PATTERN.match(line)
        if name_match:
            current_name = name_match.group(1)
            continue

        if not current_name:
            continue

        for match in YOUTUBE_VIDEO_PATTERN.findall(line):
            pairs.append((current_name, normalize_youtube_url(match)))

    # Dedupe while preserving order.
    seen = set()
    unique_pairs: List[Tuple[str, str]] = []
    for pair in pairs:
        if pair in seen:
            continue
        seen.add(pair)
        unique_pairs.append(pair)
    return unique_pairs


def normalize_youtube_url(url: str) -> str:
    return url.split("&")[0].rstrip("/")


def supadata_request(endpoint: str, api_key: str, video_url: str) -> Dict:
    base_headers = {
        "Accept": "application/json",
        "User-Agent": "100hires-transcript-client/1.0 (+python urllib)",
    }
    ssl_context = build_ssl_context()
    auth_header_options = [
        {"x-api-key": api_key},
        {"Authorization": f"Bearer {api_key}"},
        {"x-api-key": api_key, "Authorization": f"Bearer {api_key}"},
    ]
    errors: List[str] = []

    # Some Supadata endpoints accept GET with url query string.
    get_url = f"{endpoint}?{urlencode({'url': video_url})}"

    for auth_headers in auth_header_options:
        try:
            get_req = Request(get_url, method="GET", headers={**base_headers, **auth_headers})
            with urlopen(get_req, timeout=60, context=ssl_context) as response:
                body = response.read().decode("utf-8")
                return json.loads(body)
        except HTTPError as get_exc:
            errors.append(f"GET with headers {list(auth_headers.keys())} -> HTTP {get_exc.code}")
        except URLError as get_exc:
            errors.append(f"GET with headers {list(auth_headers.keys())} -> network error: {get_exc}")

        # Fall back to POST JSON for endpoints requiring request body.
        payload = json.dumps({"url": video_url}).encode("utf-8")
        post_req = Request(
            endpoint,
            data=payload,
            method="POST",
            headers={**base_headers, **auth_headers, "Content-Type": "application/json"},
        )
        try:
            with urlopen(post_req, timeout=60, context=ssl_context) as response:
                body = response.read().decode("utf-8")
                return json.loads(body)
        except HTTPError as post_exc:
            error_body = post_exc.read().decode("utf-8", errors="replace")
            if post_exc.code == 403 and "1010" in error_body:
                raise RuntimeError(
                    "HTTP 403 (error code 1010). Access blocked by provider edge policy. "
                    "Confirm your Supadata endpoint/key are correct for your account region, "
                    "and try setting the exact endpoint from your Supadata dashboard via --endpoint."
                ) from post_exc
            errors.append(
                f"POST with headers {list(auth_headers.keys())} -> HTTP {post_exc.code}: {error_body}"
            )
        except URLError as post_exc:
            errors.append(f"POST with headers {list(auth_headers.keys())} -> network error: {post_exc}")

    raise RuntimeError(
        "All Supadata auth/method attempts failed. "
        + " | ".join(errors[-6:])
    )


def extract_transcript_text(data: Dict) -> str:
    # Handle common response structures.
    if isinstance(data.get("transcript"), str):
        return data["transcript"].strip()

    if isinstance(data.get("text"), str):
        return data["text"].strip()

    segments = (
        data.get("segments")
        or data.get("captions")
        or data.get("data")
        or data.get("content")
    )
    if isinstance(segments, list):
        chunks: List[str] = []
        for item in segments:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                chunks.append(item["text"].strip())
            elif isinstance(item, str):
                chunks.append(item.strip())
        return " ".join(part for part in chunks if part).strip()

    return ""


def build_ssl_context() -> ssl.SSLContext:
    insecure = os.getenv("SUPADATA_INSECURE_SSL", "").strip().lower() in {"1", "true", "yes"}
    if insecure:
        return ssl._create_unverified_context()  # noqa: SLF001

    cert_file = os.getenv("SSL_CERT_FILE", "").strip()
    if cert_file:
        return ssl.create_default_context(cafile=cert_file)

    try:
        import certifi  # type: ignore

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:  # noqa: BLE001
        return ssl.create_default_context()


def append_entry(
    output_path: Path, person_name: str, video_url: str, response_data: Dict, transcript_text: str
) -> None:
    title = (
        response_data.get("title")
        or response_data.get("video_title")
        or response_data.get("name")
        or "Untitled YouTube Video"
    )
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    with output_path.open("a", encoding="utf-8") as f:
        f.write(f"\n## {person_name}\n")
        f.write(f"- Source: [{title}]({video_url})\n")
        f.write(f"- Collected: {timestamp}\n")
        if transcript_text:
            f.write("- Transcript:\n\n")
            f.write(f"{transcript_text}\n")
        else:
            f.write("- Transcript: (No transcript text returned by API)\n")
            f.write("- Raw response:\n\n")
            f.write(f"```json\n{json.dumps(response_data, indent=2)}\n```\n")
        f.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch YouTube transcripts from Supadata using URLs in research/sources.md"
    )
    parser.add_argument(
        "--sources",
        default=str(SCRIPT_DIR / "sources.md"),
        help="Path to markdown sources file (default: research/sources.md next to script)",
    )
    parser.add_argument(
        "--output",
        default=str(SCRIPT_DIR / "youtube-transcripts.md"),
        help="Output markdown file path (default: research/youtube-transcripts.md next to script)",
    )
    parser.add_argument(
        "--endpoint",
        default=os.getenv("SUPADATA_ENDPOINT", DEFAULT_ENDPOINT),
        help=f"Supadata endpoint (default: {DEFAULT_ENDPOINT})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of videos to process (0 = no limit)",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable SSL certificate verification (only for local cert issues).",
    )
    args = parser.parse_args()

    if args.insecure:
        os.environ["SUPADATA_INSECURE_SSL"] = "1"

    api_key = os.getenv("SUPADATA_API_KEY")
    if not api_key:
        print("Missing SUPADATA_API_KEY environment variable.", file=sys.stderr)
        return 1

    sources_path = Path(args.sources)
    output_path = Path(args.output)
    if not sources_path.exists():
        print(f"Sources file not found: {sources_path}", file=sys.stderr)
        return 1

    targets = parse_sources_for_videos(sources_path)
    if args.limit > 0:
        targets = targets[: args.limit]

    if not targets:
        print("No direct YouTube video links found in sources file.")
        return 0

    print(f"Found {len(targets)} video URL(s).")
    success_count = 0
    for person_name, video_url in targets:
        print(f"- Fetching transcript for {person_name}: {video_url}")
        try:
            data = supadata_request(args.endpoint, api_key, video_url)
            transcript_text = extract_transcript_text(data)
            append_entry(output_path, person_name, video_url, data, transcript_text)
            success_count += 1
        except Exception as exc:  # noqa: BLE001
            with output_path.open("a", encoding="utf-8") as f:
                f.write(f"\n## {person_name}\n")
                f.write(f"- Source: {video_url}\n")
                f.write(f"- Error: {exc}\n\n")
            print(f"  Error: {exc}", file=sys.stderr)

    print(f"Done. Wrote {success_count}/{len(targets)} transcript entries to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
