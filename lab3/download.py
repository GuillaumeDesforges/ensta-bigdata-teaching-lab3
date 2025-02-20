"""
Download a day's worth of data from adsbexchange.com
"""

from pathlib import Path
import urllib.request
import urllib.error
from datetime import datetime, timedelta, date
import os
import concurrent.futures
import ssl
import argparse


def download_file(url, output_path: Path):
    if output_path.exists():
        return
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Linux"',
    }
    request = urllib.request.Request(url)
    for key, value in headers.items():
        request.add_header(key, value)
    with urllib.request.urlopen(request, context=context, timeout=10) as response:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        content = response.read()
        with open(output_path, "wb") as f:
            f.write(content)


def urls_of_day(day: date, output_dir: Path, delta=timedelta(seconds=5)):
    """
    Generate URLs within a day at a given delta
    """
    current_time = datetime.combine(day, datetime.min.time())
    while current_time.date() == day:
        url = current_time.strftime(
            "https://samples.adsbexchange.com/readsb-hist/%Y/%m/%d/%H%M%SZ.json.gz"
        )
        output_path = output_dir / current_time.strftime("%Y%m%d%H%M%SZ.json.gz")
        yield url, output_path
        current_time += delta


def download_day_files(day: date, output_dir: Path, n_executors=5):
    """
    Download all files for a given day
    """
    urls = list(urls_of_day(day, output_dir))
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_executors) as executor:
        future_to_url = {
            executor.submit(download_file, url, output_path): (url, output_path)
            for url, output_path in urls
        }

        for future in concurrent.futures.as_completed(future_to_url):
            url, _ = future_to_url[future]
            try:
                future.result()
            except Exception as e:
                print(f"{url} error {str(e)}")
            else:
                print(f"{url} success")


def _parse_date(date_str: str) -> date:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Date must be in YYYY-MM-DD format")


def main():
    parser = argparse.ArgumentParser(
        description="Download ADS-B Exchange data for a specific date"
    )
    parser.add_argument(
        "--day",
        type=_parse_date,
        required=True,
        help="Date to download data for (YYYY-MM-DD format)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data"),
        help="Directory to store downloaded files (default: ./data)",
    )

    args = parser.parse_args()
    output_dir: Path = args.output_dir
    day: date = args.day

    if not output_dir.exists():
        raise FileNotFoundError(f"{args.output_dir} does not exist.")
    if not output_dir.is_dir():
        raise NotADirectoryError(f"{args.output_dir} is not a directory.")

    download_day_files(day, output_dir)


if __name__ == "__main__":
    main()
