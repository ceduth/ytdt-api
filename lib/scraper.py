#!/usr/bin/env python3
"""
Installation requirements:
pip install playwright
playwright install  # To download browser binaries
"""

import functools
import os
import re
import asyncio
import json
import logging

import urllib.parse
from collections import defaultdict
from typing import Dict

import aiometer
from dateutil import parser
from urllib.parse import urlparse, urljoin
from playwright.async_api import async_playwright
from tqdm import tqdm

from models import DataPipeline, Video, asdict
from lib.exceptions import AsyncException, VideoError
from helpers import IO_RATE_LIMIT, IO_TIMEOUT, IO_CONCURRENCY_LIMIT

logging.basicConfig(
    level=os.environ.get('LOGLEVEL', logging.INFO))


class YouTubeVideoScraper:

    def __init__(self, concurrency=IO_CONCURRENCY_LIMIT):
        self.browser = None
        self.page = None
        self.concurrency = concurrency

    async def __aenter__(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True)  # Run browser in background
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.browser.close()
        await self.playwright.stop()

    def _parse_count(self, count_str):
        """
        Parse view, like, or other numeric count from string.
        Converts K, M, etc. to actual numbers.
        """
        count_str = "".join(count_str)
        if not count_str or not isinstance(count_str, str):
            return 0

        # Remove non-numeric characters except dots and K, M, B
        count_str = count_str.replace(',', '').replace(' ', '')

        multipliers = {
            'K': 1000,
            'M': 1000000,
            'B': 1000000000
        }

        # Check if last character is a multiplier
        if count_str[-1] in multipliers:
            try:
                number = float(count_str[:-1]) * multipliers[count_str[-1]]
                return int(number)
            except ValueError:
                return 0

        try:
            return int(count_str)
        except ValueError:
            return 0

    def _make_absolute_url(self, url):
        """Whether url is FQDN and not relative"""
        if not bool(urlparse(url).netloc):
            url = urljoin('https://www.youtube.com', url)
        return url

    async def _extract_channel_details(self, page):
        """
        Extract advanced channel details.
        
        :param Page page: Playwright page object
        :returns dict: Channel details
        """
        channel_details = {
            "channel_id": "Unknown",
            "subscribers_gained": 0,
            "subscribers_lost": 0,
            "country": "Unknown",
            "language_name": "Unknown",
            "language_code": "Unknown"
        }

        try:
            # Try to extract channel URL and ID
            # $$('yt-formatted-string.ytd-channel-name a')[0].href in chrome devtools
            channel_link = await page.query_selector('yt-formatted-string.ytd-channel-name a')
            if not channel_link:
                raise AsyncException("Couldn't extract channel link")

            # Extract channel ID from URL
            channel_url = await channel_link.get_attribute('href')
            channel_url = self._make_absolute_url(channel_url)
            channel_id_match = re.search(r'/@([^/]+)', channel_url)

            if channel_id_match:
                channel_details["channel_id"] = urllib.parse.unquote(
                    channel_id_match.group(1))

            # Navigate to About page for more details
            await page.goto(f"{channel_url}/about", wait_until='networkidle', timeout=IO_TIMEOUT)

            # Try to extract country
            try:
                country_element = await page.query_selector('yt-formatted-string:has-text("Country")')
                if country_element:
                    country_text = await country_element.inner_text()
                    channel_details["country"] = country_text.split(":")[1].strip()
            except Exception:
                pass

            # Try to extract language (limited accuracy via web scraping)
            try:
                details_elements = await page.query_selector_all(
                    'yt-formatted-string.ytd-channel-about-metadata-renderer')
                for element in details_elements:
                    text = await element.inner_text()
                    # Basic language detection logic
                    if "Language" in text:
                        language_parts = text.split(":")
                        if len(language_parts) > 1:
                            language = language_parts[1].strip()
                            channel_details["language_name"] = language
                            # Attempt to map language name to code (very basic)
                            language_map = {
                                "English": "en",
                                "Spanish": "es",
                                "French": "fr",
                                "German": "de",
                                "Chinese": "zh",
                                "Japanese": "ja"
                            }
                            channel_details["language_code"] = language_map.get(language, "unknown")
            except Exception:
                pass

        except Exception as e:
            logging.debug(f'Error extracting channel details: {e}')

        return channel_details

    async def scrape_video_stats(self, video_id):
        """
        Scrape comprehensive statistics for a specific YouTube video.

        :param str video_id: YouTube video ID
        :returns dict: Detailed video statistics
        """

        page = None

        try:
            # Create a new page in new browser context
            page = await self.browser.new_page(locale='en-US')

            # Navigate to the video page, wait for the page to fully load
            # ie. until there are no more than 0 network connections for at least 500 milliseconds.
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            # await self._goto_page(page, video_url)
            await page.goto(video_url, wait_until='networkidle', timeout=IO_TIMEOUT)

            # Waiting for video info to load => missing view_count, upload_date
            # tried unsuccessfully following selectors: `div#info`, `video`, `#contents > ytd-video-renderer` eg.:
            await page.wait_for_selector('#contents', timeout=IO_TIMEOUT)
            # Rather, giving some time for additional content to load is reliable,
            # but not recommended in prod
            # await page.wait_for_timeout(20000)

            # Extract video title
            try:
                title_element = await page.query_selector('h1.ytd-watch-metadata')
                video_title = await title_element.inner_text() if title_element else "Unknown Title"
            except Exception as e:
                logging.debug(f'Could not extract title for video "{video_id}": {e}')
                video_title = "Unknown Title"

            # Extract view count
            try:
                view_element = await page.query_selector('div#info span.ytd-video-view-count-renderer')
                view_text = await view_element.inner_text() if view_element else "0 views"
                view_count = self._parse_count(view_text.split()[:-1])
            except Exception as e:
                logging.debug(f'Could not extract views for video "{video_id}": {e}')
                view_count = 0

            # Extract likes count
            try:
                like_button = await page.query_selector('yt-button-shape[title*="like"]')
                if like_button:
                    like_text = await like_button.inner_text()
                    likes = self._parse_count(like_text)
                else:
                    likes = 0
            except Exception as e:
                logging.debug(f'Could not extract likes for video "{video_id}": {e}')
                likes = 0

            # Extract comments count
            try:
                comments_element = await page.query_selector('yt-formatted-string.ytd-comments-header-renderer')
                if comments_element:
                    comments_text = await comments_element.inner_text()
                    comments = self._parse_count(comments_text.split()[0])
                else:
                    comments = 0
            except Exception as e:
                logging.debug(f'Could not extract comments for video "{video_id}": {e}')
                comments = 0

            # Extract shares count (challenging to scrape accurately)
            shares = 0  # YouTube typically doesn't show share count directly

            # Extract dislikes (no longer directly shown on YouTube)
            dislikes = 0  # Dislikes are hidden on YouTube

            # Extract publish date. using locale='en-US' in browser context
            # preferring locator api https://stackoverflow.com/a/76745106/4615806
            try:
                date_element = page.locator('div#info yt-formatted-string.ytd-video-primary-info-renderer')
                published_at = await date_element.all_inner_texts() if date_element else "Unknown Date"
                published_at = parser.parse(" ".join(published_at), fuzzy=True)
            except Exception:
                published_at = "Unknown Date"

            # Extract duration from iso8601 into seconds
            duration = await page.evaluate("""document.querySelector('meta[itemprop=\"duration\"]').content""")

            # Extract channel name
            try:
                channel_element = await page.query_selector('yt-formatted-string.ytd-channel-name a')
                channel_name = await channel_element.inner_text() if channel_element else "Unknown Channel"
            except Exception:
                channel_name = "Unknown Channel"

            # TODO: this is unstable
            # Extract additional channel details
            channel_details = await self._extract_channel_details(page)

            # Compile comprehensive video statistics
            video_stats = {
                "video_id": video_id,
                "title": video_title,
                "duration": duration,
                "view_count": view_count,
                "likes": likes,
                "comments": comments,
                "shares": shares,
                "dislikes": dislikes,
                "published_at": str(published_at),
                "upload_date": '',
                "channel_name": channel_name,
                "url": video_url,
                "thumbnail_url": '',
                **channel_details  # Merge channel details
            }

            return Video(**video_stats)

        except Exception as e:
            raise VideoError(video_id, f'Error scraping video "{video_id}"', exc=e)

        finally:
            if page:
                await page.close()

    async def scrape_multiple_videos(self, video_ids, progress_callback=None, **pipeline_kwargs):
        """
        Scrape multiple videos concurrently with progress tracking.
        Pushes results to data pipeline for saving to permanent storage reliably.
        Uses Playwright automation to wait on Javascript, pop ups, etc.

        Args:
            video_ids:
            progress_callback: Optional[Callable[[int, str], None]]
            video_ids list: List of YouTube video IDs
            pipeline_kwargs dict: optional kwargs for the data pipeline


        TODO: async generator -> yield datum one by one not to compromise entire result set

            {
            "video_id": "uuo2KqoJxsc",
            "title": "God's Rescue Plan",
            "duration": "PT2H30M",
            "published_at": "",
            "upload_date": "2023-04-13 00:00:00",
            "view_count": 4597725,
            "url": "https://www.youtube.com/watch?v=uuo2KqoJxsc",
            "channel_id": "Godlife",
            "channel_name": "GodLife.com",

            # TODO: Not implemented 👇👇👇
            "language_name": "Unknown",
            "language_code": "Unknown",
            "thumbnail_url": "",
            "country": "Unknown",
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "dislikes": 0,
            "subscribers_gained": 0,
            "subscribers_lost": 0,
            }

        """

        async def _scrape_to_pipeline(pipeline: DataPipeline, task_index, video_id):
            """
            # TODO: only AsyncException are currently properly formatted for saving to csv
            Args:
                pipeline :
                task_index:
                video_id:

            Returns:
            """
            try:
                if progress_callback:
                    await progress_callback(task_index, video_id)

                video = await self.scrape_video_stats(video_id)
                return await pipeline.enqueue(asdict(video)), 0

            except Exception as e:
                logging.debug(str(e))
                return await pipeline.enqueue(e.__dict__, is_error=True), -1

        async def run_tasks(video_ids: [str]):
            """ Scrape (with rate control) and save videos to the data pipeline  """

            async with DataPipeline(**pipeline_kwargs) as pipeline:

                desc = f'asynchronously scraping {len(video_ids)} videos'
                return await aiometer.run_all([
                    functools.partial(_scrape_to_pipeline, pipeline, i, v)
                    for i, v in enumerate(tqdm(video_ids, desc=desc))
                ], max_per_second=IO_RATE_LIMIT, max_at_once=IO_CONCURRENCY_LIMIT)

        return await run_tasks(video_ids)


async def scrape_multiple_videos(video_ids, progress_callback=None, **pipeline_kwargs):
    """
    Scrape videos from YouTube website.
    """

    # Create scraper and scrape videos concurrently
    async with YouTubeVideoScraper() as scraper:

        results = defaultdict(list)
        response = await scraper.scrape_multiple_videos(
            video_ids, progress_callback=progress_callback, **pipeline_kwargs)

        for item, err_code in response:
            key = 'videos' if err_code > -1 else 'errors'
            item = asdict(item)
            results[key] += [item]

        return results


if __name__ == "__main__":
    """
    Example usage: python lib/scraper.py
    Watch for output file: data/youtube_video_stats.json
    """

    # video IDs (replace with actual video IDs)
    video_ids = [

        # "Video unavailable"
        "9eHseYggb-I",  # This video is private
        "W7Tkx2oXIyk",  # This video is no longer available because the YouTube account associated with this video has been closed.

        # These are okay
        "uuo2KqoJxsc",
        "UJfX-ZrDZmU",
        "0_jC8Lg-oxY"
    ]

    pipeline_kwargs = {
        "csv_output_path": f"video-ids-three-scraped-out.csv",
        "name": f"Scrape videos with {IO_TIMEOUT}ms timeout",
        "dry_run": False,
    }

    # progress callback
    async def print_progress(completed: int, current_video: str):
        print(f"Progress: {completed} videos scraped. Currently processing: {current_video}")

    results = asyncio.run(scrape_multiple_videos(
        video_ids, progress_callback=print_progress, **pipeline_kwargs))

    # optionally, save results to a JSON file
    with open('../data/youtube_video_stats.json', 'w', encoding='utf-8-sig') as f:
        print(json.dumps(results, indent=2))
        json.dump(results, f, indent=2, ensure_ascii=False)

