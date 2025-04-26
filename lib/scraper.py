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
from helpers import IO_RATE_LIMIT, IO_TIMEOUT, IO_CONCURRENCY_LIMIT, LOG_LEVEL, \
    map_language


logging.basicConfig(level=LOG_LEVEL)


class YouTubeVideoScraper:

    def __init__(self, concurrency=None):
        self.browser = None
        self.page = None
        self.concurrency = concurrency or int(IO_CONCURRENCY_LIMIT)

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
                            channel_details["language_code"] = map_language(language)
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
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        video_stats = {
            "video_id": video_id,
            "title": "Unknown",
            "duration": "Unknown",
            "view_count": 0,
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "dislikes": 0,
            "published_at": "Unknown",
            "upload_date": "Unknown",
            "channel_name": "Unknown",
            "url": video_url,
            "thumbnail_url": "Unknown",
        }

        try:
            # Create a new page in new browser context
            page = await self.browser.new_page(locale='en-US')
            page.set_default_timeout(IO_TIMEOUT)  

            # Navigate to the video page, wait for the page to fully load
            # wait_until='networkidle' waits till there are no more than 0 network connections for at least 500 milliseconds.
            # wait_until='domcontentloaded' => less strict page load condition
            await page.goto(video_url, wait_until='domcontentloaded')  

             # Wait for title to be visible (a safe indicator the page has loaded key elements)
            await page.wait_for_selector('h1.ytd-watch-metadata yt-formatted-string', state='visible', timeout=90000)

            # If the reels or video pages use iframes or lazy-loading content, you may need to scroll or interact:
            await page.mouse.wheel(0, 1000)
            
            # Give JavaScript more time to execute
            await asyncio.sleep(3)

            # Extract video title
            try:
                title_element = await page.query_selector('h1.ytd-watch-metadata yt-formatted-string')
                video_stats["title"] = await title_element.inner_text() if title_element else "Unknown Title"
            except Exception as e:
                logging.debug(f'Could not extract title for video "{video_id}": {e}')

            # Extract view count
            try:
                view_element = await page.query_selector('div#info span.ytd-video-view-count-renderer')
                view_text = await view_element.inner_text() if view_element else "0 views"
                video_stats["view_count"] = self._parse_count(view_text.split()[:-1])
            except Exception as e:
                logging.debug(f'Could not extract views for video "{video_id}": {e}')

            # Extract likes count - try multiple potential selectors
            # Try to get aria-label first, fallback to inner text 
            try:
                like_selectors = [
                    'ytd-menu-renderer button[aria-label*="like"]',
                    'like-button-view-model button',
                    'segmented-like-button-view-model button',
                    '#top-level-buttons-computed button:first-child'
                ]
                for selector in like_selectors:
                    like_element = await page.query_selector(selector)
                    if like_element:
                        like_text = await like_element.get_attribute('aria-label')
                        if not like_text:
                            like_text = await like_element.inner_text()
                        if like_text:
                            match = re.search(r'\b\d+(?:\.\d+)?[KM]?\b', like_text)
                            video_stats["likes"] = self._parse_count(match.group())
                            break
            except Exception as e:
                logging.debug(f'Could not extract likes for video "{video_id}": {e}')
                video_stats["likes"] = 0

            # Extract comments count
            # Try to scroll down to make sure comments section is loaded
            try:

                await page.wait_for_selector('#comments', timeout=5000)
                await page.evaluate('''() => { window.scrollBy(0, 800); }''')

                comment_selectors = [
                    '#comments #count .count-text',
                    'h2.ytd-comments-header-renderer',
                    'yt-formatted-string.ytd-comments-header-renderer',
                ]
                for selector in comment_selectors:
                    comments_element = await page.query_selector(selector)
                    if comments_element:
                        comments_text = await comments_element.inner_text()
                        if comments_text:
                            video_stats["comments"] = comments_text
                            match = re.search(r'\b\d+(?:\.\d+)?[KM]?\b', comments_text)
                            if match:
                                video_stats["comments"] = self._parse_count(match.group())
                            else:
                                video_stats["comments"] = self._parse_count(comments_text.split()[0])
                        break
                
            except Exception as e:
                logging.debug(f'Could not extract comments for video "{video_id}": {e}')


            # Extract shares count (challenging to scrape accurately)
            shares = 0  # YouTube typically doesn't show share count directly

            # Extract dislikes (no longer directly shown on YouTube)
            dislikes = 0  # Dislikes are hidden on YouTube

            # Extract publish date. using locale='en-US' in browser context
            # preferring locator api https://stackoverflow.com/a/76745106/4615806
            try:
                date_element = page.locator('div#info yt-formatted-string.ytd-video-primary-info-renderer')
                video_stats["published_at"] = await date_element.all_inner_texts() if date_element else "Unknown Date"
                video_stats["published_at"] = parser.parse(" ".join(video_stats["published_at"]), fuzzy=True)
            except Exception:
                logging.debug(f'Could not extract publish date for video "{video_id}": {e}')

            # Extract duration from iso8601 into seconds
            video_stats["duration"] = await page.evaluate(
                """document.querySelector('meta[itemprop=\"duration\"]').content""")

            # Extract channel name
            try:
                channel_element = await page.query_selector('yt-formatted-string.ytd-channel-name a')
                video_stats["channel_name"] = await channel_element.inner_text() if channel_element else "Unknown Channel"
            except Exception:
                logging.debug(f'Could not extract channel name for video "{video_id}": {e}')

            # TODO: this is unstable
            # Extract additional channel details
            channel_details = await self._extract_channel_details(page)
            if channel_details:
                video_stats.update(channel_details)

        except Exception as e:
            raise VideoError(video_id, f'Error scraping video "{video_id}"', exc=e)

        finally:
            if page:
                await page.close()

        logging.debug(f"Scraped video: {video_stats['title']} ({video_stats['view_count']} views)\n", video_stats)
        return Video(**video_stats)


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
                ], max_per_second=IO_RATE_LIMIT, max_at_once=self.concurrency)

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
        "csv_output_path": f"data/video-ids-three-scraped-out.csv",
        "name": f"Scrape videos with {IO_TIMEOUT}ms timeout",
        "dry_run": True,
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

