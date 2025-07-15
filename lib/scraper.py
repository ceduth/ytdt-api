#!/usr/bin/env python3
"""
Installation requirements:
pip install playwright
playwright install  # To download browser binaries
"""

import os
import re
import json
import asyncio
import logging
import argparse
import functools

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
from utils.json import DateTimeEncoder
from utils.env import IO_RATE_LIMIT, IO_TIMEOUT, IO_CONCURRENCY_LIMIT, IO_BATCH_SIZE, LOG_LEVEL
from utils.helpers import map_language, load_video_ids_from_csv, split_kwargs, rename_file_with_extension


logging.basicConfig(level=LOG_LEVEL)


class YouTubeVideoScraper:

    def __init__(self, concurrency=None, max_per_second=None, timeout=None):
        self.browser = None
        self.page = None
        self.concurrency = int(concurrency or IO_CONCURRENCY_LIMIT)
        self.max_per_second = int(max_per_second or IO_RATE_LIMIT)
        self.timeout = int(timeout or IO_TIMEOUT)

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

    async def _extract_channel_id(self, page):
        """
        Extract the real YouTube channel ID (e.g., UCE2kA-abkzL1tdhmIpQYoEQ).
        
        :param Page page: Playwright page object
        :returns str: Channel ID or 'Unknown'
        """
        try:
            # Method 1: Extract from canonical link tag
            canonical_element = await page.query_selector('link[rel="canonical"]')
            if canonical_element:
                canonical_url = await canonical_element.get_attribute('href')
                if canonical_url and '/channel/' in canonical_url:
                    channel_id_match = re.search(r'/channel/([A-Za-z0-9_-]+)', canonical_url)
                    if channel_id_match:
                        logging.debug(f'Channel ID found in canonical URL: {channel_id_match.group(1)}')
                        return channel_id_match.group(1)

            # Method 2: Extract from ytInitialData JSON
            try:
                yt_initial_data = await page.evaluate('''
                    () => {
                        if (window.ytInitialData) {
                            return JSON.stringify(window.ytInitialData);
                        }
                        return null;
                    }
                ''')
                
                if yt_initial_data:
                    data = json.loads(yt_initial_data)
                    # Navigate through the complex YouTube data structure
                    video_details = data.get('contents', {}).get('twoColumnWatchNextResults', {}).get('results', {}).get('results', {}).get('contents', [])
                    
                    for content in video_details:
                        if 'videoPrimaryInfoRenderer' in content:
                            continue
                        elif 'videoSecondaryInfoRenderer' in content:
                            owner_info = content.get('videoSecondaryInfoRenderer', {}).get('owner', {}).get('videoOwnerRenderer', {})
                            browse_endpoint = owner_info.get('browseEndpoint', {})
                            if browse_endpoint and 'browseId' in browse_endpoint:
                                channel_id = browse_endpoint['browseId']
                                logging.debug(f'Channel ID found in ytInitialData: {channel_id}')
                                return channel_id
                            
                            # Alternative path in the data structure
                            navigation_endpoint = owner_info.get('navigationEndpoint', {}).get('commandMetadata', {}).get('webCommandMetadata', {})
                            if navigation_endpoint and 'url' in navigation_endpoint:
                                url = navigation_endpoint['url']
                                if '/channel/' in url:
                                    channel_id_match = re.search(r'/channel/([A-Za-z0-9_-]+)', url)
                                    if channel_id_match:
                                        logging.debug(f'Channel ID found in navigation endpoint: {channel_id_match.group(1)}')
                                        return channel_id_match.group(1)
            except Exception as e:
                logging.debug(f'Error parsing ytInitialData: {e}')

            # Method 3: Extract from meta tags
            meta_selectors = [
                'meta[property="og:url"]',
                'meta[name="twitter:url"]',
                'meta[itemprop="channelId"]'
            ]
            
            for selector in meta_selectors:
                meta_element = await page.query_selector(selector)
                if meta_element:
                    content = await meta_element.get_attribute('content')
                    if content and '/channel/' in content:
                        channel_id_match = re.search(r'/channel/([A-Za-z0-9_-]+)', content)
                        if channel_id_match:
                            logging.debug(f'Channel ID found in meta tag: {channel_id_match.group(1)}')
                            return channel_id_match.group(1)

            # Method 4: Extract from channel link and follow redirect
            channel_link = await page.query_selector('yt-formatted-string.ytd-channel-name a')
            if channel_link:
                channel_url = await channel_link.get_attribute('href')
                if channel_url:
                    channel_url = self._make_absolute_url(channel_url)
                    
                    # If it's a handle URL, we need to resolve it
                    if '/@' in channel_url:
                        try:
                            # Create a new page to navigate to channel
                            channel_page = await self.browser.new_page()
                            await channel_page.goto(channel_url, wait_until='domcontentloaded', timeout=10000)
                            
                            # Look for channel ID in the channel page
                            channel_canonical = await channel_page.query_selector('link[rel="canonical"]')
                            if channel_canonical:
                                canonical_href = await channel_canonical.get_attribute('href')
                                if canonical_href and '/channel/' in canonical_href:
                                    channel_id_match = re.search(r'/channel/([A-Za-z0-9_-]+)', canonical_href)
                                    if channel_id_match:
                                        logging.debug(f'Channel ID found via channel page redirect: {channel_id_match.group(1)}')
                                        await channel_page.close()
                                        return channel_id_match.group(1)
                            
                            await channel_page.close()
                        except Exception as e:
                            logging.debug(f'Error following channel redirect: {e}')

        except Exception as e:
            logging.debug(f'Error extracting channel ID: {e}')

        return 'Unknown'

    async def _extract_channel_details(self, page):
        """
        Extract advanced channel details.
        
        :param Page page: Playwright page object
        :returns dict: Channel details
        """
        channel_details = {
            "channel_id": "Unknown",
            "channel_title": "Unknown",
            "channel_handle": "Unknown",
            "subscribers_gained": 0,
            "subscribers_lost": 0,
            "country": "Unknown",
            "language_name": "Unknown",
            "language_code": "Unknown"
        }

        try:
            # Extract channel ID using the new method
            channel_details["channel_id"] = await self._extract_channel_id(page)

            # Try to extract channel URL and handle
            channel_link = await page.query_selector('yt-formatted-string.ytd-channel-name a')
            if not channel_link:
                raise AsyncException("Couldn't extract channel link")

            # Extract channel title (display name)
            channel_details["channel_title"] = await channel_link.inner_text() if channel_link else "Unknown"

            # Extract channel handle from URL
            channel_url = await channel_link.get_attribute('href')
            channel_url = self._make_absolute_url(channel_url)
            handle_match = re.search(r'/@([^/]+)', channel_url)

            if handle_match:
                channel_details["channel_handle"] = urllib.parse.unquote(handle_match.group(1))

            # Navigate to About page for more details
            await page.goto(f"{channel_url}/about", wait_until='networkidle', timeout=self.timeout)

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

    async def _extract_unlisted_status(self, page):
        """
        Extract whether a YouTube video is unlisted by checking for the "Unlisted" badge.
        
        :param Page page: Playwright page object
        :returns bool: True if video is unlisted, False otherwise
        """
        try:
            # Check for unlisted badge - it typically appears in the video metadata area
            unlisted_selectors = [
                # Primary selector for unlisted badge
                'ytd-badge-supported-renderer[aria-label*="Unlisted"]',
                'ytd-badge-supported-renderer:has-text("Unlisted")',
                # Alternative selectors for unlisted status
                'yt-formatted-string:has-text("Unlisted")',
                '.badge:has-text("Unlisted")',
                '[aria-label*="Unlisted"]',
                # Sometimes it appears in the video info section
                'ytd-video-primary-info-renderer :has-text("Unlisted")',
                'ytd-video-secondary-info-renderer :has-text("Unlisted")',
            ]
            
            for selector in unlisted_selectors:
                try:
                    element = await page.query_selector(selector)
                    if element:
                        # Double-check the text content contains "Unlisted"
                        text_content = await element.inner_text()
                        if "Unlisted" in text_content:
                            logging.debug(f'Video is unlisted - found badge with selector: {selector}')
                            return True
                        
                        # Also check aria-label for accessibility text
                        aria_label = await element.get_attribute('aria-label')
                        if aria_label and "Unlisted" in aria_label:
                            logging.debug(f'Video is unlisted - found aria-label with selector: {selector}')
                            return True
                except Exception as e:
                    logging.debug(f'Selector {selector} failed: {e}')
                    continue
            
            # Additional check in the video description/info area
            try:
                # Look for unlisted indicator in the broader video info section
                info_section = await page.query_selector('ytd-video-primary-info-renderer')
                if info_section:
                    info_text = await info_section.inner_text()
                    if "Unlisted" in info_text:
                        logging.debug('Video is unlisted - found in video info section')
                        return True
            except Exception as e:
                logging.debug(f'Error checking video info section for unlisted status: {e}')
            
            logging.debug('Video is not unlisted')
            return False
            
        except Exception as e:
            logging.debug(f'Error extracting unlisted status: {e}')
            return False
    
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
            "channel_id": "Unknown",
            "channel_title": "Unknown",
            "channel_handle": "Unknown",
            "url": video_url,
            "thumbnail_url": "Unknown",
            "is_unlisted": False,
        }

        try:
            # Ensure browser is initialized
            if not self.browser:
                raise RuntimeError("Browser is not initialized. Use YouTubeVideoScraper as an async context manager (with statement).")
            
            # Create a new page in new browser context
            page = await self.browser.new_page(locale='en-US')
            page.set_default_timeout(self.timeout)  

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

            # Extract unlisted status
            try:
                video_stats["is_unlisted"] = await self._extract_unlisted_status(page)
            except Exception as e:
                logging.debug(f'Could not extract unlisted status for video "{video_id}": {e}')
                video_stats["is_unlisted"] = False

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
                            # Extract all numbers from the text, including those with commas
                            # Match patterns like "12,345", "567890", "1.2K", "1.5M"
                            match = re.search(r'\b(\d{1,3}(?:,\d{3})*|\d+(?:\.\d+)?[KMB]?)\b', like_text)
                            if match:
                                # Remove commas before parsing
                                count_str = match.group(1).replace(',', '')
                                video_stats["likes"] = self._parse_count(count_str)
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
                            # Extract all numbers from the text, including those with commas
                            # Match patterns like "10,449", "503047", "1.2K", "1.5M"
                            match = re.search(r'\b(\d{1,3}(?:,\d{3})*|\d+(?:\.\d+)?[KMB]?)\b', comments_text)
                            if match:
                                # Remove commas before parsing
                                count_str = match.group(1).replace(',', '')
                                video_stats["comments"] = self._parse_count(count_str)
                            else:
                                # Fallback: try to extract first word and parse
                                first_word = comments_text.split()[0].replace(',', '')
                                video_stats["comments"] = self._parse_count(first_word)
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
            except Exception as e:
                logging.debug(f'Could not extract publish date for video "{video_id}": {e}')

            # Extract duration from iso8601 into seconds
            video_stats["duration"] = await page.evaluate(
                """document.querySelector('meta[itemprop=\"duration\"]').content""")

            # Extract channel details (includes channel_id, channel_title, channel_handle)
            try:
                channel_details = await self._extract_channel_details(page)
                if channel_details:
                    video_stats.update(channel_details)
            except Exception as e:
                logging.debug(f'Could not extract channel details for video "{video_id}": {e}')
                # Fallback to basic channel name extraction
                try:
                    channel_element = await page.query_selector('yt-formatted-string.ytd-channel-name a')
                    video_stats["channel_title"] = await channel_element.inner_text() if channel_element else "Unknown Channel"
                except Exception as fallback_e:
                    logging.debug(f'Could not extract basic channel name for video "{video_id}": {fallback_e}')

        except Exception as e:
            raise VideoError(video_id, f'Error scraping video "{video_id}"', exc=e)

        finally:
            if page:
                await page.close()

        logging.debug(f"Scraped video {video_stats['video_id']}: {video_stats['title']} ({video_stats['view_count']} views)\n")
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
            "channel_id": "UCE2kA-abkzL1tdhmIpQYoEQ",
            "channel_title": "GodLife.com",
            "channel_handle": "Godlife",

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
                logging.debug(f"Error scraping to pipeline, video {video_id}: {e}")
                return await pipeline.enqueue(e.__dict__, is_error=True), -1

        async def run_tasks(video_ids: list[str]):
            """ Scrape (with rate control) and save videos to the data pipeline  """

            async with DataPipeline(**pipeline_kwargs) as pipeline:

                desc = f'asynchronously scraping {len(video_ids)} videos'
                return await aiometer.run_all([
                    functools.partial(_scrape_to_pipeline, pipeline, i, v)
                    for i, v in enumerate(tqdm(video_ids, desc=desc))
                ], max_per_second=self.max_per_second, max_at_once=self.concurrency)

        return await run_tasks(video_ids)


async def scrape_multiple_videos(video_ids, progress_callback=None, **kwargs):
    """
    Scrape videos from YouTube website.
    Returns results dict, with the scraped videos into `results['videos']`, 
        errors into `results['errors']`
    """

    # Create scraper and scrape videos concurrently
    scraper_kwargs, pipeline_kwargs = split_kwargs({'concurrency', 'max_per_second', 'timeout'}, kwargs)
    async with YouTubeVideoScraper(**scraper_kwargs) as scraper:

        results = defaultdict(list)
        video_ids = [Video.clean_video_id(v) for v in video_ids if Video.clean_video_id(v)]
        response = await scraper.scrape_multiple_videos(
            video_ids, progress_callback=progress_callback, **pipeline_kwargs)

        for item, err_code in response:
            key = 'videos' if err_code > -1 else 'errors'
            item = asdict(item)
            results[key] += [item]

        return results


if __name__ == "__main__":
    """
    Scrape video_ids
    Watch for output file: data/youtube_video_stats.json
    Runs with dry_run=False, instructing not to flush pieline to disk.

    Example usage (ran from dir ytdt-api/)

        # with options
        PYTHONPATH=${PYTHONPATH}:. python lib/scraper.py data/video-ids-demo.csv \
                --csv_output_path data/scraped_output.csv \
                --ids_column yt_video_id \
                --timeout 1000
                --concurrency 10 \
                --max_per_second 5 

        # or demo data with default options
        PYTHONPATH=${PYTHONPATH}:. python lib/scraper.py 

    """

    arg_parser = argparse.ArgumentParser(description="Scrape video metadata from YouTube")
    arg_parser.add_argument('csv_input_path', type=str, help='Input CSV file path with video IDs')
    arg_parser.add_argument('--csv_output_path', type=str, help='Optional output CSV file path')
    arg_parser.add_argument('--ids_column', type=str, default='yt_video_id', help="Column name containing video IDs")
    arg_parser.add_argument('--timeout', type=int, default=IO_TIMEOUT, help="Scrape timeout in ms")
    arg_parser.add_argument('--concurrency', type=int, default=IO_CONCURRENCY_LIMIT, help="Number of concurrent tasks")
    arg_parser.add_argument('--max_per_second', type=int, default=IO_RATE_LIMIT, help="Max requests per second")
    arg_parser.add_argument('--data_queue_limit', type=int, default=IO_BATCH_SIZE, help="Data queue limit for pipeline (items flushed to disk)")
    arg_parser.add_argument('--dry_run', action='store_true', help="Run without saving to disk (dry run)")
    arg_parser.add_argument('--json', action='store_true', help='Also saves a XLSX file')
    args = arg_parser.parse_args()

    dry_run = True
    video_ids = load_video_ids_from_csv(args.csv_input_path, args.ids_column)
    csv_output_path = args.csv_output_path or \
        rename_file_with_extension(args.csv_input_path, suffix='scraped')

    pipeline_kwargs = {
        "name": f"Scrape videos with {args.timeout}ms timeout",
        "csv_output_path": csv_output_path,
        "data_queue_limit": args.data_queue_limit,
        "dry_run": args.dry_run,
    }

    scraper_kwargs = {
        "concurrency": args.concurrency,
        "max_per_second": args.max_per_second,
        "timeout": args.timeout
    }

    # progress callback
    async def print_progress(completed: int, current_video: str):
        print(f"Progress: {completed} videos scraped. Currently processing: {current_video}")

    # Scrape videos to `results['videos']`, errors to `results['errors']`
    results = asyncio.run(scrape_multiple_videos(
        video_ids, progress_callback=print_progress, **{**pipeline_kwargs, **scraper_kwargs}))

    # Optionally save to JSON
    if args.json and not args.dry_run:
        json_output_path = rename_file_with_extension(csv_output_path, 'json')
        logging.info(f'saving json file to: {json_output_path}')
        with open(json_output_path, 'w', encoding='utf-8-sig') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)
