"""
Installation requirements:
pip install playwright
playwright install  # To download browser binaries
"""

import asyncio
import json
import re
import logging
from dateutil import parser
from urllib.parse import urlparse, urljoin
from playwright.async_api import async_playwright


logging.basicConfig(level=logging.INFO)


class YouTubeVideoScraper:

    def __init__(self):
        self.browser = None
        self.page = None

    async def __aenter__(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,  # Run browser in background
        )
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
            print('--- channel_link:', channel_link)

            if not channel_link:
                raise Exception("Couldn't extract channel link")

            channel_url = await channel_link.get_attribute('href')
            channel_url = self._make_absolute_url(channel_url)
            print('--- channel_url:', channel_url)
            
            # Extract channel ID from URL
            channel_id_match = re.search(r'/@([^/]+)', channel_url)
            print('--- channel_id_match:', channel_id_match)

            if channel_id_match:
                channel_details["channel_id"] = channel_id_match.group(1)
        
            # Navigate to About page for more details
            await page.goto(f"{channel_url}/about", wait_until='networkidle', timeout=30000)
            
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
                details_elements = await page.query_selector_all('yt-formatted-string.ytd-channel-about-metadata-renderer')
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
        try:
            # Create a new page in new browser context
            page = await self.browser.new_page(locale='en-US')
            
            # Navigate to the video page, wait for the page to fully load
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            await page.goto(video_url, wait_until='networkidle', timeout=30000)
            
            # Wait for video info to load. This is bug-prone!
            # await page.wait_for_selector('div#info', timeout=10000)
            # Giving some time for additional content to load
            await page.wait_for_timeout(10000)
            
            # Extract video title
            try:
                title_element = await page.query_selector('h1.ytd-watch-metadata')
                video_title = await title_element.inner_text() if title_element else "Unknown Title"
            except Exception:
                video_title = "Unknown Title"
            
            # Extract view count
            try:
                view_element = await page.query_selector('div#info span.ytd-video-view-count-renderer')
                view_text = await view_element.inner_text() if view_element else "0 views"
                views = self._parse_count(view_text.split()[:-1])
            except Exception as e:
                logging.debug(f'Could not extract views for video "{video_id}": {e}')
                views = 0
            
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
            
            # Extract upload date. using locale='en-US' in browser context
            # preferring locator api https://stackoverflow.com/a/76745106/4615806
            try:
                date_element = page.locator('div#info yt-formatted-string.ytd-video-primary-info-renderer')
                upload_date = await date_element.all_inner_texts() if date_element else "Unknown Date"
                upload_date = parser.parse(" ".join(upload_date), fuzzy=True)
            except Exception:
                upload_date = "Unknown Date"
            
            # Extract channel name
            try:
                channel_element = await page.query_selector('yt-formatted-string.ytd-channel-name a')
                channel_name = await channel_element.inner_text() if channel_element else "Unknown Channel"
            except Exception:
                channel_name = "Unknown Channel"
            
            # TODO: this is unstable
            # Extract additional channel details
            channel_details = await self._extract_channel_details(page)
            # channel_details = {}

            # Compile comprehensive video statistics
            video_stats = {
                "video_id": video_id,
                "title": video_title,
                "views": views,
                "likes": likes,
                "comments": comments,
                "shares": shares,
                "dislikes": dislikes,
                "upload_date": str(upload_date),
                "channel_name": channel_name,
                "url": video_url,
                **channel_details  # Merge channel details
            }
            
            return video_stats
        
        except Exception as e:
            logging.debug(f'Error scraping video "{video_id}": {e}')
            return {"error": str(e), "video_id": video_id}
        
        finally:
            if page:
                await page.close()

    async def scrape_multiple_videos(self, video_ids):
        """
        Scrape statistics for multiple videos.
        Uses Playwight automation to wait on Javascript, pop ups, etc.

        :param list video_ids: List of YouTube video IDs
        :returns list: List of video statistics

        TODO: async generator -> yield datum one by one not to compromise entire result set

        {   "video_id": "9eHseYggb-I",
            "title": "Introducing JESUS: A new, animated family film",
            "views": 5250,
            "channel_id": "Unknown",
            "channel_name": "Jesus Film",
            "url": "https://www.youtube.com/watch?v=9eHseYggb-I",

            # TODO 👇👇👇
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "dislikes": 0,
            "upload_date": "",
            "subscribers_gained": 0,
            "subscribers_lost": 0,
            "country": "Unknown",
            "language_name": "Unknown",
            "language_code": "Unknown"
        }
        """
        tasks = [self.scrape_video_stats(video_id) for video_id in video_ids]
        return await asyncio.gather(*tasks)


async def scrape_multiple_videos(video_ids, verbose=False):
    """
    Scrape videos from Youtube website.
    """
    
    # Create scraper and scrape videos
    async with YouTubeVideoScraper() as scraper:
        # Scrape multiple videos concurrently
        results = await scraper.scrape_multiple_videos(video_ids)
        
        # Print or save results
        if verbose:
            for result in results:
                print(json.dumps(result, indent=2))
                print("\n---\n")
                
        return results



if __name__ == "__main__":
    """
    Example usage: python lib/scraper.py
    Watch for output file: data/youtube_video_stats.json
    """

    # Example usage / video IDs (replace with actual video IDs)
    video_ids = [
        # "9eHseYggb-I",  # Video unavailable: This video is private
        "uuo2KqoJxsc",
        # "UJfX-ZrDZmU",
        # "0_jC8Lg-oxY"
    ]

    # Optionally, save results to a JSON file
    results = asyncio.run(scrape_multiple_videos(video_ids, verbose=True))
    with open('data/youtube_video_stats.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


