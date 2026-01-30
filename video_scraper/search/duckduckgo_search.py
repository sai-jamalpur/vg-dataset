import re
import time
import random
from typing import List, Optional, Set, Any, Dict
from ddgs import DDGS
from urllib.parse import urlparse
from video_scraper.config import USER_AGENTS, SEARCH_DELAY_MIN, SEARCH_DELAY_MAX, MAX_RETRIES, SEARCH_FETCH_LIMIT
from video_scraper.utils import logger


class YouTubeSearcher:
    def __init__(self, existing_urls: Optional[Set[str]] = None, db_manager: Any = None):
        self.existing_urls = existing_urls or set()
        self.request_count = 0
        self.db_manager = db_manager
        self.region = "wt-wt"
        self.safesearch = "moderate"

    def _extract_video_id(self, url: str) -> Optional[str]:
        patterns = [
            r'(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/)([a-zA-Z0-9_-]{11})',
            r'youtube\.com\/shorts\/([a-zA-Z0-9_-]{11})',
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    def _is_valid_youtube_url(self, url: str) -> bool:
        return bool(self._extract_video_id(url))

    def _is_youtube_domain(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            host = (parsed.netloc or "").lower()
            return "youtube.com" in host
        except Exception:
            return False

    def _is_shorts_url(self, url: str) -> bool:
        return "/shorts/" in url.lower()

    def _normalize_url(self, url: str) -> str:
        video_id = self._extract_video_id(url)
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"
        return url

    def _extract_result_url(self, result: Dict[str, Any]) -> Optional[str]:
        url = result.get("content") or result.get("embed_url") or result.get("url")
        return url
    
    def _parse_duration_seconds(self, duration_str: Optional[str]) -> Optional[int]:
        if not duration_str or not isinstance(duration_str, str):
            return None
        try:
            parts = duration_str.strip().split(":")
            parts = [int(p) for p in parts]
            if len(parts) == 3:
                h, m, s = parts
                return h * 3600 + m * 60 + s
            if len(parts) == 2:
                m, s = parts
                return m * 60 + s
            if len(parts) == 1:
                return parts[0]
        except Exception:
            return None
        return None

    def search_videos(
        self,
        query: str,
        max_results: int = 50,
        exclude_existing: bool = True,
        topic: Optional[str] = None,
        subtopic: Optional[str] = None,
        exclude_shorts: bool = True,
        max_duration_seconds: Optional[int] = 900,
        require_youtube_domain: bool = True,
    ) -> List[str]:
        retries = 0
        while retries <= MAX_RETRIES:
            try:
                self.request_count += 1
                logger.info(f"Searching for: {query}")

                fetch_count = min(SEARCH_FETCH_LIMIT, max_results * 5)
                with DDGS() as ddgs:
                    results = list(
                        ddgs.videos(
                            query,
                            max_results=fetch_count,
                            region=self.region,
                            safesearch=self.safesearch,
                        )
                    )

                video_urls = []
                for result in results:
                    url = self._extract_result_url(result) or ""
                    if not url:
                        continue
                    if require_youtube_domain and not self._is_youtube_domain(url):
                        continue
                    if exclude_shorts and self._is_shorts_url(url):
                        continue
                    if self._is_valid_youtube_url(url):
                        normalized_url = self._normalize_url(url)
                        
                        if exclude_existing and normalized_url in self.existing_urls:
                            logger.debug(f"Skipping existing URL: {normalized_url}")
                            continue
                        if max_duration_seconds is not None:
                            d = self._parse_duration_seconds(result.get("duration"))
                            if d is not None and d > max_duration_seconds:
                                continue
                        video_urls.append(normalized_url)

                logger.info(f"Found {len(video_urls)} new videos for query: {query}")
                
                if self.db_manager and len(video_urls) > 0:
                    try:
                        self.db_manager.insert_search_log(
                            topic=topic,
                            subtopic=subtopic,
                            result_count=len(video_urls)
                        )
                    except Exception:
                        pass
                
                delay = max(2.0, random.uniform(SEARCH_DELAY_MIN, SEARCH_DELAY_MAX))
                time.sleep(delay)
                
                return video_urls

            except Exception as e:
                retries += 1
                logger.warning(f"Error searching for videos (Attempt {retries}/{MAX_RETRIES + 1}): {e}")
                
                if retries <= MAX_RETRIES:
                    sleep_time = random.uniform(SEARCH_DELAY_MIN, SEARCH_DELAY_MAX) * (2 ** retries)
                    logger.info(f"Waiting {sleep_time:.2f}s before retry...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Max retries reached for query: {query}")
                    raise e
        return []

    def search_videos_for_topic(
        self,
        topic: str,
        subtopics: List[str],
        max_results_per_subtopic: int = 3,
    ) -> List[str]:
        all_urls = []
        
        for subtopic in subtopics:
            if self.existing_urls and len(all_urls) >= max_results_per_subtopic * len(subtopics):
                break
            
            queries = [
                f"{topic} {subtopic}",
                f"{subtopic} tutorial",
                f"{subtopic} explained",
                f"{subtopic} educational",
            ]
            collected = []
            for q in queries:
                if len(collected) >= max_results_per_subtopic:
                    break
                urls = self.search_videos(
                    q,
                    max_results=min(10, max_results_per_subtopic - len(collected)),
                    topic=topic,
                    subtopic=subtopic,
                )
                collected.extend(urls)
                time.sleep(2)
            urls = collected
            all_urls.extend(urls)
        
        unique_urls = list(dict.fromkeys(all_urls))
        logger.info(f"Total unique videos found for topic '{topic}': {len(unique_urls)}")
        
        return unique_urls
    
    def search_videos_for_subtopic(self, topic: str, subtopic: str, max_results: int = 10) -> List[str]:
        queries = [
            f"{topic} {subtopic} video",
            f"{subtopic} educational video",
            f"{subtopic} animated video",
        ]
        collected: List[str] = []
        seen = set()
        for q in queries:
            if len(collected) >= max_results:
                break
            need = max_results - len(collected)
            urls = self.search_videos(
                q,
                max_results=need,
                exclude_existing=True,
                topic=topic,
                subtopic=subtopic,
                exclude_shorts=True,
                max_duration_seconds=900,
                require_youtube_domain=True,
            )
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    collected.append(u)
                if len(collected) >= max_results:
                    break
        return collected

    def update_existing_urls(self, new_urls: Set[str]):
        self.existing_urls.update(new_urls)
        logger.debug(f"Updated existing URLs cache. Total: {len(self.existing_urls)}")
