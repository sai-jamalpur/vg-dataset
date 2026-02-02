import yt_dlp
import time
import random
import concurrent.futures
from pathlib import Path
from typing import Optional, Dict, Any, List
from video_scraper.config import (
    BASE_DIR,
    TEMP_DIR,
    MAX_VIDEO_DURATION_SECONDS,
    USER_AGENTS,
    DOWNLOAD_DELAY_MIN,
    DOWNLOAD_DELAY_MAX,
    MAX_RETRIES,
    BACKOFF_BASE_DELAY,
    BACKOFF_MAX_DELAY,
    BACKOFF_FACTOR,
    MAX_CONCURRENT_DOWNLOADS
)
from video_scraper.utils import logger

class VideoDownloader:
    def __init__(self):
        self.temp_dir = TEMP_DIR
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.session_user_agent = random.choice(USER_AGENTS)
        
        # Define cookie path based on user input
        self.cookie_file = BASE_DIR / "www.youtube.com_cookies.txt"
        
        if self.cookie_file.exists():
            logger.info(f"🍪 Cookie file found and loaded: {self.cookie_file.name}")
        else:
            logger.warning(f"⚠️ Cookie file not found at {self.cookie_file}. Continuing without authentication.")

    def _get_ydl_options(self, output_path: str = None) -> Dict[str, Any]:
        """
        Returns optimized yt-dlp options with 'Get Anything' Fallback.
        """
        opts = {
            # --- 1. The "Get Anything" Cascade Strategy ---
            "format": ( 
                # Tier 1: The "Perfect" File (Small, MP4)
                "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/"  
                
                # Tier 2: The "Good" File (Small, Any Container like WebM)
                "bestvideo[height<=480]+bestaudio/"                    
                
                # Tier 3: The "Acceptable" File (720p or lower, Any Container)
                "bestvideo[height<=720]+bestaudio/"   

                # Tier 4: The "Single File" Fallback (Sometimes video/audio aren't separate)
                "best[height<=720]/"

                # Tier 5: The "Panic Button" - Download literally anything available
                "best"                                                 
            ),
            
            # --- 2. Authentication (Cookies) ---
            "cookiefile": str(self.cookie_file) if self.cookie_file.exists() else None,

            # --- 3. Stealth & Headers ---
            "quiet": True,
            "no_warnings": True,
            "extractor_args": {'youtube': {'player_client': ['android', 'web']}}, 
            "http_headers": {
                "User-Agent": self.session_user_agent,
                "Accept-Language": "en-US,en;q=0.9",
            },
            
            # --- 4. Filters ---
            "match_filter": self._filter_shorts_and_duration,

            # --- 5. Reliability ---
            "nocheckcertificate": False, 
            "ignoreerrors": True,
            "retries": MAX_RETRIES,
            "fragment_retries": MAX_RETRIES,
            
            # --- 6. Output ---
            "outtmpl": output_path if output_path else str(self.temp_dir / "%(id)s.%(ext)s"),
        }
        return opts

    def _filter_shorts_and_duration(self, info_dict, *, incomplete=False):
        """
        Custom filter to reject Shorts and long videos.
        """
        duration = info_dict.get('duration')
        if duration and duration > MAX_VIDEO_DURATION_SECONDS:
            return f"Video too long: {duration}s"
        
        width = info_dict.get('width')
        height = info_dict.get('height')
        url = info_dict.get('webpage_url', '')

        if '/shorts/' in url:
            return "Rejected: Detected as YouTube Short URL"
        
        if width and height and height > width:
            return "Rejected: Vertical video aspect ratio (likely a Short)"
            
        return None

    def _get_video_info(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            ydl_opts = self._get_ydl_options()
            ydl_opts.update({"skip_download": True})
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                return info
        except Exception as e:
            logger.error(f"Error getting video info for {url}: {e}")
            return None

    def download_with_info(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Downloads video and returns dict with path and info.
        Compatible with Orchestrator expectations.
        """
        try:
            # 1. Get info first
            info = self._get_video_info(url)
            if not info:
                return None
            
            # 2. Download using the info
            path = self.download_video(url, pre_fetched_info=info)
            if not path:
                return None
                
            return {
                "path": path,
                "info": info
            }
        except Exception as e:
            logger.error(f"Error in download_with_info for {url}: {e}")
            return None

    def download_video(
        self,
        url: str,
        filename: Optional[str] = None,
        pre_fetched_info: Optional[Dict[str, Any]] = None
    ) -> Optional[Path]:
        try:
            # Use pre-fetched info if available to save a request
            video_info = pre_fetched_info
            if not video_info:
                video_info = self._get_video_info(url)
            
            if not video_info:
                return None

            video_id = video_info.get("id", "unknown")
            
            if filename is None:
                filename = f"{video_id}.mp4"
            output_path = self.temp_dir / filename
            output_template = str(self.temp_dir / f"{video_id}.%(ext)s")

            if output_path.exists() and output_path.stat().st_size > 0:
                logger.info(f"Skipping download, file exists: {output_path}")
                return output_path

            logger.info(f"Downloading (Flexible Format): {url} -> {output_path}")
            
            ydl_opts = self._get_ydl_options(output_template)
            
            attempts = 0
            while attempts <= MAX_RETRIES:
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        ydl.download([url])
                    break 
                except Exception as e:
                    attempts += 1
                    error_str = str(e).lower()
                    
                    if "429" in error_str or "too many requests" in error_str:
                        delay = min(BACKOFF_MAX_DELAY, BACKOFF_BASE_DELAY * (BACKOFF_FACTOR ** attempts))
                        logger.warning(f"🛑 Rate Limited (429). Cooling down for {delay}s...")
                        time.sleep(delay)
                    elif "sign in" in error_str:
                        logger.error("🛑 Authentication failed! Check your cookies.txt file expiration.")
                        return None
                    elif "unavailable" in error_str or "private" in error_str:
                        logger.warning(f"Video unavailable: {url}")
                        return None
                    else:
                        delay = random.uniform(DOWNLOAD_DELAY_MIN, DOWNLOAD_DELAY_MAX)
                        logger.warning(f"Retry ({attempts}): {e}")
                        time.sleep(delay)

            # Robust file finder: Look for ANY extension
            candidates = list(self.temp_dir.glob(f"{video_id}.*"))
            # Filter out non-video files just in case (like .json or .part)
            valid_candidates = [
                p for p in candidates 
                if p.stat().st_size > 0 and p.suffix.lower() in ['.mp4', '.webm', '.mkv', '.flv', '.avi', '.mov']
            ]
            
            if valid_candidates:
                # Mimic human "watching" time
                time.sleep(random.uniform(DOWNLOAD_DELAY_MIN, DOWNLOAD_DELAY_MAX))
                return valid_candidates[0]
            else:
                return None

        except Exception as e:
            logger.error(f"Critical error downloading {url}: {e}")
            return None

    def download_videos_parallel(
        self,
        urls: List[str],
        max_videos: Optional[int] = None,
        max_workers: int = MAX_CONCURRENT_DOWNLOADS
    ) -> List[Path]:
        if max_videos:
            urls = urls[:max_videos]

        downloaded_files = []
        logger.info(f"Starting parallel download of {len(urls)} videos using Cookies...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(self.download_video, url): url for url in urls}
            
            for future in concurrent.futures.as_completed(future_to_url):
                path = future.result()
                if path:
                    downloaded_files.append(path)

        return downloaded_files

    def cleanup_temp_files(self):
        try:
            files = list(self.temp_dir.glob("*"))
            for file in files:
                if file.is_file():
                    file.unlink()
        except Exception as e:
            logger.error(f"Error cleaning up temp files: {e}")