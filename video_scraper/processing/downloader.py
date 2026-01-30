import yt_dlp
import time
import random
from pathlib import Path
from typing import Optional, Dict, Any
from video_scraper.config import (
    TEMP_DIR,
    MAX_VIDEO_DURATION_SECONDS,
    USER_AGENTS,
    DOWNLOAD_DELAY_MIN,
    DOWNLOAD_DELAY_MAX,
    MAX_RETRIES,
    BACKOFF_BASE_DELAY,
    BACKOFF_MAX_DELAY,
    BACKOFF_FACTOR,
)
from video_scraper.utils import logger


class VideoDownloader:
    def __init__(self):
        self.temp_dir = TEMP_DIR
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def download_with_info(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            info = self._get_video_info(url)
            if not info:
                logger.warning(f"Could not get video info for: {url}")
                return None
            path = self.download_video(url)
            if not path:
                return None
            return {"path": path, "info": info}
        except Exception as e:
            logger.error(f"Error in download_with_info for {url}: {e}")
            return None

    def _get_ydl_options(self, output_path: str) -> Dict[str, Any]:
        return {
            "format": "best[ext=mp4]/best",
            "outtmpl": output_path,
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": False,
            "nocheckcertificate": True,
            "user_agent": random.choice(USER_AGENTS),
            "http_headers": {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
            "retries": MAX_RETRIES,
            "fragment_retries": MAX_RETRIES,
            "extractor_retries": MAX_RETRIES,
            "file_access_retries": MAX_RETRIES,
            "sleep_interval": random.uniform(DOWNLOAD_DELAY_MIN, DOWNLOAD_DELAY_MAX),
            "max_sleep_interval": DOWNLOAD_DELAY_MAX,
        }

    def _get_video_info(self, url: str) -> Optional[Dict[str, Any]]:
        try:
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "user_agent": random.choice(USER_AGENTS),
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                return info
        except Exception as e:
            logger.error(f"Error getting video info for {url}: {e}")
            return None

    def _is_duration_valid(self, duration: Optional[int]) -> bool:
        if duration is None:
            return True
        return duration <= MAX_VIDEO_DURATION_SECONDS

    def download_video(
        self,
        url: str,
        filename: Optional[str] = None,
    ) -> Optional[Path]:
        try:
            logger.info(f"Checking video info for: {url}")
            video_info = self._get_video_info(url)
            
            if not video_info:
                logger.warning(f"Could not get video info for: {url}")
                return None

            duration = video_info.get("duration")
            if not self._is_duration_valid(duration):
                logger.warning(
                    f"Video duration {duration}s exceeds limit {MAX_VIDEO_DURATION_SECONDS}s: {url}"
                )
                return None

            if filename is None:
                video_id = video_info.get("id", "unknown")
                filename = f"{video_id}.mp4"

            output_path = self.temp_dir / filename
            output_template = str(self.temp_dir / "%(id)s.%(ext)s")

            logger.info(f"Downloading video: {url} -> {output_path}")
            
            ydl_opts = self._get_ydl_options(output_template)
            
            # Retry loop with exponential backoff
            attempts = 0
            while attempts <= MAX_RETRIES:
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        ydl.download([url])
                    break # Success
                except Exception as e:
                    attempts += 1
                    is_blocked = "429" in str(e) or "Too Many Requests" in str(e) or "HTTP Error 429" in str(e)
                    
                    if attempts > MAX_RETRIES:
                        logger.error(f"Max retries reached for {url}: {e}")
                        return None

                    if is_blocked:
                        delay = min(BACKOFF_MAX_DELAY, BACKOFF_BASE_DELAY * (BACKOFF_FACTOR ** attempts))
                        logger.warning(f"Blocked by YouTube (HTTP 429). Retrying in {delay}s... (Attempt {attempts}/{MAX_RETRIES})")
                        time.sleep(delay)
                    else:
                        delay = random.uniform(DOWNLOAD_DELAY_MIN, DOWNLOAD_DELAY_MAX)
                        logger.warning(f"Download error: {e}. Retrying in {delay}s... (Attempt {attempts}/{MAX_RETRIES})")
                        time.sleep(delay)

            # Check for downloaded file
            # 1. Check exact filename if provided/constructed
            if output_path.exists() and output_path.stat().st_size > 0:
                final_path = output_path
            else:
                # 2. Check via glob using video ID
                video_id = video_info.get('id')
                candidates = list(self.temp_dir.glob(f"{video_id}.*"))
                valid_candidates = [p for p in candidates if p.stat().st_size > 0]
                
                if valid_candidates:
                    final_path = valid_candidates[0]
                else:
                    logger.error(f"Download completed but file not found or empty: {url}")
                    return None

            logger.info(f"Successfully downloaded: {final_path}")
            
            delay = random.uniform(DOWNLOAD_DELAY_MIN, DOWNLOAD_DELAY_MAX)
            time.sleep(delay)
            
            return final_path

        except Exception as e:
            logger.error(f"Error downloading video {url}: {e}")
            return None

    def download_videos(
        self,
        urls: list[str],
        max_videos: Optional[int] = None,
    ) -> list[Path]:
        downloaded_files = []
        
        for i, url in enumerate(urls):
            if max_videos and len(downloaded_files) >= max_videos:
                logger.info(f"Reached maximum download limit: {max_videos}")
                break
            
            file_path = self.download_video(url)
            if file_path:
                downloaded_files.append(file_path)
        
        logger.info(f"Successfully downloaded {len(downloaded_files)}/{len(urls)} videos")
        return downloaded_files

    def cleanup_temp_files(self):
        try:
            files = list(self.temp_dir.glob("*"))
            for file in files:
                if file.is_file():
                    file.unlink()
            logger.info(f"Cleaned up {len(files)} temporary files")
        except Exception as e:
            logger.error(f"Error cleaning up temp files: {e}")
