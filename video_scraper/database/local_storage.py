import json
from pathlib import Path
from typing import Optional, List, Dict, Any, Set
from video_scraper.config import STORAGE_DIR, METADATA_DIR
from video_scraper.utils import logger

class LocalStorageManager:
    def __init__(self):
        self.storage_dir = STORAGE_DIR
        self.metadata_dir = METADATA_DIR
        
        # Main files
        self.harvested_file = self.storage_dir / "harvested.jsonl"
        self.processed_file = self.storage_dir / "processed.jsonl"
        self.failed_file = self.storage_dir / "failed.jsonl"
        self.search_logs_file = self.storage_dir / "search_logs.jsonl"

    def _append_jsonl(self, file_path: Path, data: Dict[str, Any]):
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(data) + "\n")
        except Exception as e:
            logger.error(f"Error appending to {file_path}: {e}")

    def video_exists(self, video_url: str) -> bool:
        """
        Check if video exists in harvested or processed lists.
        Ideally we cache this in memory for performance.
        """
        # For efficiency, this should probably rely on a cached set
        # But for "strict writing", we check files. 
        # To avoid reading files every time, we should use a cache.
        return video_url in self.get_existing_urls()

    def get_existing_urls(self) -> Set[str]:
        urls = set()
        for file_path in [self.harvested_file, self.processed_file, self.failed_file]:
            if file_path.exists():
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        for line in f:
                            if not line.strip(): continue
                            try:
                                data = json.loads(line)
                                if "video_url" in data:
                                    urls.add(data["video_url"])
                            except json.JSONDecodeError:
                                pass
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {e}")
        return urls

    def batch_insert_videos(self, videos_data: List[Dict[str, Any]]) -> int:
        count = 0
        existing = self.get_existing_urls()
        
        for vid in videos_data:
            if vid["video_url"] not in existing:
                self._append_jsonl(self.harvested_file, vid)
                existing.add(vid["video_url"])
                count += 1
        return count

    def insert_video(self, video_url: str, **kwargs) -> bool:
        data = {"video_url": video_url, **kwargs}
        self._append_jsonl(self.harvested_file, data)
        return True

    def update_video_details(
        self,
        video_url: str,
        local_path: str,
        duration: Optional[int] = None,
        title: Optional[str] = None,
        channel: Optional[str] = None,
        upload_date: Optional[str] = None,
        status: str = "processed",
        full_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        try:
            # 1. Append to processed.jsonl (log of completions)
            record = {
                "video_url": video_url,
                "local_path": local_path,
                "duration": duration,
                "title": title,
                "channel": channel,
                "upload_date": upload_date,
                "status": status,
                "timestamp": str(datetime.now())
            }
            self._append_jsonl(self.processed_file, record)

            # 2. Save full metadata to individual file
            if full_info:
                # Use video ID if available, else sanitized URL
                video_id = full_info.get("id") or video_url.split("v=")[-1]
                safe_name = "".join(x for x in video_id if x.isalnum() or x in "-_")
                meta_path = self.metadata_dir / f"{safe_name}.json"
                
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(full_info, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Saved local records for: {video_url}")
            return True
        except Exception as e:
            logger.error(f"Error updating video details: {e}")
            return False

    def mark_video_failed(self, video_url: str, error_msg: str) -> bool:
        data = {
            "video_url": video_url,
            "status": "failed",
            "error": error_msg,
            "timestamp": str(datetime.now())
        }
        self._append_jsonl(self.failed_file, data)
        return True

    def insert_search_log(self, topic: str, subtopic: str, result_count: int):
        data = {
            "topic": topic,
            "subtopic": subtopic,
            "result_count": result_count,
            "timestamp": str(datetime.now())
        }
        self._append_jsonl(self.search_logs_file, data)

    def check_search_log_exists(self, topic: str, subtopic: str) -> bool:
        if not self.search_logs_file.exists():
            return False
        try:
            with open(self.search_logs_file, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        if data.get("topic") == topic and data.get("subtopic") == subtopic:
                            return True
                    except:
                        pass
        except:
            pass
        return False

    def get_pending_videos(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get videos that are in harvested but not in processed or failed.
        Note: This implementation scans files every time.
        """
        completed_urls = set()
        
        # Load completed URLs
        for file_path in [self.processed_file, self.failed_file]:
            if file_path.exists():
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        for line in f:
                            if not line.strip(): continue
                            try:
                                data = json.loads(line)
                                if "video_url" in data:
                                    completed_urls.add(data["video_url"])
                            except:
                                pass
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {e}")

        pending = []
        if self.harvested_file.exists():
            try:
                with open(self.harvested_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            data = json.loads(line)
                            url = data.get("video_url")
                            if url and url not in completed_urls:
                                pending.append(data)
                                if len(pending) >= limit:
                                    break
                        except:
                            pass
            except Exception as e:
                logger.error(f"Error reading {self.harvested_file}: {e}")
        
        return pending


from datetime import datetime
