from pathlib import Path
from typing import Optional, List, Dict, Any
import time
import concurrent.futures
from datetime import datetime, timedelta
from queue import Queue, PriorityQueue
import random
from threading import Thread, Event
from typing import List, Dict, Any, Optional
from video_scraper.database.local_storage import LocalStorageManager
from video_scraper.search import YouTubeSearcher
from video_scraper.processing import VideoDownloader, VideoProcessor
from video_scraper.utils import StateManager, JSONParser, logger
from video_scraper.config import PROCESSED_DIR, MAX_VIDEO_DURATION_SECONDS


class VideoScraperOrchestrator:
    def __init__(self, json_path: str | Path):
        self.json_parser = JSONParser(json_path)
        self.db_manager = LocalStorageManager()
        self.state_manager = StateManager()
        self.searcher = None
        self.downloader = VideoDownloader()
        self.processor = VideoProcessor()
        self.start_time: Optional[float] = None
        self.processed_count: int = 0
        self.total_subtopics: int = 0
        self.searched_subtopics: int = 0
        self.max_videos_param: int = 0
        self.expected_total_videos: Optional[int] = None
        self.is_harvesting: bool = False
        
        self._initialize_searcher()
        self.failed_queue = Queue()
        self.download_queue = PriorityQueue()
        self.stop_event = Event()
        self.retry_worker = Thread(target=self._retry_worker_loop, daemon=True)
        self.retry_worker.start()

    def _initialize_searcher(self):
        existing_urls = self.db_manager.get_existing_urls()
        self.searcher = YouTubeSearcher(existing_urls, db_manager=self.db_manager)
        logger.info(f"Initialized searcher with {len(existing_urls)} existing URLs")

    def _sanitize_filename(self, name: str) -> str:
        invalid = '<>:"/\\|?*'
        for ch in invalid:
            name = name.replace(ch, "_")
        name = name.strip().strip(".")
        if len(name) > 150:
            name = name[:150]
        return name

    def _build_output_dir(self, subject: str, class_range: str, topic: str, subtopic: str) -> Path:
        parts = [
            PROCESSED_DIR,
            self._sanitize_filename(subject),
            self._sanitize_filename(class_range),
            self._sanitize_filename(topic),
            self._sanitize_filename(subtopic),
        ]
        out_dir = Path(parts[0]) / Path(*parts[1:])
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def _log_eta(self):
        try:
            if self.start_time is None:
                return
            
            elapsed = time.time() - self.start_time
            processed = self.processed_count
            queue_size = self.download_queue.qsize()
            
            total_subtopics = self.total_subtopics
            searched_subtopics = self.searched_subtopics
            max_videos = self.max_videos_param
            
            remaining_subtopics = max(0, total_subtopics - searched_subtopics)
            
            # Estimate remaining items based on max_videos per subtopic
            est_future_items = remaining_subtopics * max_videos
            total_est_items = processed + queue_size + est_future_items
            remaining_items = queue_size + est_future_items
            
            # Calculate processing rate
            if processed > 0:
                rate = processed / elapsed
            else:
                rate = 0.0
            
            # Calculate ETA
            if rate > 0:
                eta_seconds = remaining_items / rate
                eta_str = str(timedelta(seconds=int(eta_seconds)))
                finish_time = (datetime.now() + timedelta(seconds=int(eta_seconds))).strftime('%H:%M:%S')
            else:
                eta_str = "Calculating..."
                finish_time = "?"
            
            logger.info(
                f"Progress: {processed} done | {queue_size} queued | "
                f"Topics: {searched_subtopics}/{total_subtopics} | "
                f"Est. Total: ~{total_est_items} (Max {max_videos}/sub) | "
                f"ETA: {eta_str} (Finish ~ {finish_time})"
            )
        except Exception:
            pass

    def _check_pause(self):
        if self.state_manager.is_paused():
            logger.info("Operation paused by user")
            return True
        return False
    
    def _enqueue_failed_query(self, topic: str, subtopic: str, class_range: str, subject: str, max_videos: int):
        item = {
            "topic": topic,
            "subtopic": subtopic,
            "class_range": class_range,
            "subject": subject,
            "max_videos": max_videos,
            "attempts": 0,
        }
        self.failed_queue.put(item)
        logger.info(f"Queued for retry: {topic} - {subtopic}")
    
    def _retry_worker_loop(self):
        while not self.stop_event.is_set():
            try:
                item = self.failed_queue.get(timeout=1)
            except Exception:
                continue
            topic = item.get("topic")
            subtopic = item.get("subtopic")
            class_range = item.get("class_range")
            subject = item.get("subject")
            max_videos = int(item.get("max_videos", 5))
            attempts = int(item.get("attempts", 0))
            delay = min(30, 2 ** attempts)
            time.sleep(delay)
            try:
                query = f"{topic} {subtopic}"
                urls = self.searcher.search_videos(
                    query,
                    max_results=max_videos,
                    topic=topic,
                    subtopic=subtopic,
                )
                if urls:
                    videos_to_insert = []
                    for url in urls:
                        videos_to_insert.append({
                            "video_url": url,
                            "topic": topic,
                            "subtopic": subtopic,
                            "class_range": class_range,
                            "subject": subject,
                            "status": "pending",
                            "local_path": None,
                        })
                    new_count = self.db_manager.batch_insert_videos(videos_to_insert)
                    logger.info(f"Retry stored {new_count} new pending videos for {subtopic}")
                else:
                    item["attempts"] = attempts + 1
                    if item["attempts"] <= 5:
                        self.failed_queue.put(item)
                        logger.info(f"Requeued empty result for {subtopic}, attempt {item['attempts']}")
                    else:
                        logger.warning(f"Dropped after retries: {subtopic}")
            except Exception as e:
                item["attempts"] = attempts + 1
                if item["attempts"] <= 5:
                    self.failed_queue.put(item)
                    logger.warning(f"Retry error for {subtopic}, attempt {item['attempts']}: {e}")
                else:
                    logger.error(f"Dropped after failures: {subtopic}: {e}")
            finally:
                try:
                    self.failed_queue.task_done()
                except Exception:
                    pass

    def _process_single_video(
        self,
        video_url: str,
        topic: str,
        subtopic: str,
        class_range: str,
        subject: str,
    ) -> bool:
        try:
            if self._check_pause():
                return False

            task = {
                "video_url": video_url,
                "topic": topic,
                "subtopic": subtopic,
                "class_range": class_range,
                "subject": subject,
            }
            self.state_manager.set_current_task(task)

            logger.info(f"Processing video: {video_url}")

            dl = self.downloader.download_with_info(video_url)
            if not dl:
                logger.warning(f"Failed to download: {video_url}")
                self.state_manager.add_failed_task(task, "Download failed")
                return False
            downloaded_file = dl["path"]
            info = dl["info"] or {}
            title = info.get("title") or "video"
            channel = info.get("channel") or info.get("uploader")
            duration = info.get("duration")
            upload_date_raw = info.get("upload_date")
            upload_date = None
            if upload_date_raw and isinstance(upload_date_raw, str) and len(upload_date_raw) == 8:
                try:
                    upload_date = f"{upload_date_raw[0:4]}-{upload_date_raw[4:6]}-{upload_date_raw[6:8]}"
                except Exception:
                    upload_date = None

            out_dir = self._build_output_dir(subject, class_range, topic, subtopic)
            safe_title = self._sanitize_filename(title)
            video_id = info.get("id") or video_url.split("v=")[-1]
            output_path = out_dir / f"{safe_title}_{video_id}.mp4"

            processed_file = self.processor.process_video(downloaded_file, output_path=output_path, delete_original=True)
            if not processed_file:
                logger.warning(f"Failed to process: {downloaded_file}")
                self.state_manager.add_failed_task(task, "Processing failed")
                return False

            # Update the existing video record with details and status
            success = self.db_manager.update_video_details(
                video_url=video_url,
                local_path=str(processed_file),
                duration=duration,
                title=title,
                channel=channel,
                upload_date=upload_date,
                status="processed",
                full_info=info
            )

            if success:
                self.state_manager.add_completed_task(task)
                # No need to update existing_urls as it's already there
                logger.info(f"Successfully processed and stored: {video_url}")
                self.processed_count += 1
                self._log_eta()
                return True
            else:
                logger.error(f"Failed to update database record: {video_url}")
                self.state_manager.add_failed_task(task, "Database update failed")
                return False

        except Exception as e:
            logger.error(f"Error processing video {video_url}: {e}")
            task = {
                "video_url": video_url,
                "topic": topic,
                "subtopic": subtopic,
                "class_range": class_range,
                "subject": subject,
            }
            self.state_manager.add_failed_task(task, str(e))
            return False

    def process_topic(
        self,
        topic_data: Dict[str, Any],
        max_videos_per_subtopic: int = 3,
    ) -> Dict[str, int]:
        topic = topic_data["topic"]
        subtopics = topic_data["subtopics"]
        class_range = topic_data["class_range"]
        subject = topic_data["subject"]

        logger.info(f"Processing topic: {topic} ({len(subtopics)} subtopics)")

        stats = {"found": 0, "processed": 0, "failed": 0}
        for subtopic in subtopics:
            if self._check_pause():
                break

            # Check if subtopic is already completed
            if self.state_manager.is_subtopic_completed(topic, subtopic):
                logger.info(f"Skipping completed subtopic: {topic} - {subtopic}")
                continue

            # Check if we have recent search logs for this subtopic (backup for lost local state)
            if self.db_manager.check_search_log_exists(topic, subtopic):
                logger.info(f"Found existing search logs for {topic} - {subtopic}, marking as completed")
                self.state_manager.add_completed_subtopic(topic, subtopic)
                continue

            query = f"{topic} {subtopic}"
            urls = self.searcher.search_videos(
                query, 
                max_results=max_videos_per_subtopic,
                topic=topic,
                subtopic=subtopic
            )
            stats["found"] += len(urls)
            
            subtopic_failed = False
            for video_url in urls:
                if self._check_pause():
                    break
                success = self._process_single_video(
                    video_url=video_url,
                    topic=topic,
                    subtopic=subtopic,
                    class_range=class_range,
                    subject=subject,
                )
                if success:
                    stats["processed"] += 1
                else:
                    stats["failed"] += 1
            
            # Mark subtopic as completed if we didn't pause
            if not self._check_pause():
                self.state_manager.add_completed_subtopic(topic, subtopic)

        return stats

    def _harvest_subtopic(self, topic, subtopic, class_range, subject, max_videos_per_subtopic):
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                # 1. Check if we already have a successful search log for this
                if self.db_manager.check_search_log_exists(topic, subtopic):
                    logger.info(f"Skipping already searched: {topic} - {subtopic}")
                    return

                # 2. Search
                query = f"{topic} {subtopic}"
                logger.info(f"Harvesting: {query}")
                urls = self.searcher.search_videos_for_subtopic(
                    topic=topic,
                    subtopic=subtopic,
                    max_results=max_videos_per_subtopic
                )
                if not urls:
                    raise ValueError("No results")
                
                # 3. Store pending videos
                videos_to_insert = []
                for url in urls:
                    videos_to_insert.append({
                        "video_url": url,
                        "topic": topic,
                        "subtopic": subtopic,
                        "class_range": class_range,
                        "subject": subject,
                        "status": "pending",
                        "local_path": None
                    })
                
                new_count = self.db_manager.batch_insert_videos(videos_to_insert)
                logger.info(f"Stored {new_count} new pending videos for {subtopic}")
                return  # Success
            except Exception as e:
                logger.warning(f"Error harvesting subtopic '{subtopic}' (Attempt {attempt + 1}/{max_retries + 1}): {e}")
                if attempt == max_retries:
                    logger.error(f"Failed to harvest subtopic '{subtopic}' after {max_retries + 1} attempts")
                    self._enqueue_failed_query(topic, subtopic, class_range, subject, max_videos_per_subtopic)
                else:
                    time.sleep(2) # Small backoff

    def harvest_links(self, max_videos_per_subtopic: int = 10):
        """Phase 1: Search and store all links without downloading."""
        logger.info("Starting Phase 1: Harvesting links from DuckDuckGo with 6 workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = []
            for topic_data in self.json_parser.get_all_topics():
                if self._check_pause():
                    break
                
                topic = topic_data["topic"]
                subtopics = topic_data["subtopics"]
                class_range = topic_data["class_range"]
                subject = topic_data["subject"]
                
                for subtopic in subtopics:
                    futures.append(
                        executor.submit(
                            self._harvest_subtopic,
                            topic,
                            subtopic,
                            class_range,
                            subject,
                            max_videos_per_subtopic
                        )
                    )
            
            # Wait for all tasks to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Worker task failed with unhandled exception: {e}")

    def _download_worker(self):
        """
        Worker thread that pulls tasks from the download queue.
        Implements random delays and exponential backoff for retries.
        """
        while not self.stop_event.is_set():
            try:
                # Get item from queue (priority_time, task_data)
                priority_time, task = self.download_queue.get(timeout=2)
                
                # If it's not time yet, put it back and wait
                now = time.time()
                if priority_time > now:
                    self.download_queue.put((priority_time, task))
                    time.sleep(1) # Wait a bit before checking again
                    continue
                
                # Random delay to be less aggressive
                delay = random.uniform(1, 3)
                time.sleep(delay)
                
                if self._check_pause():
                    # If paused, put back with same priority
                    self.download_queue.put((priority_time, task))
                    time.sleep(2)
                    continue

                video_url = task["video_url"]
                # Process the video
                success = self._process_single_video(
                    video_url=video_url,
                    topic=task["topic"],
                    subtopic=task["subtopic"],
                    class_range=task["class_range"],
                    subject=task["subject"]
                )
                
                if not success:
                    # Handle Retry
                    retries = task.get("retries", 0) + 1
                    max_retries = 5
                    
                    if retries <= max_retries:
                        backoff_delay = (2 ** retries) * 5 # 5s, 10s, 20s, 40s, 80s
                        next_time = time.time() + backoff_delay
                        task["retries"] = retries
                        
                        logger.warning(f"Download/Process failed for {video_url}. Retrying in {backoff_delay}s (Attempt {retries}/{max_retries})")
                        self.download_queue.put((next_time, task))
                    else:
                        logger.error(f"Max retries reached for {video_url}. Giving up.")
                        self.db_manager.mark_video_failed(video_url, "Max retries reached")
                
                self.download_queue.task_done()
                
            except Exception:
                # Empty queue or other error
                pass

    def _search_producer(self, max_videos_per_subtopic: int):
        """
        Producer thread that searches for videos serially and populates the download queue.
        """
        logger.info("Starting Search Producer (Serial)...")
        
        for topic_data in self.json_parser.get_all_topics():
            if self._check_pause() or self.stop_event.is_set():
                break
            
            topic = topic_data["topic"]
            subtopics = topic_data["subtopics"]
            class_range = topic_data["class_range"]
            subject = topic_data["subject"]
            
            for subtopic in subtopics:
                self.searched_subtopics += 1
                if self._check_pause() or self.stop_event.is_set():
                    break
                
                # Check if already done
                if self.state_manager.is_subtopic_completed(topic, subtopic):
                    continue
                if self.db_manager.check_search_log_exists(topic, subtopic):
                    continue
                
                # Search
                try:
                    logger.info(f"Harvesting: {topic} - {subtopic}")
                    urls = self.searcher.search_videos_for_subtopic(
                        topic=topic,
                        subtopic=subtopic,
                        max_results=max_videos_per_subtopic
                    )
                    
                    if urls:
                        videos_to_insert = []
                        for url in urls:
                            task = {
                                "video_url": url,
                                "topic": topic,
                                "subtopic": subtopic,
                                "class_range": class_range,
                                "subject": subject,
                                "status": "pending",
                                "local_path": None,
                                "retries": 0
                            }
                            videos_to_insert.append(task)
                            # Add to download queue immediately
                            self.download_queue.put((time.time(), task))
                        
                        self.db_manager.batch_insert_videos(videos_to_insert)
                        logger.info(f"Queued {len(urls)} videos for {subtopic}")
                        
                        # Log success
                        self.db_manager.insert_search_log(topic, subtopic, len(urls))
                    
                    # Be nice, wait between searches
                    time.sleep(random.uniform(2, 5))
                    
                except Exception as e:
                    logger.error(f"Error harvesting {subtopic}: {e}")
                    time.sleep(5)

        logger.info("Search Producer finished.")
        self.is_harvesting = False

    def process_all_topics(
        self,
        max_videos_per_subtopic: int = 3,
        resume: bool = False,
        mode: str = "all"
    ) -> Dict[str, Any]:
        self.start_time = time.time()
        self.max_videos_param = max_videos_per_subtopic
        self.total_subtopics = 0
        for t in self.json_parser.get_all_topics():
            self.total_subtopics += len(t.get("subtopics", []))
        self.searched_subtopics = 0
        
        if resume:
            self.state_manager.resume()
        else:
            self.state_manager.reset_state()

        # Load existing pending videos into queue first
        pending_videos = self.db_manager.get_pending_videos(limit=1000)
        for vid in pending_videos:
            task = vid.copy()
            task["retries"] = 0
            self.download_queue.put((time.time(), task))
        logger.info(f"Loaded {len(pending_videos)} pending videos from DB")

        if mode == "harvest":
            # Just run searcher
            self.is_harvesting = True
            self._search_producer(max_videos_per_subtopic)
            
        elif mode == "process":
            # Just start workers
            self.is_harvesting = False
            workers = []
            for _ in range(3):
                t = Thread(target=self._download_worker, daemon=True)
                t.start()
                workers.append(t)
            
            # Wait until queue is empty
            self.download_queue.join()
            
        elif mode == "all":
            self.is_harvesting = True
            
            # Start Workers
            workers = []
            for _ in range(3):
                t = Thread(target=self._download_worker, daemon=True)
                t.start()
                workers.append(t)
            
            # Start Producer (in main thread or separate)
            # We run producer in main thread to block
            self._search_producer(max_videos_per_subtopic)
            
            # Wait for queue to empty
            logger.info("Waiting for downloads to complete...")
            self.download_queue.join()

        try:
            self.stop_event.set()
        except Exception:
            pass
        
        return {"status": "completed", "mode": mode}

    def _harvest_wrapper(self, max_videos_per_subtopic):
        try:
            self.harvest_links(max_videos_per_subtopic)
        finally:
            self.is_harvesting = False
            logger.info("Harvesting phase finished. Processors will stop when queue is empty.")

    def pause(self):
        self.state_manager.pause()

    def resume(self):
        self.state_manager.resume()

    def get_progress(self) -> Dict[str, Any]:
        return self.state_manager.get_progress_summary()

    def get_summary(self) -> Dict[str, Any]:
        return self.json_parser.get_summary()
