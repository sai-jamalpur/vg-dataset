import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from video_scraper.config import STATE_DIR
from video_scraper.utils import logger


class StateManager:
    def __init__(self, state_file: str = "scraper_state.json"):
        self.state_file = STATE_DIR / state_file
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        if self.state_file.exists():
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading state file: {e}")
                return self._create_empty_state()
        return self._create_empty_state()

    def _create_empty_state(self) -> Dict[str, Any]:
        return {
            "is_paused": False,
            "current_task": None,
            "completed_tasks": [],
            "completed_subtopics": [],
            "pending_tasks": [],
            "failed_tasks": [],
            "last_updated": datetime.now().isoformat(),
            "metadata": {},
        }

    def _save_state(self):
        try:
            self.state["last_updated"] = datetime.now().isoformat()
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving state file: {e}")

    def is_paused(self) -> bool:
        return self.state.get("is_paused", False)

    def pause(self):
        self.state["is_paused"] = True
        self._save_state()
        logger.info("Scraper paused")

    def resume(self):
        self.state["is_paused"] = False
        self._save_state()
        logger.info("Scraper resumed")

    def set_current_task(self, task: Dict[str, Any]):
        self.state["current_task"] = task
        self._save_state()

    def get_current_task(self) -> Optional[Dict[str, Any]]:
        return self.state.get("current_task")

    def add_completed_task(self, task: Dict[str, Any]):
        if task not in self.state["completed_tasks"]:
            self.state["completed_tasks"].append(task)
            self._save_state()

    def add_completed_subtopic(self, topic: str, subtopic: str):
        item = {"topic": topic, "subtopic": subtopic}
        if "completed_subtopics" not in self.state:
            self.state["completed_subtopics"] = []
        
        # Check if already exists
        exists = any(
            x["topic"] == topic and x["subtopic"] == subtopic 
            for x in self.state["completed_subtopics"]
        )
        if not exists:
            self.state["completed_subtopics"].append(item)
            self._save_state()

    def is_subtopic_completed(self, topic: str, subtopic: str) -> bool:
        if "completed_subtopics" not in self.state:
            return False
        return any(
            x["topic"] == topic and x["subtopic"] == subtopic 
            for x in self.state["completed_subtopics"]
        )

    def add_pending_task(self, task: Dict[str, Any]):
        if task not in self.state["pending_tasks"]:
            self.state["pending_tasks"].append(task)
            self._save_state()

    def add_failed_task(self, task: Dict[str, Any], error: str):
        task_with_error = {**task, "error": error, "failed_at": datetime.now().isoformat()}
        if task_with_error not in self.state["failed_tasks"]:
            self.state["failed_tasks"].append(task_with_error)
            self._save_state()

    def remove_pending_task(self, task: Dict[str, Any]):
        if task in self.state["pending_tasks"]:
            self.state["pending_tasks"].remove(task)
            self._save_state()

    def get_pending_tasks(self) -> List[Dict[str, Any]]:
        return self.state.get("pending_tasks", [])

    def get_completed_tasks(self) -> List[Dict[str, Any]]:
        return self.state.get("completed_tasks", [])

    def get_failed_tasks(self) -> List[Dict[str, Any]]:
        return self.state.get("failed_tasks", [])

    def set_metadata(self, key: str, value: Any):
        if "metadata" not in self.state:
            self.state["metadata"] = {}
        self.state["metadata"][key] = value
        self._save_state()

    def get_metadata(self, key: str) -> Optional[Any]:
        return self.state.get("metadata", {}).get(key)

    def reset_state(self):
        self.state = self._create_empty_state()
        self._save_state()
        logger.info("State reset")

    def get_progress_summary(self) -> Dict[str, Any]:
        return {
            "is_paused": self.state["is_paused"],
            "current_task": self.state["current_task"],
            "completed_count": len(self.state["completed_tasks"]),
            "pending_count": len(self.state["pending_tasks"]),
            "failed_count": len(self.state["failed_tasks"]),
            "last_updated": self.state["last_updated"],
        }
