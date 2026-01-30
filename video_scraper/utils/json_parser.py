import json
from pathlib import Path
from typing import Dict, List, Any, Iterator
from video_scraper.utils import logger


class JSONParser:
    def __init__(self, json_path: str | Path):
        self.json_path = Path(json_path)
        if not self.json_path.exists():
            raise FileNotFoundError(f"JSON file not found: {self.json_path}")
        
        self.data = self._load_json()
        self.subject = self.json_path.stem

    def _load_json(self) -> Dict[str, Any]:
        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading JSON file: {e}")
            raise

    def get_class_ranges(self) -> List[str]:
        return list(self.data.keys())

    def get_topics_for_class_range(self, class_range: str) -> List[Dict[str, Any]]:
        return self.data.get(class_range, [])

    def get_all_topics(self) -> Iterator[Dict[str, Any]]:
        for class_range in self.get_class_ranges():
            for topic_data in self.get_topics_for_class_range(class_range):
                yield {
                    "class_range": class_range,
                    "subject": self.subject,
                    **topic_data,
                }

    def get_total_topic_count(self) -> int:
        count = 0
        for class_range in self.get_class_ranges():
            count += len(self.get_topics_for_class_range(class_range))
        return count

    def get_total_subtopic_count(self) -> int:
        count = 0
        for topic in self.get_all_topics():
            count += len(topic.get("subtopics", []))
        return count

    def get_summary(self) -> Dict[str, Any]:
        return {
            "subject": self.subject,
            "class_ranges": self.get_class_ranges(),
            "total_topics": self.get_total_topic_count(),
            "total_subtopics": self.get_total_subtopic_count(),
        }
