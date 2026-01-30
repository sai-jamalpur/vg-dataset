import subprocess
import json
from pathlib import Path
from typing import Optional
from video_scraper.config import PROCESSED_DIR, VIDEO_WIDTH, VIDEO_HEIGHT
from video_scraper.utils import logger


class VideoProcessor:
    def __init__(self):
        self.processed_dir = PROCESSED_DIR
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def _get_output_path(self, input_path: Path) -> Path:
        output_filename = f"{input_path.stem}_processed{input_path.suffix}"
        return self.processed_dir / output_filename

    def process_video(
        self,
        input_path: Path,
        output_path: Optional[Path] = None,
        delete_original: bool = True,
    ) -> Optional[Path]:
        try:
            if not input_path.exists():
                logger.error(f"Input file does not exist: {input_path}")
                return None

            if output_path is None:
                output_path = self._get_output_path(input_path)

            logger.info(f"Processing video: {input_path} -> {output_path}")

            cmd = [
                "ffmpeg",
                "-i", str(input_path),
                "-vf", f"scale={VIDEO_WIDTH}:{VIDEO_HEIGHT}:force_original_aspect_ratio=decrease,pad={VIDEO_WIDTH}:{VIDEO_HEIGHT}:(ow-iw)/2:(oh-ih)/2:black",
                "-c:v", "libx264",
                "-preset", "medium",
                "-crf", "23",
                "-c:a", "aac",
                "-b:a", "128k",
                "-movflags", "+faststart",
                "-y",
                str(output_path),
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=300,
            )

            if result.returncode != 0:
                logger.error(f"FFmpeg error: {result.stderr}")
                return None

            if output_path.exists():
                logger.info(f"Successfully processed video: {output_path}")
                
                if delete_original:
                    try:
                        input_path.unlink()
                        logger.info(f"Deleted original file: {input_path}")
                    except Exception as e:
                        logger.warning(f"Could not delete original file: {e}")
                
                return output_path
            else:
                logger.error(f"Output file not created: {output_path}")
                return None

        except subprocess.TimeoutExpired:
            logger.error(f"Video processing timed out: {input_path}")
            return None
        except Exception as e:
            logger.error(f"Error processing video: {e}")
            return None

    def process_videos(
        self,
        input_paths: list[Path],
        delete_originals: bool = True,
    ) -> list[Path]:
        processed_files = []
        
        for input_path in input_paths:
            output_path = self.process_video(input_path, delete_original=delete_originals)
            if output_path:
                processed_files.append(output_path)
        
        logger.info(f"Successfully processed {len(processed_files)}/{len(input_paths)} videos")
        return processed_files

    def get_video_info(self, video_path: Path) -> Optional[dict]:
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height,duration",
                "-of", "json",
                str(video_path),
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace"
            )
            
            if result.returncode == 0:
                info = json.loads(result.stdout)
                if info.get("streams"):
                    return info["streams"][0]
            return None
        except Exception as e:
            logger.error(f"Error getting video info: {e}")
            return None
