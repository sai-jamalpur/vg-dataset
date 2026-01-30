import argparse
import sys
from pathlib import Path
from video_scraper import VideoScraperOrchestrator
from video_scraper.utils import logger


def main():
    parser = argparse.ArgumentParser(
        description="Automated Educational Video Scraper & Processor"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    run_parser = subparsers.add_parser("run", help="Run the video scraper")
    run_parser.add_argument(
        "json_file",
        type=str,
        help="Path to the JSON file containing topics",
    )
    run_parser.add_argument(
        "--max-videos",
        type=int,
        default=10,
        help="Maximum videos per subtopic (default: 5)",
    )
    run_parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous state",
    )
    run_parser.add_argument(
        "--mode",
        type=str,
        default="all",
        choices=["all", "harvest", "process"],
        help="Execution mode: 'harvest' (search only), 'process' (download only), or 'all' (default)",
    )

    pause_parser = subparsers.add_parser("pause", help="Pause the scraper")
    pause_parser.add_argument(
        "json_file",
        type=str,
        help="Path to the JSON file being processed",
    )

    resume_parser = subparsers.add_parser("resume", help="Resume the scraper")
    resume_parser.add_argument(
        "json_file",
        type=str,
        help="Path to the JSON file being processed",
    )

    status_parser = subparsers.add_parser("status", help="Check scraper status")
    status_parser.add_argument(
        "json_file",
        type=str,
        help="Path to the JSON file being processed",
    )

    summary_parser = subparsers.add_parser("summary", help="Get JSON file summary")
    summary_parser.add_argument(
        "json_file",
        type=str,
        help="Path to the JSON file",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    json_path = Path(args.json_file)
    if not json_path.exists():
        logger.error(f"JSON file not found: {json_path}")
        sys.exit(1)

    try:
        orchestrator = VideoScraperOrchestrator(json_path)

        if args.command == "run":
            logger.info(f"Starting video scraper in '{args.mode}' mode...")
            summary = orchestrator.process_all_topics(
                max_videos_per_subtopic=args.max_videos,
                resume=args.resume,
                mode=args.mode,
            )
            logger.info(f"Processing complete: {summary}")

        elif args.command == "pause":
            orchestrator.pause()
            logger.info("Scraper paused")

        elif args.command == "resume":
            orchestrator.resume()
            logger.info("Scraper resumed")

        elif args.command == "status":
            progress = orchestrator.get_progress()
            logger.info(f"Current progress: {progress}")

        elif args.command == "summary":
            summary = orchestrator.get_summary()
            logger.info(f"JSON file summary: {summary}")

    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
