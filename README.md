# VG Video Script Scraper

Automated educational video scraper and processor for harvesting and processing educational content.

## Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Command

Run the scraper with a JSON configuration file:

```bash
python main.py run by_class_range/science.json --max-videos 10
```

### Available Commands

#### `run` - Run the video scraper

```bash
python main.py run <json_file> [options]
```

**Arguments:**
- `json_file` - Path to the JSON file containing topics

**Options:**
- `--max-videos` - Maximum videos per subtopic (default: 10)
- `--resume` - Resume from previous state
- `--mode` - Execution mode:
  - `all` - Search and download videos (default)
  - `harvest` - Search only (no download)
  - `process` - Download only (no search)

**Examples:**

```bash
# Run with default settings
python main.py run by_class_range/science.json

# Run with custom video limit
python main.py run by_class_range/science.json --max-videos 10

# Resume from previous state
python main.py run by_class_range/science.json --resume

# Search only mode (no download)
python main.py run by_class_range/science.json --mode harvest

# Download only mode (no search)
python main.py run by_class_range/science.json --mode process

# Combined options
python main.py run by_class_range/science.json --max-videos 10 --resume --mode all
```

#### `pause` - Pause the scraper

```bash
python main.py pause <json_file>
```

**Example:**
```bash
python main.py pause by_class_range/science.json
```

#### `resume` - Resume the scraper

```bash
python main.py resume <json_file>
```

**Example:**
```bash
python main.py resume by_class_range/science.json
```

#### `status` - Check scraper status

```bash
python main.py status <json_file>
```

**Example:**
```bash
python main.py status by_class_range/science.json
```

#### `summary` - Get JSON file summary

```bash
python main.py summary <json_file>
```

**Example:**
```bash
python main.py summary by_class_range/science.json
```

## Available JSON Files

The project includes pre-configured JSON files for different subjects:

- `by_class_range/science.json` - Science topics
- `by_class_range/math.json` - Mathematics topics
- `by_class_range/english.json` - English topics
- `by_class_range/social_science.json` - Social Science topics

## Requirements

- Python 3.7+
- yt-dlp
- duckduckgo-search
- python-dotenv
- requests
- pydantic
- ffmpeg-python
- httpx

## License

MIT
