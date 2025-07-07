# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Commands

### Development Setup
```bash
# Install dependencies
poetry install

# Install development dependencies
poetry install --with dev

# Run main crawler (production mode)
poetry run python src/main.py

# Run main crawler via shell script
./main.sh
```

### Testing
```bash
# Run all async tests
poetry run python tests/test_all_async.py

# Run specific crawler tests
poetry run python tests/test_async_rss.py
poetry run python tests/test_async_api.py
poetry run python tests/test_async_bs4.py
poetry run python tests/test_embeddings.py

# Run embedding tests
poetry run python tests/test_async_embedding.py
```

### Code Quality
```bash
# Format code with ruff
poetry run ruff format .

# Lint code with ruff
poetry run ruff check .

# Auto-fix linting issues
poetry run ruff check --fix .
```

### Logging and Monitoring
```bash
# View logs
./logs.sh

# Check log files
tail -f logs/main_logger.log
tail -f logs/script_output.log
tail -f logs/logs_output.log
```

## Architecture Overview

JobsCrawler is an asynchronous job aggregation system that crawls multiple sources concurrently. The architecture follows a modular, configuration-driven approach:

### Core Components

1. **AsyncCrawlerEngine** (`src/crawler.py`): Central orchestration class that manages the crawling lifecycle
2. **Strategy Modules** (`src/crawlers/`): Specialized crawlers for different data sources
3. **Configuration System** (`src/resources/`): JSON-based configuration for each crawler type
4. **Data Processing** (`src/utils/`): Shared utilities for data cleaning and location tagging
5. **Embeddings** (`src/embeddings/`): ML pipeline for job data embedding and RAG preparation

### Crawler Strategies

- **RSS Crawler** (`async_rss.py`): Processes RSS/XML feeds using feedparser
- **API Crawler** (`async_api.py`): Handles JSON API endpoints with configurable mappings
- **BS4 Crawler** (`async_bs4.py`): Scrapes HTML pages using BeautifulSoup with CSS selectors
- **Selenium Crawler** (`async_sel.py`): Currently inactive, for JavaScript-heavy sites

### Data Flow

1. **Fetch**: Async HTTP requests to configured sources
2. **Extract**: Parse data using strategy-specific methods
3. **Clean**: Normalize and validate extracted data
4. **Enrich**: Add location tags using `WorldLocations.json`
5. **Store**: Save to PostgreSQL with duplicate detection
6. **Embed**: Generate embeddings for RAG applications

### Configuration Structure

Each crawler type has separate JSON configs:
- `{strategy}_main.json`: Production configurations
- `{strategy}_test.json`: Test configurations

Configuration includes:
- Target URLs and pagination settings
- Element extraction paths (CSS selectors, JSON paths, XML tags)
- Link following behavior
- Data mapping rules

## Key Files and Directories

- `src/main.py`: Entry point that runs all crawlers concurrently
- `src/crawler.py`: Core AsyncCrawlerEngine implementation
- `src/models.py`: Data models and configuration classes
- `src/resources/`: JSON configurations for each crawler type
- `src/tasks/`: Task management files for crawler coordination
- `src/utils/`: Shared utilities (logging, location processing, scripts)
- `src/embeddings/`: ML pipeline for job data embedding

## Environment Setup

The project requires a `.env` file with:
- `URL_DB`: PostgreSQL connection string
- Additional environment variables for Discord logging and embedding models

## Database Schema

The system uses PostgreSQL with these key tables:
- `main_jobs`: Production job listings
- `test`: Test job listings
- Vector storage for embeddings (via pgvector)

## Development Patterns

- All crawlers follow the same async interface pattern
- Configuration-driven design allows easy addition of new sources
- Consistent error handling and logging throughout
- Database operations use conflict resolution (`ON CONFLICT DO NOTHING`)
- Location processing uses a standardized world locations dataset

## Testing Strategy

- Each crawler has dedicated test files
- Test mode uses separate JSON configurations and database tables
- Async test runner (`test_all_async.py`) runs all strategies concurrently
- Environment validation ensures tests run in proper isolation