# ORAC Global Data Collector

A robust, modular Python tool for collecting, scraping, and storing global data from multiple sources — web pages, public APIs, news RSS feeds, government portals, and public datasets.

Built with concurrency, persistent storage (SQLite + JSON), and extensible architecture to support wide-ranging data acquisition for intelligence systems.

## Features

- Concurrent web scraping with `requests` + `BeautifulSoup`
- Public API data fetching and JSON storage
- RSS feed parsing for news articles collection
- Persistent storage via SQLite database and JSON backup files
- Modular design for easy extension of data sources
- Logging and error handling for reliable operation
- Summary report generation of collected data

## Getting Started

### Prerequisites

- Python 3.8+
- `requests`
- `beautifulsoup4`
- `pandas`

Install dependencies with:

```bash
pip install -r requirements.txt
