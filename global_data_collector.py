import requests
from bs4 import BeautifulSoup
import json
import csv
import sqlite3
import os
from datetime import datetime
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GlobalDataCollector:
    def __init__(self):
        self.data_storage = "global_data_collection"
        os.makedirs(self.data_storage, exist_ok=True)
        
        # Initialize database
        self.db_path = os.path.join(self.data_storage, "global_data.db")
        self.init_database()
        
        # User agent for web requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
    def init_database(self):
        """Initialize SQLite database for structured data storage"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables for different data types
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS web_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            title TEXT,
            content TEXT,
            timestamp DATETIME,
            source TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS api_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT,
            data_type TEXT,
            data_json TEXT,
            timestamp DATETIME
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS news_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            content TEXT,
            source TEXT,
            published_date TEXT,
            url TEXT,
            authors TEXT,
            timestamp DATETIME
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def save_to_database(self, table_name, data_dict):
        """Save structured data to SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        columns = ', '.join(data_dict.keys())
        placeholders = ', '.join(['?'] * len(data_dict))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        try:
            cursor.execute(query, tuple(data_dict.values()))
            conn.commit()
            logger.info(f"Saved data to {table_name} table")
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
        finally:
            conn.close()
    
    def scrape_website(self, url):
        """Scrape data from a given website"""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.title.string if soup.title else "No title"
            
            # Remove scripts and styles
            for script in soup(["script", "style"]):
                script.decompose()
                
            text = soup.get_text(separator=' ', strip=True)
            
            data = {
                'url': url,
                'title': title,
                'content': text,
                'timestamp': datetime.now().isoformat(),
                'source': 'web_scraping'
            }
            
            self.save_to_database('web_data', data)
            
            # Also save to JSON file
            filename = f"webpage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(os.path.join(self.data_storage, filename), 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Successfully scraped {url}")
            return data
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None
    
    def fetch_from_api(self, endpoint, params=None):
        """Fetch data from a public API"""
        try:
            response = requests.get(endpoint, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Save to database
            api_data = {
                'endpoint': endpoint,
                'data_type': 'json',
                'data_json': json.dumps(data),
                'timestamp': datetime.now().isoformat()
            }
            
            self.save_to_database('api_data', api_data)
            
            # Save to JSON file
            filename = f"api_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(os.path.join(self.data_storage, filename), 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Successfully fetched data from {endpoint}")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching from API {endpoint}: {e}")
            return None
    
    def collect_news_data(self):
        """Collect news data from various sources"""
        news_sources = [
            # News API endpoints (would require actual API keys)
            # 'https://newsapi.org/v2/top-headlines?country=us&apiKey=YOUR_API_KEY',
            # 'https://newsapi.org/v2/everything?q=world&apiKey=YOUR_API_KEY',
            
            # Public news RSS feeds
            'http://feeds.bbci.co.uk/news/world/rss.xml',
            'https://www.aljazeera.com/xml/rss/all.xml',
            'http://rss.cnn.com/rss/cnn_world.rss'
        ]
        
        for source in news_sources:
            try:
                if 'rss' in source:
                    self.parse_rss_feed(source)
                else:
                    self.fetch_from_api(source)
            except Exception as e:
                logger.error(f"Error collecting news from {source}: {e}")
    
    def parse_rss_feed(self, feed_url):
        """Parse RSS feed data"""
        try:
            response = requests.get(feed_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'xml')
            items = soup.find_all('item')
            
            for item in items:
                title = item.title.text if item.title else "No title"
                description = item.description.text if item.description else ""
                pub_date = item.pubDate.text if item.pubDate else ""
                link = item.link.text if item.link else ""
                
                news_data = {
                    'title': title,
                    'content': description,
                    'source': feed_url,
                    'published_date': pub_date,
                    'url': link,
                    'authors': '',
                    'timestamp': datetime.now().isoformat()
                }
                
                self.save_to_database('news_articles', news_data)
            
            logger.info(f"Successfully parsed RSS feed from {feed_url}")
            
        except Exception as e:
            logger.error(f"Error parsing RSS feed {feed_url}: {e}")
    
    def collect_public_datasets(self):
        """Download public datasets from various sources"""
        public_datasets = [
            # Example public dataset URLs
            'https://data.worldbank.org/indicator/NY.GDP.MKTP.CD',
            'https://catalog.data.gov/dataset',
            'https://www.kaggle.com/datasets',
            'https://registry.opendata.aws'
        ]
        
        for dataset_url in public_datasets:
            try:
                self.scrape_website(dataset_url)
                logger.info(f"Collected dataset info from {dataset_url}")
            except Exception as e:
                logger.error(f"Error collecting dataset from {dataset_url}: {e}")
    
    def collect_social_media_data(self):
        """Collect public social media data (would require API keys)"""
        # Note: Most social media platforms require API keys and have rate limits
        platforms = [
            # Twitter, Reddit, etc. would go here with proper authentication
        ]
        
        for platform in platforms:
            logger.warning(f"Social media collection for {platform} would require API authentication")
    
    def collect_government_data(self):
        """Collect open government data"""
        government_sources = [
            'https://www.data.gov',
            'https://www.gov.uk/government/statistics',
            'https://data.europa.eu',
            'https://data.gov.au'
        ]
        
        for source in government_sources:
            try:
                self.scrape_website(source)
                logger.info(f"Collected government data from {source}")
            except Exception as e:
                logger.error(f"Error collecting government data from {source}: {e}")
    
    def collect_all_data(self, max_workers=5):
        """Main method to coordinate all data collection"""
        logger.info("Starting global data collection process")
        start_time = time.time()
        
        # Use threading for concurrent data collection
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Web scraping tasks
            websites_to_scrape = [
                'https://en.wikipedia.org/wiki/Main_Page',
                'https://www.worldometers.info/',
                'https://www.cia.gov/the-world-factbook/',
                'https://www.who.int/data/gho'
            ]
            executor.map(self.scrape_website, websites_to_scrape)
            
            # API data collection tasks
            public_apis = [
                'https://api.publicapis.org/entries',  # List of public APIs
                'https://restcountries.com/v3.1/all',  # Country data
                'https://api.spacexdata.com/v4/launches/latest'  # Space data
            ]
            executor.map(self.fetch_from_api, public_apis)
            
            # Other collection methods
            executor.submit(self.collect_news_data)
            executor.submit(self.collect_public_datasets)
            executor.submit(self.collect_government_data)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Completed data collection in {elapsed_time:.2f} seconds")
        
        # Generate a summary report
        self.generate_summary_report()
    
    def generate_summary_report(self):
        """Generate a summary report of collected data"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'data_sources': {
                'web_scraping': self.count_table_records('web_data'),
                'api_data': self.count_table_records('api_data'),
                'news_articles': self.count_table_records('news_articles')
            },
            'storage_size': self.get_storage_size(),
            'file_count': len(os.listdir(self.data_storage)) - 1  # exclude the DB file
        }
        
        report_path = os.path.join(self.data_storage, 'collection_report.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Generated summary report at {report_path}")
        return report
    
    def count_table_records(self, table_name):
        """Count records in a database table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def get_storage_size(self):
        """Get total size of collected data in MB"""
        total_size = 0
        for dirpath, _, filenames in os.walk(self.data_storage):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return f"{(total_size / (1024 * 1024)):.2f} MB"


if __name__ == "__main__":
    collector = GlobalDataCollector()
    
    # Start collecting data (with limited scope for demonstration)
    try:
        collector.collect_all_data(max_workers=3)
    except KeyboardInterrupt:
        logger.warning("Data collection interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error during data collection: {e}")
    
    logger.info("Data collection process completed")
