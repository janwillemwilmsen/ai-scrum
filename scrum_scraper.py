import requests
import os
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from pathlib import Path
from datetime import datetime
import subprocess
import argparse

CRAWL4AI_HOST = 'http://localhost:11235'
OUTPUT_DIR = Path(__file__).parent / 'scrum_org_content'
ERROR_LOG_FILE = Path(__file__).parent / 'scrape_errors.log'
SITEMAP_BASE_URL = 'https://www.scrum.org/sitemap.xml'
START_PAGE = 6  # Start from page 6 to continue after first 5 sitemaps
MAX_SITEMAP_PAGES = 10
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
PAGES_PER_SITEMAP = 2000  # Adjust as needed
PAGE_CRAWL_DELAY = 2  # seconds between each page crawl to prevent server overload
BATCH_SIZE = 50  # Process pages in batches
BATCH_DELAY = 30  # seconds between batches to let server recover
RESTART_INTERVAL = 50  # Restart server every N URLs to prevent recursion bug
CONTAINER_NAME = "crawl4ai"  # Default container name, will try alternatives

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Create a persistent session for better connection management
session = requests.Session()
session.headers.update(HEADERS)

def log_error(url, error_message):
    with open(ERROR_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"[{datetime.utcnow().isoformat()}] {url}: {error_message}\n")

def url_to_filename(url):
    parsed = urlparse(url)
    filename = parsed.path.lstrip('/').replace('/', '_').replace('.', '_').replace('?', '_').replace('=', '_').replace('&', '_').lower()
    if not filename or filename == '_':
        filename = 'index'
    if not filename.endswith('.md'):
        filename += '.md'
    return OUTPUT_DIR / filename

def save_content(filename, content, source_url):
    metadata = f"---\nsource_url: {source_url}\ndate_scraped: {datetime.utcnow().isoformat()}\n---\n\n"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(metadata + content)

def fetch_sitemap(url):
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.content

def extract_page_urls(sitemap_xml):
    root = ET.fromstring(sitemap_xml)
    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = [elem.text for elem in root.findall('.//ns:url/ns:loc', ns)]
    return urls

def check_page_exists(url):
    resp = requests.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
    if resp.status_code >= 400:
        raise Exception(f"Page returned status {resp.status_code}")
    return True

def scrape_page_content(url):
    endpoint = f"{CRAWL4AI_HOST}/md"
    payload = {
        "url": url,
        "f": "fit",  # Filter type: fit for main content
        "q": None,   # Query (optional)
        "c": "0"     # Cache bust counter
    }
    resp = session.post(endpoint, json=payload, timeout=60)
    resp.raise_for_status()
    
    # The /md endpoint returns JSON with a markdown field
    data = resp.json()
    if 'markdown' in data and data['markdown']:
        return data['markdown']
    else:
        raise Exception('No markdown content returned from /md endpoint')

def check_server_health():
    """Check if the Crawl4AI server is responsive"""
    try:
        response = session.get(f'{CRAWL4AI_HOST}/health', timeout=10)
        if response.status_code == 200:
            return True
        else:
            print(f"⚠️ Server health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"⚠️ Server health check error: {e}")
        return False

def detect_recursion_error(error_message):
    """Detect if the error is the known recursion bug in Crawl4AI"""
    recursion_indicators = [
        "maximum recursion depth exceeded",
        "recursion",
        "colorama",
        "ansitowin32.py"
    ]
    return any(indicator in str(error_message).lower() for indicator in recursion_indicators)

def wait_for_server_recovery():
    """Wait for server to become responsive again"""
    print("🔄 Waiting for server to recover...")
    max_wait_attempts = 12  # 2 minutes max wait
    wait_time = 10
    
    for attempt in range(max_wait_attempts):
        if check_server_health():
            print("✅ Server is responsive again")
            return True
        print(f"⏳ Server not ready, waiting {wait_time}s... (attempt {attempt + 1}/{max_wait_attempts})")
        time.sleep(wait_time)
    
    print("❌ Server failed to recover after 2 minutes")
    print("💡 The server may need to be restarted manually due to the recursion bug.")
    print("💡 Try: docker restart crawl4ai-server (or restart your Crawl4AI container)")
    return False

def handle_server_crash():
    """Handle server crash due to recursion bug"""
    print("\n🚨 DETECTED CRAWL4AI SERVER CRASH (Recursion Bug)")
    print("This is a known issue in Crawl4AI 0.6.1 after processing many URLs.")
    print("\n📋 To continue scraping, you need to restart the server:")
    print("   1. Stop the current server:")
    print("      docker stop crawl4ai-server")
    print("   2. Start it again:")
    print("      docker start crawl4ai-server")
    print("   3. Or restart in one command:")
    print("      docker restart crawl4ai-server")
    print("\n⏳ Waiting 60 seconds for manual server restart...")
    
    # Wait for manual restart
    for i in range(60, 0, -5):
        print(f"⏱️  {i} seconds remaining... (restart the server now)")
        time.sleep(5)
        if check_server_health():
            print("✅ Server is back online!")
            return True
    
    print("\n❌ Server still not responsive. Please restart manually and run the script again.")
    return False

def find_crawl4ai_container():
    """Find the actual Crawl4AI container name"""
    possible_names = ["crawl4ai", "crawl4ai-server", "crawl4ai_server"]
    
    for name in possible_names:
        try:
            result = subprocess.run(['docker', 'ps', '-a', '--filter', f'name={name}', '--format', '{{.Names}}'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0 and name in result.stdout:
                print(f"🔍 Found Crawl4AI container: {name}")
                return name
        except:
            continue
    
    print("⚠️ Could not find Crawl4AI container. Using default name 'crawl4ai'")
    return "crawl4ai"

def stop_crawl4ai_server(container_name=None):
    """Stop the Crawl4AI Docker container"""
    if container_name is None:
        container_name = find_crawl4ai_container()
    
    try:
        print(f"🛑 Stopping Crawl4AI container '{container_name}'...")
        result = subprocess.run(['docker', 'stop', container_name], 
                              capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"✅ Container '{container_name}' stopped successfully")
            return True
        else:
            print(f"⚠️ Warning stopping container: {result.stderr}")
            return True  # Sometimes returns error but still works
    except subprocess.TimeoutExpired:
        print("❌ Timeout while stopping container")
        return False
    except Exception as e:
        print(f"❌ Error stopping container: {e}")
        return False

def start_crawl4ai_server(container_name=None):
    """Start the Crawl4AI Docker container"""
    if container_name is None:
        container_name = find_crawl4ai_container()
    
    try:
        print(f"▶️ Starting Crawl4AI container '{container_name}'...")
        result = subprocess.run(['docker', 'start', container_name], 
                              capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"✅ Container '{container_name}' started successfully")
            return True
        else:
            print(f"❌ Failed to start container: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print("❌ Timeout while starting container")
        return False
    except Exception as e:
        print(f"❌ Error starting container: {e}")
        return False

def restart_crawl4ai_server(container_name=None):
    """Restart the Crawl4AI server to prevent recursion bug"""
    if container_name is None:
        container_name = find_crawl4ai_container()
    
    print(f"\n🔄 PREVENTIVE SERVER RESTART (every {RESTART_INTERVAL} URLs)")
    print("This prevents the recursion bug from occurring.")
    
    # Stop the server
    if not stop_crawl4ai_server(container_name):
        print("⚠️ Could not stop server, trying to start anyway...")
    
    # Wait a moment for cleanup
    time.sleep(5)
    
    # Start the server
    if not start_crawl4ai_server(container_name):
        print("❌ Failed to start server after restart")
        return False
    
    # Wait for server to be ready
    print("⏳ Waiting for server to be ready...")
    for i in range(60):  # Wait up to 60 seconds
        if check_server_health():
            print(f"✅ Server is ready after {i+1} seconds!")
            return True
        time.sleep(1)
    
    print("❌ Server did not become ready within 60 seconds")
    return False

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Scrape Scrum.org content')
    parser.add_argument('--start-page', type=int, default=START_PAGE, 
                       help=f'Starting sitemap page (default: {START_PAGE})')
    parser.add_argument('--max-pages', type=int, default=MAX_SITEMAP_PAGES,
                       help=f'Maximum sitemap pages to process (default: {MAX_SITEMAP_PAGES})')
    args = parser.parse_args()
    
    start_page = args.start_page
    max_pages = args.max_pages
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(ERROR_LOG_FILE, 'w', encoding='utf-8') as f:
        f.write(f"Scrape errors log - {datetime.utcnow().isoformat()}\n\n")
    
    # Check server health before starting
    if not check_server_health():
        print("❌ Server is not responsive. Please check if Crawl4AI is running.")
        return
    
    # Find the container name once at the start
    container_name = find_crawl4ai_container()
    
    total_processed = 0
    urls_since_restart = 0
    
    print(f"🚀 Starting scrape from page {start_page} to {max_pages}")
    
    for page in range(start_page, max_pages + 1):
        sitemap_url = f"{SITEMAP_BASE_URL}?page={page}"
        print(f"Processing sitemap: {sitemap_url}")
        try:
            sitemap_xml = fetch_sitemap(sitemap_url)
            page_urls = extract_page_urls(sitemap_xml)
            if not page_urls:
                print(f"No pages found in sitemap {sitemap_url}. This may be the last page.")
                break
            print(f"Found {len(page_urls)} pages in sitemap {page}")
            pages_to_process = page_urls[:PAGES_PER_SITEMAP]
            
            # Process pages in batches
            for batch_start in range(0, len(pages_to_process), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(pages_to_process))
                batch = pages_to_process[batch_start:batch_end]
                
                print(f"📦 Processing batch {batch_start//BATCH_SIZE + 1}: pages {batch_start+1}-{batch_end} of {len(pages_to_process)}")
                
                # Check server health before each batch
                if not check_server_health():
                    print("⚠️ Server not responsive, attempting recovery...")
                    if not wait_for_server_recovery():
                        print("❌ Stopping due to server issues")
                        return
                
                for idx, page_url in enumerate(batch, batch_start + 1):
                    # Check if we need to restart the server
                    if urls_since_restart >= RESTART_INTERVAL:
                        print(f"\n⏰ Time for preventive restart! Processed {urls_since_restart} URLs since last restart.")
                        if restart_crawl4ai_server(container_name):
                            urls_since_restart = 0
                            print("🔄 Continuing with scraping after restart...\n")
                        else:
                            print("❌ Failed to restart server. Continuing anyway...")
                    
                    print(f"Processing page {idx}/{len(pages_to_process)}: {page_url}")
                    success = False
                    attempts = 0
                    while not success and attempts < MAX_RETRIES:
                        attempts += 1
                        try:
                            check_page_exists(page_url)
                            content = scrape_page_content(page_url)
                            filename = url_to_filename(page_url)
                            save_content(filename, content, page_url)
                            print(f"Saved content to {filename}")
                            success = True
                            total_processed += 1
                            urls_since_restart += 1
                        except Exception as e:
                            if attempts < MAX_RETRIES:
                                print(f"Attempt {attempts} failed for {page_url}. Retrying in {RETRY_DELAY}s...")
                                time.sleep(RETRY_DELAY)
                            else:
                                print(f"Failed to process {page_url} after {MAX_RETRIES} attempts: {e}")
                                log_error(page_url, str(e))
                                
                                # Check for the known recursion bug
                                if detect_recursion_error(str(e)):
                                    print("🔍 Detected recursion error - restarting server immediately!")
                                    if restart_crawl4ai_server(container_name):
                                        urls_since_restart = 0
                                        print("🔄 Server restarted, continuing from next page...")
                                        break  # Exit retry loop and continue with next page
                                    else:
                                        print("❌ Could not recover from server crash. Stopping.")
                                        return
                                
                                # If we get connection errors, check server health
                                if "connection" in str(e).lower() or "timeout" in str(e).lower():
                                    if not check_server_health():
                                        print("⚠️ Server appears to be having issues, will check before next batch")
                                        break
                        
                        time.sleep(PAGE_CRAWL_DELAY)
                
                # Batch completed - give server time to recover
                if batch_end < len(pages_to_process):  # Not the last batch
                    print(f"✅ Batch completed. Processed {total_processed} pages total ({urls_since_restart} since last restart). Waiting {BATCH_DELAY}s for server recovery...")
                    time.sleep(BATCH_DELAY)
                    
        except Exception as e:
            print(f"Error processing sitemap {sitemap_url}: {e}")
            log_error(sitemap_url, str(e))
            if '404' in str(e):
                break
    
    print(f'🎉 Scraping completed! Processed {total_processed} pages total. Check error log for any issues.')

if __name__ == '__main__':
    main() 