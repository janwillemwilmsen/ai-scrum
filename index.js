const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');
const xml2js = require('xml2js');
const { URL } = require('url');

// Crawl4AI API endpoint
const CRAWL4AI_HOST = 'http://localhost:11235';
const OUTPUT_DIR = path.join(__dirname, 'scrum_org_content');
const ERROR_LOG_FILE = path.join(__dirname, 'scrape_errors.log');

// Scrum.org sitemap configuration
const SITEMAP_BASE_URL = 'https://www.scrum.org/sitemap.xml';
const MAX_SITEMAP_PAGES = 10; // Adjust based on actual number of sitemap pages
const MAX_RETRIES = 3;        // Number of retries for failed requests
const RETRY_DELAY = 2000;     // Delay between retries in ms

async function main() {
  try {
    console.log('Starting Scrum.org scraper...');
    
    // Create output directory if it doesn't exist
    await fs.mkdir(OUTPUT_DIR, { recursive: true });
    
    // Initialize error log
    await fs.writeFile(ERROR_LOG_FILE, `Scrape errors log - ${new Date().toISOString()}\n\n`);
    
    // Process each numbered sitemap
    for (let page = 1; page <= MAX_SITEMAP_PAGES; page++) {
      const sitemapUrl = `${SITEMAP_BASE_URL}?page=${page}`;
      console.log(`Processing sitemap: ${sitemapUrl}`);
      
      try {
        // Fetch sitemap
        const sitemap = await fetchSitemap(sitemapUrl);
        
        // Extract page URLs from sitemap
        const pageUrls = extractPageUrls(sitemap);
        
        if (pageUrls.length === 0) {
          console.log(`No pages found in sitemap ${sitemapUrl}. This may be the last page.`);
          break; // Exit the loop if no pages are found
        }
        
        console.log(`Found ${pageUrls.length} pages in sitemap ${page}`);
        
        // Process pages (adjust the slice as needed)
        const pagesToProcess = pageUrls.slice(0, 5);
        
        for (const [pageIndex, pageUrl] of pagesToProcess.entries()) {
          console.log(`Processing page ${pageIndex + 1}/${pagesToProcess.length}: ${pageUrl}`);
          
          let success = false;
          let attempts = 0;
          
          while (!success && attempts < MAX_RETRIES) {
            attempts++;
            try {
              // First, check if the page is accessible directly
              await checkPageExists(pageUrl);
              
              // Try different methods to scrape the content
              let content;
              
              // Method 1: Try regular Fit Markdown approach
              try {
                content = await scrapePageContent(pageUrl, 'fit');
              } catch (error) {
                console.log(`Standard scraping failed for ${pageUrl}, trying fallback method...`);
                
                // Method 2: Use full page capture as fallback
                content = await scrapePageContent(pageUrl, 'fullpage');
              }
              
              if (content) {
                // Save content to file
                const filename = urlToFilename(pageUrl);
                await saveContent(filename, content, pageUrl);
                console.log(`Saved content to ${filename}`);
                success = true;
              } else {
                throw new Error('Empty content returned');
              }
            } catch (error) {
              if (attempts < MAX_RETRIES) {
                console.log(`Attempt ${attempts} failed for ${pageUrl}. Retrying in ${RETRY_DELAY/1000}s...`);
                await delay(RETRY_DELAY);
              } else {
                console.error(`Failed to process ${pageUrl} after ${MAX_RETRIES} attempts: ${error.message}`);
                await logError(pageUrl, error.message);
              }
            }
          }
          
          // Add a small delay between pages
          await delay(1000);
        }
      } catch (error) {
        console.error(`Error processing sitemap ${sitemapUrl}:`, error.message);
        await logError(sitemapUrl, error.message);
        // If we get an error on a sitemap page, we might have reached the end
        if (error.message.includes('404')) {
          break;
        }
      }
    }
    
    console.log('Scraping completed! Check error log for any issues.');
  } catch (error) {
    console.error('Error in main process:', error);
    await logError('MAIN_PROCESS', error.message);
  }
}

async function checkPageExists(url) {
  try {
    // Try a HEAD request first to check if the page exists
    const response = await axios.head(url, {
      timeout: 100000,
      validateStatus: status => true, // Accept any status code to prevent throwing
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      }
    });
    
    if (response.status >= 400) {
      throw new Error(`Page returned status ${response.status}`);
    }
    
    return true;
  } catch (error) {
    throw new Error(`Failed to access ${url}: ${error.message}`);
  }
}

async function fetchSitemap(url) {
  try {
    const response = await axios.get(url, {
      timeout: 15000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      }
    });
    const parser = new xml2js.Parser({ explicitArray: false });
    return await parser.parseStringPromise(response.data);
  } catch (error) {
    throw new Error(`Failed to fetch sitemap ${url}: ${error.message}`);
  }
}

function extractPageUrls(sitemap) {
  try {
    if (!sitemap.urlset || !sitemap.urlset.url) {
      return [];
    }
    
    const urls = Array.isArray(sitemap.urlset.url) 
      ? sitemap.urlset.url 
      : [sitemap.urlset.url];
      
    return urls.map(item => item.loc);
  } catch (error) {
    console.error('Error extracting page URLs:', error);
    return [];
  }
}

async function scrapePageContent(url, method = 'fit') {
  try {
    let endpoint, payload;
    
    if (method === 'fit') {
      // Method 1: Use Fit Markdown
      endpoint = `${CRAWL4AI_HOST}/api/fit`;
      payload = {
        url: url,
        outputFormat: 'markdown',
        includeLinks: true,
        extractMainContent: true,
        enableScripts: true,  // Allow scripts to execute
        followRedirects: true,
        waitForSelector: 'main, article, .content, .main-content', // Wait for main content
        timeout: 30000 // Increase timeout
      };
    } else {
      // Method 2: Use full page capture
      endpoint = `${CRAWL4AI_HOST}/api/extract`;
      payload = {
        url: url,
        outputFormat: 'markdown',
        includeLinks: true,
        removeSelectors: ['header', 'footer', 'nav', '.navigation', '.menu', '.sidebar', '.ads'],
        followRedirects: true,
        enableScripts: true,
        timeout: 30000
      };
    }
    
    const response = await axios.post(endpoint, payload, {
      timeout: 60000 // 60-second timeout for the API call
    });
    
    if ((method === 'fit' && response.data && response.data.markdown) ||
        (method === 'fullpage' && response.data && response.data.content)) {
      return method === 'fit' ? response.data.markdown : response.data.content;
    } else {
      throw new Error('Invalid response from Crawl4AI');
    }
  } catch (error) {
    throw new Error(`Failed to scrape ${url} using ${method}: ${error.message}`);
  }
}

function urlToFilename(url) {
  try {
    const parsedUrl = new URL(url);
    // Remove protocol and domain, replace special chars with underscores
    let filename = parsedUrl.pathname
      .replace(/^\//, '')
      .replace(/[\/\.?=&]/g, '_')
      .toLowerCase();
    
    // Handle empty filenames and trailing slashes
    if (!filename || filename === '' || filename === '_') {
      filename = 'index';
    }
    
    // Add .md extension
    if (!filename.endsWith('.md')) {
      filename += '.md';
    }
    
    return path.join(OUTPUT_DIR, filename);
  } catch (error) {
    // Fallback to a sanitized version of the full URL
    const sanitized = url.replace(/[^a-z0-9]/gi, '_').toLowerCase();
    return path.join(OUTPUT_DIR, `${sanitized}.md`);
  }
}

async function saveContent(filename, content, sourceUrl) {
  // Add metadata at the top of the file
  const metadataContent = `---
source_url: ${sourceUrl}
date_scraped: ${new Date().toISOString()}
---

${content}
`;

  await fs.writeFile(filename, metadataContent);
}

async function logError(url, errorMessage) {
  const logEntry = `[${new Date().toISOString()}] ${url}: ${errorMessage}\n`;
  await fs.appendFile(ERROR_LOG_FILE, logEntry);
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Run the script
main();