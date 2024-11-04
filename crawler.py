import time
import heapq
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from urllib.request import urlopen
from urllib.error import HTTPError

# Webpage Class stores all properties of a particular webpage.
class Webpage:

    def __init__(self, url, depth,  priority):
        self.parsed_url = urlparse(url)
        self.url = url
        self.depth = depth
        self.priority = priority
        self.domain = self.parsed_url.netloc
        self.url_scheme = self.parsed_url.scheme
        self.content_type = None
        self.page_size = None
        self.page_status = None

    def __lt__(self, other):
        if self.depth == other.depth:
            return self.priority < other.priority
        return self.depth < other.depth

    def __eq__(self, other):
        return self.depth == other.depth and self.priority == other.priority

# Crawler Class stores all the functions required by the crawler.
class Crawler:

    # Checks if url is of nz domain and checks if the Content-Type is "text/html".
    def validate_url(self, webpage):
        if ".nz" not in webpage.domain:
            return None
        request_url = urlopen(webpage.url, timeout=5.0)  
        if 'text/html' in request_url.headers.get('Content-Type', ''):
            return request_url
        return None

    # Checks if domain of the webpage has robots.txt file and parse it and gets the excluded urls that should be ignored.
    def check_robot_txt(self, webpage):
        excluded_urls = []
        robot_txt_url = f"{webpage.url_scheme}://{webpage.domain}/robots.txt"
        with urlopen(robot_txt_url) as robot_file:
            robots_txt = robot_file.read().decode('utf-8')
            for line in robots_txt.splitlines():
                line = line.strip()
                if line.startswith('Disallow:'):
                    path = line.split(':', 1)[1].strip()
                    if path:
                        if path[0] == "/":
                            continue
                        elif path.endswith('*'):
                            path = path[:-1] 
                        excluded_urls.append(f"{webpage.url_scheme}://{webpage.domain}{path}")
        return excluded_urls

    # Checks if the given url is not in the excluded_urls given by robot.txt.
    def check_if_allowed(self, url, excluded_urls):
        for excluded in excluded_urls:
            if excluded in url:
                return False
        return True

    # Reads the url and gets the webpage parse it and gets the new urls in the webpage, checks for duplicates and adds it to the list of urls.
    def read_webpage(self, webpage):
        request_url = self.validate_url(webpage)
        if not request_url:
            return False
        excluded_urls = self.check_robot_txt(webpage)
        if not self.check_if_allowed(webpage.url, excluded_urls):
            return False
        page_content = request_url.read()
        webpage.page_size = len(page_content)
        webpage.page_status = str(request_url.status)
        soup = BeautifulSoup(page_content, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            # For handling relative path
            if not link['href'].startswith('http://') and not link['href'].startswith('https://'):
                link['href'] = urljoin(f"{webpage.url_scheme}://{webpage.domain}", link['href'])
            if link['href'] in url_seen:
                continue
            url_seen.add(link['href'])
            url = Webpage(link['href'], webpage.depth + 1, 1)
            # Provided high priority for new domains and low priority for already seen domain.
            if url.domain not in domain_seen:
                domain_seen.add(url.domain)
            else:
                url.priority = 2
            heapq.heappush(urls, url)
        return True

    # Stores the webpage data in log file.
    def store_webpage_metadata(self, webpage):
        formatted_time = datetime.now().strftime("%H:%M:%S %m/%d/%Y")
        with open("Web Crawler Log 1.txt", "a") as log_file:
            log_file.write(f"{webpage.url}, {formatted_time}, {webpage.page_size}, {webpage.depth}, {webpage.page_status}\n")

    # Stores the error logs.
    def store_error_logs(self, url, e):
        with open("Error Logs 1.txt", "a") as error_logs:
            error_logs.write(f'{url}, {e}\n')

    # Stores the stats of the log file.
    def store_stats(self, stats, total_time_taken):
        with open("Web Crawler Log 1.txt", "a") as log_file:
            log_file.write(f"\n\n\n--------stats--------\n")
            log_file.write(f"Number of URLs Crawled : {stats['url_count']}\n\n")
            log_file.write(f"Number of 200 status : {stats['status_count']['200']}\n")
            log_file.write(f"Number of 403 status : {stats['status_count']['403']}\n")
            log_file.write(f"Number of 404 status : {stats['status_count']['404']}\n")
            log_file.write(f"Total time in Seconds : {total_time_taken}\n")
            log_file.write(f"Total Size : {stats['total_size']}\n")

start_time = time.time()

SEEDS_LIST = "seeds1.txt"

with open(SEEDS_LIST, "r") as seeds_list:
    seeds_list = seeds_list.read().split('\n')

with open("Web Crawler Log 1.txt", "w") as log_file:
    log_file.write("url, time, page_size, depth, page_status\n")

with open("Error Logs 1.txt", "w") as error_logs:
    error_logs.write("url, error\n")

url_seen = set()
domain_seen = set()
urls = []

STATS = {
    "status_count" : {
        "200" : 0,
        "403" : 0,
        "404" : 0
    },
    "url_count" : 0,
    "total_size" : 0
}

for seed in seeds_list:
    url_seen.add(seed)
    url = Webpage(seed, 0, 1)
    heapq.heappush(urls, url)
    domain_seen.add(url.domain)

crawler = Crawler()

# priority queue is used to maintain the flow of the bfs also maintains the priority of the domain in that level.
while urls and (time.time() - start_time < 18000):
    try:
        url = heapq.heappop(urls)
        if not crawler.read_webpage(url):
            continue
    except HTTPError as e:
        url.page_status = str(e.code)
    except Exception as e:
        crawler.store_error_logs(url.url, e)

    crawler.store_webpage_metadata(url)
    STATS["url_count"] = STATS.get("url_count", 0) + 1
    STATS["status_count"][url.page_status] = STATS["status_count"].get(url.page_status, 0) + 1
    if url.page_size:
        STATS["total_size"] = STATS["total_size"] + url.page_size
    print(url.url)

crawler.store_stats(STATS, time.time() - start_time)
