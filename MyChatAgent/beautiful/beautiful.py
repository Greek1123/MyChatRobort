import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import json
import time
import hashlib


class SimpleCrawler:
    def __init__(self, user_agent="MyStudentCrawler/1.0"):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent
        })

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.visited_urls = set()
        self.saved_hashes = set()
        self.user_agent = user_agent

    def can_fetch(self, url):
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
            return rp.can_fetch(self.user_agent, url)
        except Exception:
            # robots 读取失败时，先保守一点
            return False

    def fetch_page(self, url):
        if url in self.visited_urls:
            return None

        if not self.can_fetch(url):
            print(f"robots 不允许抓取: {url}")
            return None

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            self.visited_urls.add(url)
            return response.text
        except requests.RequestException as e:
            print(f"请求失败: {url} -> {e}")
            return None

    def parse_page(self, html, url):
        soup = BeautifulSoup(html, "html.parser")

        title = soup.title.get_text(strip=True) if soup.title else "无标题"

        paragraphs = []
        for tag in soup.find_all("p"):
            text = tag.get_text(" ", strip=True)
            if text:
                paragraphs.append(text)

        content = "\n".join(paragraphs).strip()

        if not content:
            return None

        content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
        if content_hash in self.saved_hashes:
            return None

        self.saved_hashes.add(content_hash)

        return {
            "url": url,
            "title": title,
            "content": content
        }

    def save_jsonl(self, data, filename="data.jsonl"):
        if not data:
            return
        with open(filename, "a", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")

    def crawl_one(self, url, output_file="data.jsonl"):
        html = self.fetch_page(url)
        if not html:
            return

        data = self.parse_page(html, url)
        if data:
            self.save_jsonl(data, output_file)
            print(f"已保存: {url}")


if __name__ == "__main__":
    crawler = SimpleCrawler()
    crawler.crawl_one("http://example.com")
    time.sleep(1)