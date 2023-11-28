import mechanicalsoup as ms
from elasticsearch import Elasticsearch, helpers
import redis
import configparser


config = configparser.ConfigParser()
config.read('example.ini')
# print(config.read('example.ini'))
# ['example.ini']

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password']),
)

print(es.info())


def write_to_elastic(es, url, html):
    es.index(
        index='webpages',
        document={
            'url': url.decode('utf-8'),
            'html': html
        }
    )


def crawl(browser, r, es, url):
    print("Downloading url")
    browser.open(url)

    write_to_elastic(es, url, str(browser.page))

    # Parse for more urls
    print("Parsing for more links")
    a_tags = browser.page.find_all("a")
    # print(a_tags)
    hrefs = [a.get("href") for a in a_tags]
    # print(hrefs)
    # Do wiki specific URL filtering
    wikipedia_domain = "https://en.wikipedia.org"
    links = [wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/")]
    print(links)

    # Put urls in Redis queue
    print("pushing links to redis")
    r.lpush("links", *links)


browser = ms.StatefulBrowser()
r = redis.Redis()
r.flushall()
start_url = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)
while link := r.rpop("links"):
    if "Jesus" in str(link):
        break
    crawl(browser, r, es, link)
