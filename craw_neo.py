import mechanicalsoup as ms
from elasticsearch import Elasticsearch, helpers
import redis
import configparser
from neo4j import GraphDatabase


class Neo4jConnector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def add_links(self, page, links):
        with self.driver.session() as session:
            session.execute_write(self._create_links, page, links)

    def flush_db(self):
        print("clearing graph db")
        with self.driver.session() as session:
            session.execute_write(self._flush_db)

    @staticmethod
    def _create_links(tx, page, links):
        page = page.decode('utf-8')
        tx.run("CREATE (:Page { url: $page })", page=page)
        for link in links:
            tx.run(
                "MATCH (p:Page) WHERE p.url = $page "
                "CREATE (:Page { url: $link }) -[:LINKS_TO]-> (p)", link=link, page=page)

    @staticmethod
    def _flush_db(tx):
        tx.run("MATCH (a) -[r]-> () DELETE a, r")
        tx.run("MATCH (a) DELETE a")


def write_to_elastic(es, url, html):
    es.index(index='webpages', document={'url': url.decode('utf-8'), 'html': html})


def crawl(browser, r, es, neo4j_connector, url):
    print("Downloading url")
    browser.open(url)

    # write_to_elastic(es, url, str(browser.page))

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

    neo4j_connector.add_links(url, links)


neo4j_connector = Neo4jConnector("bolt://127.0.0.1:7687", "neo4j", "00000000")
config = configparser.ConfigParser()
config.read('example.ini')
# print(config.read('example.ini'))
es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password']),
)
# print(es.info())

# Initialize Redis
r = redis.Redis()
r.flushall()

# Initialize MechanicalSoup headless browser
browser = ms.StatefulBrowser()
start_url = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)

# Start crawling
while link := r.rpop("links"):
    crawl(browser, r, es, neo4j_connector, link)

neo4j_connector.close()
