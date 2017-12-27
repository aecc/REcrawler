# -*- coding: utf-8 -*-
# filename: crawler.py

import sqlite3 
import re
import urllib
import logging
import my_logger
#from urllib.request import urlopen
from html.parser import HTMLParser  
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class HREFParser(HTMLParser):  
    hrefs = set()
    """
    Parser that extracts hrefs
    """
    hrefs = set()
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            dict_attrs = dict(attrs)
            if dict_attrs.get('href'):
                self.hrefs.add(dict_attrs['href'])


def get_local_links(html, domain):  
    """
    Read through HTML content and returns a tuple of links
    internal to the given domain
    """
    hrefs = set()
    parser = HREFParser()
    parser.hrefs = set()
    parser.feed(html)
    for href in parser.hrefs:
        u_parse = urlparse(href)
        if href.startswith('/'):
            # purposefully using path, no query, no hash
            hrefs.add(u_parse.path)
        else:
          # only keep the local urls
          if u_parse.netloc == domain:
            hrefs.add(u_parse.path)
    return hrefs


class CrawlerCache(object):  
    """
    Crawler data caching per relative URL and domain.
    """
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS sites
            (domain text, url text PRIMARY KEY, content text, particular boolean, verified boolean, date integer)''')
        self.conn.commit()
        self.cursor = self.conn.cursor()

    def set(self, domain, url, data, particular, verified):
        """
        store the content for a given domain and relative url
        """
        self.cursor.execute("INSERT OR IGNORE INTO sites VALUES (?,?,?,?,?,datetime('now'))",
            (domain, url, data, particular, verified))
        self.conn.commit()

    def get(self, domain, url):
        """
        return the content for a given domain and relative url
        """
        self.cursor.execute("SELECT content FROM sites WHERE domain=? and url=?",
            (domain, url))
        row = self.cursor.fetchone()
        if row:
            return row[0]

    def get_urls(self, domain, particular, verified):
        """
        return all the URLS within a domain
        """
        self.cursor.execute("SELECT url FROM sites WHERE domain=? and particular=? and verified=?", (domain,particular,verified,))
        # could use fetchone and yield but I want to release
        # my cursor after the call. I could have create a new cursor tho.
        # ...Oh well
        return [row[0] for row in self.cursor.fetchall()]

    def verify(self, domain):
        """
        updates verified flag on all rows (submitted)
        """
        self.cursor.execute("UPDATE sites SET verified=1 WHERE domain=?", (domain,))
        self.conn.commit()
    
    def mark_as_particular(self, domain, url):
        """
        updates verified flag on all rows (submitted)
        """
        self.cursor.execute("UPDATE sites SET particular=1 WHERE domain=? AND url=?", (domain,url,))
        self.conn.commit()


class Crawler(object):  
    def __init__(self, cache=None, depth=2):
        """
        depth: how many time it will bounce from page one (optional)
        cache: a basic cache controller (optional)
        """
        self.depth = depth
        self.content = {}
        self.cache = cache

    def crawl(self, url, no_cache=None, reg=re.compile(".*")):
        """
        url: where we start crawling, should be a complete URL like
        'http://www.intel.com/news/'
        no_cache: function returning True if the url should be refreshed
        """
        u_parse = urlparse(url)
        self.domain = u_parse.netloc
        self.content[self.domain] = {}
        self.content[self.domain][u_parse.path] = {}
        self.root_page = u_parse.path
        self.scheme = u_parse.scheme
        self.no_cache = no_cache
        self._crawl([u_parse.path], self.depth, reg)

    def set(self, url, html, verified):
        self.content[self.domain][self.root_page][url] = html
        if self.is_cacheable(url):
            self.cache.set(self.domain, url, html, 0, verified)

    def get(self, url, force):
        page = None
        if self.is_cacheable(url) and not force:
          page = self.cache.get(self.domain, url)
        if page is None:
          page = self.curl(url)
        else:
          logger.info("cached url... [%s] %s" % (self.domain, url))
        return page

    def is_cacheable(self, url):
        return self.cache and self.no_cache \
            and not self.no_cache(url)

    def _crawl(self, urls, max_depth, reg):
        n_urls = set()
        if max_depth:
            for url in urls:
                # do not crawl twice the same page
                if url not in self.content and (len(urls) == 1 or reg.match(url)):
                    force = False
                    html = ""
                    if len(urls) == 1:
                        force = True
                        html = self.get(url, force)
                        self.set(url, html, 1)
                    else:
                        html = self.get(url, force)
                        #print(html)
                        self.set(url, html, 0)
                    n_urls = n_urls.union(get_local_links(html, self.domain))
            self._crawl(n_urls, max_depth-1, reg)

    def curl(self, url):
        """
        return content at url.
        return empty string if response raise an HTTPError (not found, 500...)
        """
        try:
            logger.info("retrieving url... [%s] %s" % (self.domain, url))
            req = urllib.request.Request('%s://%s%s' % (self.scheme, self.domain, url))
            response = urllib.request.urlopen(req)
            return response.read().decode('ascii', 'ignore')
        except urllib.request.HTTPError as e:
            logger.exception("error [%s] %s: %s" % (self.domain, url, e))
            return ''
