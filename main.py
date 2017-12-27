#!/usr/bin/python
# filename: run.py
import logging
import my_logger
import re
from crawler import Crawler, CrawlerCache
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time
from gmail import simple_message
import requests

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    starttime=time.time()
    repeat_time = 3600.0
    email_to = "@gmail.com"

    def crawl():
        website = "www.idealista.com"
        url_no_domain = "/venta-viviendas/madrid/moncloa/aravaca/pagina-{n}.htm"
        url_format = "https://{url}{url_no_dom}"
        url = url_format.format(url = website, url_no_dom = url_no_domain)
        # Using SQLite as a cache to avoid pulling twice
        root_re = re.compile('^/$').match
        inmueble_re = re.compile('^/inmueble/.*')
        #crawler.crawl(url.format(n = "1"), no_cache=root_re, reg=re.compile(".*"))
        n_page = 1
        crawler = Crawler(CrawlerCache('crawler.db'), depth=2) #TODO
        while (n_page <= 100):
            formatted_url = url.format(n = n_page)
            r = requests.head(formatted_url)
            if n_page != 1 and r.status_code != 200:
                break
            crawler = Crawler(CrawlerCache('crawler.db'), depth=2)
            crawler.crawl(formatted_url, no_cache=root_re, reg=inmueble_re)
            #print(crawler.content[website].keys())
            #print(crawler.content[website][url_no_domain.format(n = n_page)].keys())
            n_elems = len(crawler.content[website][url_no_domain.format(n = n_page)].keys())
            logger.info("{n} elems in this page".format(n=n_elems))
            if n_elems == 1:
                break
            else:
                n_page += 1
        logger.info("Crawled {n} pages from {site}".format(n = n_page-1, site = website))
        
        particular_urls = []
        inmuebles_urls = crawler.cache.get_urls(website, 0, 0)
        for url_inmueble in inmuebles_urls:
            soup = BeautifulSoup(crawler.cache.get(website, url_inmueble), 'html.parser')
            publication_type_text = soup.findAll("div", { "class" : "advertiser-data txt-soft" })
            if len(publication_type_text) != 0 and "Particular" in publication_type_text[0].text:
                crawler.cache.mark_as_particular(website, url_inmueble)
                url_particular = url_format.format(url = website, url_no_dom = url_inmueble)
                particular_urls.append(url_particular)

        crawler.cache.verify(website)
        for url_particular in particular_urls:
            logger.info("Particular encontrado: {url_particular}".format(url_particular = url_particular))
        
        if (len(particular_urls) != 0):
            simple_message(email_to, "Pisos de particulares encontrados", "\n".join(particular_urls))

    while True:
        try:
            crawl()
            logger.info("Crawl finalizado.. en espera a un nuevo ciclo")
            time.sleep(repeat_time - ((time.time() - starttime) % repeat_time))
        except:
            logger.exception("Hubo un error realizando el crawl, se reintentara en 10 minutos.")
            time.sleep(600)
