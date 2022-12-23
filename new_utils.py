from requests_html import HTMLSession
import logging
import re
import time

def find_editions_link(url, max_retries=2, sleep=10):
    
    retry_num = 0
    while retry_num < max_retries:
        session = HTMLSession()
        r = session.get(url)
        elements = r.html.find('a.actionLinkLite')
        if elements:
            break
        retry_num += 1
        logging.info(f'Retry#{retry_num}: {url}')
        time.sleep(sleep)

    for element in elements:
        for abs_link in element.absolute_links:
            if abs_link.find('editions') > 0:
                logging.info(f'Retry#{retry_num}: {url} Found link {abs_link}')
                return abs_link

    logging.info(f'Retry#{retry_num}: {url} did not find link')
    return None

def find_isbns(url):
    session = HTMLSession()
    r = session.get(url)
    for edition  in r.html.find('div.editionData', containing='ISBN'):
        return re.findall(r'\d{13}', r.text)
