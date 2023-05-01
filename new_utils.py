import requests
import re


def find_editions_link(url):
    r = requests.get(url)
    si = r.text.find('/work/editions/')
    if si == -1:
        return None
    span = r.text[si-100:si+100]
    m = re.search(r'(http.*)\"\}', span)
    if not m:
        return None
    return m.group(1)

def find_isbns(url):
    session = HTMLSession()
    r = session.get(url)
    for edition  in r.html.find('div.editionData', containing='ISBN'):
        return re.findall(r'\d{13}', r.text)
