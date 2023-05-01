import logging
import requests
import re
from bs4 import BeautifulSoup

GR_URL = "http://goodreads.com"
NEG_RESULT = "0 result(s)"


def find_editions_page(url):
    r = requests.get(url)
    if r.status_code != 200:
        logging.info(f'Editions Page bad request {r.status_code}')
        return ''

    soup = BeautifulSoup(r.text, features="html.parser")
    a = soup.find(string="All Editions")
    return f'{GR_URL}{a.parent["href"]}'


def get_isbns(url):
    r = requests.get(url)
    return re.findall(r'ISBN13:\ (\d+)', r.text)


def query_bookish_santa(book_name):
    url = f'https://www.bookishsanta.com/search?q={book_name}'
    r = requests.get(url)
    if not r.status_code == 200:
        return False

    soup = BeautifulSoup(r.text, features="html.parser")
    books = [
        book
        for book in soup.find_all(class_='productitem--title')
        if book.text.strip().lower() == book_name.lower() and book.parent.parent.text.find('Sold out') == -1
    ]
    return len(books) > 0


def query_bookchor(isbn):
    book_url = f'https://www.bookchor.com/search/?query={isbn}&Lzg4bEc1SURqeDVYOEw3cUJGMkRKQT09=cfcd208495d565ef66e7dff9f98764da'
    r = requests.get(book_url)
    soup = BeautifulSoup(r.text, features="html.parser")

    for a in soup.find_all(class_="pi-price"):
        if a.text != 'Out of Stock':
            return True
    return False


def query_shbi(book_name):
    book_url = f'https://www.secondhandbooksindia.com/search?query={book_name}'
    r = requests.get(book_url)
    soup = BeautifulSoup(r.text, features="html.parser")
    for a in soup.find_all(class_="search-results-count"):
        if a.text[:len(NEG_RESULT)] != NEG_RESULT:
            return True
    return False


def query_book(book_name, isbns, skip_shbi=False):
    results = dict()
    if not skip_shbi:
        results['shbi'] = query_shbi(book_name)
    results['bookchor'] = [
        isbn
        for isbn in isbns
        if query_bookchor(isbn)
    ]
    return results
