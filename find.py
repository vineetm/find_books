import argparse
import os
import time

from bs4 import BeautifulSoup
import csv

import logging
import pandas as pd
import re
import requests
import concurrent.futures
from tqdm import tqdm

from utils import query_book, find_editions_page, get_isbns
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


GR_URL = "http://goodreads.com"
'''
Goodreads URL to meta data
'''


def extract_series_name(book_title):
    re_search = re.search(f'\(.*\)', book_title)
    if re_search:
        return re_search.group()[1:-1]
    return ''


def get_title_and_author(book_title):
    search_key = ' by '
    by_si = book_title.find(search_key)
    author = book_title[by_si + len(search_key):].strip()

    series_si = book_title.find('(')
    if series_si < 0:
        end = by_si
    else:
        end = series_si

    return book_title[:end].strip(), author


def get_book_meta_data(book_url):
    r = requests.get(book_url)
    if r.status_code != 200:
        logging.error(f'Bad request {book_url} {r.status_code}')
        return None

    soup = BeautifulSoup(r.text, features="html.parser")

    title = soup.find_all('title')[0].text.strip()

    series = extract_series_name(title)
    book_title, author = get_title_and_author(title)

    return book_title, series, author


def get_series_url(input_fname):
    covered_set = set()

    with open(input_fname) as fr:
        url = fr.readline().strip()
        for line in fr:
            line = line.strip()
            covered_set.add(line)
    return url, covered_set


def get_series_urls(series_url, url_type='series'):
    urls = []

    for page_num in range(1, 100):
        page_url = f'{series_url}?page={page_num}&per_page=30'
        r = requests.get(page_url)
        if r.status_code != 200:
            return urls

        soup = BeautifulSoup(r.text, features="html.parser")
        class_str = "gr-h3 gr-h3--serif gr-h3--noMargin"
        if url_type == 'author':
            class_str = 'bookTitle'

        results = soup.find_all(class_=class_str)
        if not results:
            logging.info(f'Stopping at Page#{page_num}')
            return urls

        for book in results:
            if book["href"].find("/book/show") == -1:
                continue

            book_url = f'{GR_URL}{book["href"]}'
            urls.append(book_url)
    return urls


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-out_dir', default='reports')
    parser.add_argument('input')
    parser.add_argument('-max_retries', default=2, type=int)
    return parser.parse_args()


def extract_type(url):
    for t in ['series', 'author']:
        if url.find(f'/{t}/') >= 0:
            return t
    return 'book'


COVERED = 'COVERED'
def parse_input(input_fname):
    num_urls = 0
    with open(input_fname) as fr:
        url = fr.readline().strip()
        book_urls = set()

        while url != COVERED:
            num_urls += 1
            url_type = extract_type(url)
            if url_type == 'book':
                logging.info(f'{url} {url_type} #Books: 1')
                book_urls.add(url)
                url = fr.readline().strip()
                continue
            new_urls = get_series_urls(url, url_type)
            logging.info(f'{url} {url_type} #Books: {len(new_urls)}')
            book_urls.update(new_urls)
            url = fr.readline().strip()

        logging.info(f'#URLS: {num_urls} #Books: {len(book_urls)}')

        covered_set = set()
        for line in fr:
            line = line.strip()
            covered_set.add(line)

        search_urls = {
            book_url
            for book_url in book_urls
            if book_url not in covered_set
        }
        logging.info(f'#Books-Search: {len(search_urls)} #Books: {len(book_urls)}')
        return search_urls


def extract_urls_from_file(fname):
    urls = set()
    with open(fname) as fr:
        for line in fr:
            urls.add(line.strip())
    return urls


def do_work(url):
    try:
        title, series, author = get_book_meta_data(url)
        editions_url = find_editions_page(url)
        query = query_book(title, get_isbns(editions_url))
        return title, url, author, series, editions_url, query
    except Exception as e:
        return None


def do_all_work(urls):
    books = {}
    failed = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        results = executor.map(do_work, urls)

        with tqdm(total=len(urls)) as pbar:

            for url, result in zip(urls, results):
                if result:
                    books[result[0]] = result[1:]
                else:
                    failed.append(url)
                pbar.update(1)
    return books, failed


def write_books_data(books, file_suffix, out_dir, temp_csv='temp.csv'):
    columns = ['Title', 'URL', 'Author', 'Series', 'BookChor', 'BookChor:ISBNS', 'SHBI']
    with open(temp_csv, 'w') as fw:
        writer = csv.writer(fw)
        writer.writerow(columns)
        for title in books:
            book = books[title]
            bchor = len(book[-1]['bookchor']) > 0
            row = (title, book[0], book[1], book[2], bchor, ', '.join(book[-1]['bookchor']), book[-1]['shbi'])
            writer.writerow(row)

    report_path = os.path.join(out_dir, f'report_{file_suffix}.xlsx')
    writer = pd.ExcelWriter(report_path, engine='xlsxwriter')
    workbook = writer.book
    text_wrap_format = workbook.add_format({'text_wrap': True})
    number_format = workbook.add_format({'num_format': '#'})

    df = pd.read_csv(temp_csv)
    sheet_name = 'Books'
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    ws = writer.sheets[sheet_name]

    ws.set_column('A:A', 30)
    ws.set_column('B:B', 40)
    ws.set_column('C:D', 30)
    ws.set_column('E:E', 20)
    ws.set_column('F:F', 75, number_format)
    ws.set_column('G:H', 20)
    writer.save()

    os.remove(temp_csv)


def main():
    args = setup_args()
    logging.info(args)

    book_urls = parse_input(args.input)
    books, failed = do_all_work(book_urls)
    logging.info(f'#Books to write: {len(books)} #Failed: {len(failed)}')

    num_retries = 0
    while failed and num_retries < args.max_retries:
        time.sleep(10)
        logging.info(f'Retry# {num_retries} #Failed: {len(failed)} #Books: {len(books)}')
        logging.info(f'Retrying for #{len(failed)} Books')
        new_books, failed = do_all_work(failed)
        if new_books:
            for book in new_books:
                books[book] = new_books[book]
            logging.info(f'Final Writing {len(books)}')
        num_retries += 1
        
    write_books_data(books, args.input.split('/')[1], args.out_dir)


if __name__ == '__main__':
    main()