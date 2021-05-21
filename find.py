import argparse
import os

from bs4 import BeautifulSoup
import csv

import logging
import pandas as pd
import re
import requests
import concurrent.futures

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
    by_si = book_title.find('by')

    author = book_title[by_si + 2:].strip()

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


def get_author_urls(author_url, covered_set):
    r = requests.get(author_url)
    if r.status_code != 200:
        logging.info(f'Series URL: bad request {r.status_code}')
        return []

    soup = BeautifulSoup(r.text, features="html.parser")

    num_added, num_skipped = 0, 0
    results = soup.find_all(class_="gr-h3 gr-h3--serif gr-h3--noMargin")
    for book in soup.find_all(class_='bookTitle'):
        if book["href"].find("/book/show") == -1:
            continue

        book_name = book.text.strip()
        if book_name in skip_books:
            num_skipped += 1
            continue

        if book_name in books_dict:
            logging.info(f'{book_name} already present')
            num_skipped += 1
            continue

        logging.info(book_name)
        book_url = f'{GR_URL}{book["href"]}'
        logging.info(book_url)

        book_editions_url = find_editions_page(book_url)
        isbns = get_isbns(book_editions_url)
        logging.info(f'#ISBNs {len(isbns)}')

        bookchor_present = [
            isbn
            for isbn in isbns
            if is_bookchor_instock(isbn)
        ]

        logging.info(f'#BookChor: {len(bookchor_present)}')

        shbi_present = is_shbi_instock(book_name)
        bookish_santa = is_bookish_santa_instock(book_name)
        logging.info(f'shbi: {shbi_present} bookish_santa: {bookish_santa}')

        books_dict[book_name] = (book_name, book_url, book_editions_url, isbns, bookchor_present, shbi_present,
                                 bookish_santa)
        num_added += 1
        logging.info(f'Done Book# {num_added}')
    return num_added, num_skipped


def get_series_urls(series_url, covered_set, url_type='series'):
    urls = []
    num_covered = 0

    for page_num in range(1, 100):
        page_url = f'{series_url}?page={page_num}&per_page=30'
        r = requests.get(page_url)
        if r.status_code != 200:
            return urls, num_covered

        soup = BeautifulSoup(r.text, features="html.parser")
        class_str = "gr-h3 gr-h3--serif gr-h3--noMargin"
        if url_type == 'author':
            class_str = 'bookTitle'

        results = soup.find_all(class_=class_str)
        if not results:
            logging.info(f'Stopping at Page#{page_num}')
            return urls, num_covered

        for book in results:
            if book["href"].find("/book/show") == -1:
                continue

            book_url = f'{GR_URL}{book["href"]}'
            if book_url in covered_set:
                num_covered += 1
            else:
                urls.append(book_url)
    return urls, num_covered


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-type', default='url')
    parser.add_argument('-input')
    return parser.parse_args()


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
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        results = executor.map(do_work, urls)

        for url, result in zip(urls, results):
            if result:
                print(f'{result[0]} Done!')
                books[result[0]] = result[1:]
            else:
                print(f'{url} Failed!')
    return books


def write_books_data(books, file_suffix, temp_csv='temp.csv'):
    columns = ['Title', 'URL', 'Author', 'Series', 'BookChor', 'BookChor:ISBNS', 'SHBI', 'BSanta']
    with open(temp_csv, 'w') as fw:
        writer = csv.writer(fw)
        writer.writerow(columns)
        for title in books:
            book = books[title]
            bchor = len(book[-1]['bookchor']) > 0
            row = (title, book[0], book[1], book[2], bchor, ', '.join(book[-1]['bookchor']), book[-1]['shbi'], book[-1]['bookish_santa'])
            writer.writerow(row)

    writer = pd.ExcelWriter(f'report_{file_suffix}.xlsx', engine='xlsxwriter')
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

    if args.type == 'url':
        urls = extract_urls_from_file(args.input)
        logging.info(f'{args.input} #URLs: {len(urls)}')
        books = do_all_work(urls)
        write_books_data(books, args.input.split('/')[1])
        return

    url, covered = get_series_url(args.input)
    urls, num_covered = get_series_urls(url, covered, url_type=args.type)
    logging.info(f'#URLS: {len(urls)} Num_Covered: {num_covered}')
    books = do_all_work(urls)
    write_books_data(books, args.input.split('/')[1])



if __name__ == '__main__':
    main()