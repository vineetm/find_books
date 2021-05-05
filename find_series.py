import argparse
import csv
import pandas as pd
import os
import logging
from utils import *
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

GR_URL = "http://goodreads.com"
NEG_RESULT = "0 result(s)"


def get_book_names_and_url(series_url, skip_books, books_dict):
    r = requests.get(series_url)
    if r.status_code != 200:
        logging.info(f'Series URL: bad request {r.status_code}')
        return 0, 0

    soup = BeautifulSoup(r.text, features="html.parser")

    num_added, num_skipped = 0, 0
    for book in soup.find_all(class_="gr-h3 gr-h3--serif gr-h3--noMargin"):
        if book["href"].find("/book/show") == -1:
            continue

        book_name = book.text
        if book_name in skip_books or book_name in books_dict:
            num_skipped += 1
            continue

        logging.info(book_name)
        book_url = f'{GR_URL}{book["href"]}'
        logging.info(book_url)

        book_editions_url = find_editions_page(book_url)
        print(book_editions_url)

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

        books_dict[book_name] = (book_name, book_url, book_editions_url, isbns, bookchor_present, shbi_present, bookish_santa)
        num_added += 1

        logging.info(f'Done Book# {len(books_dict)}')
    return num_added, num_skipped


def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-prefix')
    return parser.parse_args()


def get_info(prefix):
    with open(f'inputs/{prefix}.url') as fr:
        url = fr.read().strip()

    covered_books = set()
    with open(f'inputs/{prefix}.covered.txt') as fr:
        for line in fr:
            covered_books.add(line.strip())
    return url, covered_books


def main():
    args = setup_args()
    logging.info(args)

    series_url, covered_books = get_info(args.prefix)
    csv_name = f'reports/report_{args.prefix}.csv'
    report_name = f'reports/report_{args.prefix}.xlsx'
    results_file = f'reports/{args.prefix}.books.pkl'

    books_dict = find_existing_db(results_file)
    logging.info(f'START #Books {len(books_dict)}')

    for page_num in range(1, 100):
        url = f'{series_url}?page={page_num}&per_page=30'
        logging.info(f'Page#{page_num} URL:{url}')

        logging.info(f'inputs/{args.prefix} URL: {url} #Covered Books: {len(covered_books)}')
        num_added, num_skipped = get_book_names_and_url(url, covered_books, books_dict)

        if not num_skipped and not num_added:
            logging.info(f'Stopping at Page: {page_num}')
            break

        write_csv(books_dict, csv_name)
        write_excel(csv_name, report_name)
        logging.info(f'Saved Info for {len(books_dict)}')

        if num_added:
            with open(results_file, 'wb') as fw:
                pkl.dump(books_dict, fw)

    csv_name = f'reports/report_{args.prefix}.csv'
    report_name = f'reports/report_{args.prefix}.xlsx'

    write_csv(books_dict, csv_name)
    write_excel(csv_name, report_name)
    os.remove(csv_name)


if __name__ == '__main__':
    main()

