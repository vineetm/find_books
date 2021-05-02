import argparse
import csv
import pandas as pd
import os
import logging
from utils import *
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

GR_URL = "http://goodreads.com"
NEG_RESULT = "0 result(s)"


def get_book_names_and_url(series_url, skip_books):
    r = requests.get(series_url)
    if r.status_code != 200:
        logging.info(f'Series URL: bad request {r.status_code}')
        return []

    soup = BeautifulSoup(r.text, features="html.parser")

    books = []
    for book in soup.find_all(class_="gr-h3 gr-h3--serif gr-h3--noMargin"):
        if book["href"].find("/book/show") == -1:
            continue

        book_name = book.text
        if book_name in skip_books:
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

        books.append((book_name, book_url, book_editions_url, isbns, bookchor_present, shbi_present, bookish_santa))
        logging.info(f'Done Book# {len(books)}')
    return books


def write_csv(books, csv_name):
    with open(csv_name, 'w') as fw:
        writer = csv.writer(fw)
        writer.writerow(['Book Name', 'URL', 'Editions', 'ISBNS', 'Bookchor', 'SHBI', 'Bookish Santa'])
        for book in books:
            datum = []
            datum.append(book[0])
            datum.append(book[1])
            datum.append(book[2])
            isbns = [
                str(num)
                for num in book[3]
            ]

            bc_isbns = [
                str(num)
                for num in book[4]
            ]

            datum.append(','.join(isbns))
            datum.append(','.join(bc_isbns))
            datum.append(book[5])
            datum.append(book[6])
            writer.writerow(datum)


def write_excel(csv_name, report_name):
    writer = pd.ExcelWriter(report_name, engine='xlsxwriter')
    workbook = writer.book
    text_wrap_format = workbook.add_format({'text_wrap': True})

    number_format = workbook.add_format({'num_format': '#'})

    df = pd.read_csv(csv_name)

    sheet_name = 'Books'
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    ws = writer.sheets[sheet_name]

    ws.set_column('A:A', 30)
    ws.set_column('B:C', 40)
    ws.set_column('D:D', 75, text_wrap_format)
    ws.set_column('E:E', 55, number_format)
    ws.set_column('F:F', 20)
    writer.save()


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

    all_books = []
    for page_num in range(1, 100):
        url = f'{series_url}?page={page_num}&per_page=30'
        logging.info(f'inputs/{args.prefix} URL: {url} #Covered Books: {len(covered_books)}')

        books = get_book_names_and_url(url, covered_books)
        if not books:
            logging.info(f'Stopping at Page# {page_num}')
            break
        all_books.extend(books)

    csv_name = f'reports/report_{args.prefix}.csv'
    report_name = f'reports/report_{args.prefix}.xlsx'

    write_csv(all_books, csv_name)
    write_excel(csv_name, report_name)
    os.remove(csv_name)


if __name__ == '__main__':
    main()

