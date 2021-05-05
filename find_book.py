import argparse
import logging
import concurrent.futures
from utils import *
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def query_book(book_url):
    book_name, url = book_url
    logging.info(f'{book_name} START')
    book_editions_url = find_editions_page(url)

    isbns = get_isbns(book_editions_url)

    bookchor_isbns = [
        isbn
        for isbn in isbns
        if is_bookchor_instock(isbn)
    ]

    shbi_present = is_shbi_instock(book_name)
    bookish_santa = is_bookish_santa_instock(book_name)

    logging.info(f'{book_name}: #ISBNs {len(isbns)} Bookchor: {len(bookchor_isbns)} SHBI:{shbi_present} BS:{bookish_santa}')
    return book_name, url, book_editions_url, isbns, bookchor_isbns, shbi_present, bookish_santa


def query_all_books(book_urls):
    missed, completed = [], []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(query_book, book_urls)

        for result in results:
            try:
                print(result[0])
                completed.append(result)
            except Exception as e:
                logging.error(e)
                pass

    return completed


def get_books(book_urls):
    books = []
    for book_name, book_url in book_urls:
        logging.info(book_name)
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

        logging.info(f'shbi: {shbi_present}')

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
    parser.add_argument('-urls', default='books.url')
    return parser.parse_args()


def get_info(urls_file):
    book_urls = []
    with open(f'inputs/{urls_file}') as fr:
        for line in fr:
            book_urls.append(line.strip().split(';'))
    return book_urls


def main():
    args = setup_args()
    logging.info(args)

    book_urls = get_info(args.urls)
    logging.info(f'Looking for #{len(book_urls)} Books')

    books = query_all_books(book_urls)
    csv_name = f'reports/report_books.csv'
    report_name = f'reports/report_books.xlsx'

    write_csv(books, csv_name)
    write_excel(csv_name, report_name)

    os.remove(csv_name)


if __name__ == '__main__':
    main()

