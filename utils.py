import csv
import logging
import os
import pandas as pd
import pickle as pkl
import requests
import re
from bs4 import BeautifulSoup

GR_URL = "http://goodreads.com"
NEG_RESULT = "0 result(s)"


def write_csv(books, csv_name):
    with open(csv_name, 'w') as fw:
        writer = csv.writer(fw)
        writer.writerow(['Book Name', 'URL', 'Editions', 'ISBNS', 'Bookchor', 'SHBI', 'Bookish Santa'])
        for book_name in books:
            book = books[book_name]
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


def is_bookish_santa_instock(book_name):
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


def is_bookchor_instock(isbn):
    book_url = f'https://www.bookchor.com/search/?query={isbn}&Lzg4bEc1SURqeDVYOEw3cUJGMkRKQT09=cfcd208495d565ef66e7dff9f98764da'
    r = requests.get(book_url)
    soup = BeautifulSoup(r.text, features="html.parser")

    for a in soup.find_all(class_="pi-price"):
        if a.text != 'Out of Stock':
            return True

    return False


def is_shbi_instock(book_name):
    book_url = f'https://www.secondhandbooksindia.com/search?query={book_name}'
    r = requests.get(book_url)
    soup = BeautifulSoup(r.text, features="html.parser")
    for a in soup.find_all(class_="search-results-count"):
        if a.text[:len(NEG_RESULT)] != NEG_RESULT:
            return True
    return False


def find_existing_db(results_file):
    all_books = {}
    if os.path.exists(results_file):
        with open(results_file, 'rb') as fr:
            all_books = pkl.load(fr)
    else:
        logging.info(f'Empty initial results')
    return all_books
