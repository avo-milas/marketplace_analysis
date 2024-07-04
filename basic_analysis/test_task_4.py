import pandas as pd
import numpy as np
import zipfile
import wget

def test_task_4():
    from task_4 import task_4
    url = 'https://github.com/Palladain/Deep_Python/raw/main/Homeworks/Homework_1/archive.zip'
    filename = wget.download(url)
    
    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall('./')
    
    customers = pd.read_csv('olist_customers_dataset.csv')
    location = pd.read_csv('olist_geolocation_dataset.csv')
    items = pd.read_csv('olist_order_items_dataset.csv')
    payments = pd.read_csv('olist_order_payments_dataset.csv')
    reviews = pd.read_csv('olist_order_reviews_dataset.csv')
    orders = pd.read_csv('olist_orders_dataset.csv')
    products = pd.read_csv('olist_products_dataset.csv')
    translation = pd.read_csv('product_category_name_translation.csv')
    sellers = pd.read_csv('olist_sellers_dataset.csv')

    res = task_4(items, orders, customers)
    assert res == (137.7540763788945, 22.823561713254815, 1.1417306873695092, 1.0348089410589412)
