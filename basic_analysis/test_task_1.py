import pandas as pd
import numpy as np
import zipfile
import wget

def test_task_1():
    from task_1 import task_1
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

    res = task_1(translation, items, products)
  
    assert res[res.category == 'portable kitchen and food preparers'].price.values[0] == 186.996
    assert len(res) == 73
    assert len(res.drop_duplicates()) == 73
    assert res[res.category == 'drinks'].products.values[0] == 81
    assert res.products.sum() == 32341
    assert res.price.sum() == 12459.751444351941
    assert res[res.category == 'home_confort'].price.values[0] == 185.56926417326417
