import pandas as pd
import numpy as np
import zipfile
import wget

def test_task_6():
    from task_6 import task_6
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

    res = task_6(reviews)
    assert res.orders.sum() == 99224
    assert np.all(res.days.values == np.sort(res.days.values))
    assert len(res) == 214
    assert res.days.min() == 0
    assert res.days.max() == 518
    assert res[res.days == 233].csat.values[0] == 3.0
    assert res[res.days == 87].orders.values[0] == 4
