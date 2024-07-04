import pandas as pd
import numpy as np
import zipfile
import wget

def test_task_8():
    from task_8 import task_8
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

    res = task_8(orders, items, sellers, customers)
    assert np.all(res.share.values == np.sort(res.share.values)[::-1])
    assert res.share.values[0] == 0.9743589743589743
    assert res.share.values[-1] == 0.9356435643564357
    assert res.seller_id.values[5] == '1b4c3a6f53068f0b6944d2d005c9fc89'
    assert res.seller_id.values[2] == '06a2c3af7b3aee5d69171b0e14f0ee87'
    assert res.share.sum() == 9.519118744616716
