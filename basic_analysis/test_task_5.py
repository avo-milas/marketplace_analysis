import pandas as pd
import numpy as np
import zipfile
import wget

def test_task_5():
    from task_5 import task_5
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

    res = task_5(reviews)
    assert res.date.min() == '2017-04-01'
    assert res.date.max() == '2018-04-30'
    assert res.csat.sum() == 1551.8881071384853
    assert res[res.date == '2017-07-11'].csat.values[0] == 4.291390728476821
    assert res[res.date == '2018-02-09'].csat.values[0] == 3.992156862745098
    assert res[res.csat == 3.6814814814814816].date.values[0] == '2018-02-25'
