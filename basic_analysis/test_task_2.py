import pandas as pd
import numpy as np
import zipfile
import wget

def test_task_2():
    from task_2 import task_2
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

    res = task_2(translation, products, items)
    assert res[res.seller_id == 'e3e15e2c0b9700561efac21c6be48066'].category.values[0] == 'housewares'
    assert res[res.seller_id == '2f73e04d12cdf0c945ded66bb3fcf6c7'].category.values[0] == 'garden_tools'
    assert len(res) == len(res.drop_duplicates())
    assert len(res) == 3035
    assert len(res[res.category == 'telephony']) == 66
    assert list(np.sort(res.groupby("category").agg({"seller_id": "nunique"}).seller_id.values)) == [  1,   1,   1,   1,   1,   2,   2,   2,   2,   2,   3,   3,   4,
             4,   4,   4,   5,   5,   5,   5,   5,   5,   6,   6,   6,   7,
             8,  10,  12,  13,  13,  13,  14,  14,  14,  15,  16,  17,  17,
            17,  19,  20,  20,  20,  21,  22,  26,  37,  37,  43,  46,  51,
            54,  59,  66,  66,  78,  87,  87,  99, 101, 116, 125, 156, 216,
           224, 256, 288, 310]
