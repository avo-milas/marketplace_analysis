import pandas as pd
import numpy as np
import zipfile
import wget

def test_task_3():
    from task_3 import task_3
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

    res = task_3(orders, customers, items)
    assert np.allclose(res.perc.sum(), 1)
    assert np.allclose(res[res.state == "RS"].perc.values[0], 0.055868056429816286)
    assert np.allclose(res.sort_values("perc", ascending=True).iloc[0, 1], 0.0005862290943146945)
    assert np.allclose(res.sort_values("perc", ascending=False).iloc[0, 1], 0.3741756035817322)
    assert len(res) == 27
