import pandas as pd
import zipfile
import wget
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from dateutil.parser import parse
from datetime import datetime
import datetime
import json
import pandas as pd
import plotly as plt
from urllib.request import urlopen


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

to_add = pd.DataFrame({"product_category_name": ["portateis_cozinha_e_preparadores_de_alimentos", "pc_gamer"],
                        "product_category_name_english" : ["portable kitchen and food preparers", "PC Gamer"]})
translation = pd.concat([translation, to_add], ignore_index = True)
products = products.merge(translation, on='product_category_name', how='left')
products.drop('product_category_name', axis=1, inplace=True)

# 4
orders['order_purchase_date'] = orders['order_purchase_timestamp'].apply(
    lambda x: parse(x, yearfirst=True).strftime('%Y-%m-%d'))
dt = items.merge(products, on='product_id', how='left')
dt = dt.merge(sellers, on='seller_id', how='left')
dt = dt.merge(orders, on='order_id', how='left')
dt = dt.merge(customers, on='customer_id', how='left')
all_states = set(dt['customer_state']).union(dt['seller_state'])
dt['dt'] = pd.to_datetime(dt['order_purchase_timestamp'])
dt['dt_ts'] = dt['dt'].apply(lambda x: x.timestamp())

daterange = pd.date_range(start=dt.dt.min(), end=dt.dt.max(), freq='w')



# 5
# Brazil coordinates / shape
with urlopen(
    "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
) as response:
    brazil = json.load(response)

all_states = set()

# Since the database doesn't have an ID or feature using which values will be mapped between the coordinate/shape database and soybean database, we are adding an ID ourselves.
for feature in brazil["features"]:
    feature["id"] = feature["properties"]["sigla"]
    all_states.add(feature["id"])


dt_map = items.merge(orders, on='order_id', how='left')
dt_map = dt_map.merge(sellers, on='seller_id', how='left')
dt_map = dt_map.merge(customers, on='customer_id', how='left')
dt_map['dt'] = pd.to_datetime(dt_map['order_purchase_timestamp'])
dt_map['dt_ts'] = dt_map['dt'].apply(lambda x: x.timestamp())

dt_sel = dt_map.groupby('seller_state', as_index=False).agg({'seller_id': 'nunique'})
dt_sel.rename(columns={'seller_state': 'state'}, inplace=True)
dt_cust = dt_map.groupby('customer_state', as_index=False).agg({'customer_id': 'nunique'})
dt_cust.rename(columns={'customer_state': 'state'}, inplace=True)
gr_dt_map = pd.concat([dt_sel, dt_cust])
gr_dt_map = gr_dt_map.groupby('state', as_index=False).agg({'seller_id': 'sum', 'customer_id': 'sum'})
states_with_cust_sel = set(gr_dt_map['state'].unique())
for st in (all_states - states_with_cust_sel):
    gr_dt_map.loc[len(gr_dt_map.index)] = [st, 0, 0]
gr_dt_map.seller_id = gr_dt_map.seller_id.astype(int)
gr_dt_map.customer_id = gr_dt_map.customer_id.astype(int)
gr_dt_map.rename(columns={'seller_id': 'количество продавцов', 'customer_id': 'количество покупателей'}, inplace=True)


counties = brazil


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H1("Продажи на бразильском маркетплейсе Olist"),

    html.H3("Штат"),
    dcc.Dropdown(
        id='states',
        options=[{'label': state, 'value': state} for state in all_states],
        multi=True,
        value=list(all_states),
        placeholder='Штат'
    ),

    html.H3("Статус заказа"),
    dcc.Dropdown(
        id='statuses',
        options=[{'label': status, 'value': status} for status in dt['order_status'].unique()],
        multi=True,
        value=dt['order_status'].unique(),
        placeholder='Статус заказа'
    ),

    html.H3("Дата"),
    dcc.RangeSlider(
        id='date-slider',
        min=dt['dt'].min().timestamp(),
        max=dt['dt'].max().timestamp(),
        step=86400,
        marks={i: f'{datetime.datetime.utcfromtimestamp(i).strftime("%Y-%m-%d")}' for i in range(int(dt.dt_ts.min()), int(dt.dt_ts.max()), 86400 * 7 * 5)},
        allowCross=False
    ),

    html.H3("Распределение по категориям покупок"),
    dcc.Graph(
        id='graph_cat_cus',
        figure=px.histogram(
            dt,
            x='product_category_name_english',
            color='order_status',
            barmode='stack'
        ),
        style={'height': '500px'}
    ),

    html.H3("Распределение по категориям продаж"),
    dcc.Graph(
        id='graph_cat_sel',
        figure=px.histogram(
            dt,
            x='product_category_name_english',
            color='order_status',
            barmode='stack'
        )
    ),

    html.H3("Карта с распределением продавцов и покупателей"),
    dcc.Graph(
        id='map_states',
        figure=px.choropleth(gr_dt_map,
                             geojson=counties, 
                             locations='state',
                             hover_data={'state': True, 'количество покупателей': True, 'количество продавцов': True},
                             scope="south america",
                             color='state'
                          )
    )


])


@app.callback(
    Output(component_id='graph_cat_cus', component_property='figure'),
    [Input(component_id='states', component_property='value'),
     Input(component_id='statuses', component_property='value'),
     Input(component_id='date-slider', component_property='value'),
     Input(component_id='map_states', component_property='clickData')]
)
def update_graph(selected_states, selected_statuses, selected_dates, click_data):

    if selected_dates is not None:
        date_l, date_r = selected_dates
    else:
        date_l, date_r = dt['dt'].min().timestamp(), dt['dt'].max().timestamp()
    filtered_date_data = dt[
        (date_l <= dt['dt_ts']) & (dt['dt_ts'] <= date_r)]

    filtered_data = filtered_date_data[filtered_date_data['order_status'].isin(selected_statuses)]

    filtered_states_data = filtered_data[filtered_data['customer_state'].isin(selected_states)]

    # 6
    if click_data is not None:
        selected_state = click_data['points'][0]['location']
        filtered_states_data = filtered_data[filtered_data['customer_state'] == selected_state]

    fig = px.histogram(filtered_states_data, x='product_category_name_english', color='order_status', barmode='stack',
                       title=f'Покупки в периоде с {datetime.datetime.utcfromtimestamp(date_l).strftime("%Y-%m-%d")} по {datetime.datetime.utcfromtimestamp(date_r).strftime("%Y-%m-%d")}')
    return fig


@app.callback(
    Output(component_id='graph_cat_sel', component_property='figure'),
    [Input(component_id='states', component_property='value'),
     Input(component_id='statuses', component_property='value'),
     Input(component_id='date-slider', component_property='value'),
     Input(component_id='map_states', component_property='clickData')]
)
def update_graph(selected_states, selected_statuses, selected_dates, click_data):
    if selected_dates is not None:
        date_l, date_r = selected_dates
    else:
        date_l, date_r = dt['dt'].min().timestamp(), dt['dt'].max().timestamp()
    filtered_date_data = dt[
        (date_l <= dt['dt_ts']) & (dt['dt_ts'] <= date_r)]

    filtered_data = filtered_date_data[filtered_date_data['order_status'].isin(selected_statuses)]

    filtered_states_data = filtered_data[filtered_data['seller_state'].isin(selected_states)]

    # 6
    if click_data is not None:
        selected_state = click_data['points'][0]['location']
        filtered_states_data = filtered_data[filtered_data['seller_state'] == selected_state]

    fig = px.histogram(filtered_states_data, x='product_category_name_english', color='order_status', barmode='stack',
                       title=f'Продажи в периоде с {datetime.datetime.utcfromtimestamp(date_l).strftime("%Y-%m-%d")} по {datetime.datetime.utcfromtimestamp(date_r).strftime("%Y-%m-%d")}')
    return fig


@app.callback(
    Output(component_id='map_states', component_property='figure'),
    [Input(component_id='statuses', component_property='value'),
     Input(component_id='date-slider', component_property='value')]
)
def update_graph(selected_statuses, selected_dates):

    filt_data = dt_map[dt_map['order_status'].isin(selected_statuses)]

    if selected_dates is not None:
        date_l, date_r = selected_dates
    else:
        date_l, date_r = filt_data['dt'].min().timestamp(), filt_data['dt'].max().timestamp()

    filt_data = filt_data[
        (date_l <= filt_data['dt_ts']) & (filt_data['dt_ts'] <= date_r)]


    dt_sel = filt_data.groupby('seller_state', as_index=False).agg({'seller_id': 'nunique'})
    dt_sel.rename(columns={'seller_state': 'state'}, inplace=True)
    dt_cust = filt_data.groupby('customer_state', as_index=False).agg({'customer_id': 'nunique'})
    dt_cust.rename(columns={'customer_state': 'state'}, inplace=True)
    gr_dt_map = pd.concat([dt_sel, dt_cust])
    gr_dt_map = gr_dt_map.groupby('state', as_index=False).agg({'seller_id': 'sum', 'customer_id': 'sum'})
    states_with_cust_sel = set(gr_dt_map['state'].unique())
    for st in (all_states - states_with_cust_sel):
        gr_dt_map.loc[len(gr_dt_map.index)] = [st, 0, 0]
    gr_dt_map.seller_id = gr_dt_map.seller_id.astype(int)
    gr_dt_map.customer_id = gr_dt_map.customer_id.astype(int)
    gr_dt_map.rename(columns={'seller_id': 'количество продавцов', 'customer_id': 'количество покупателей'}, inplace=True)


    fig = px.choropleth(gr_dt_map, geojson=counties, locations='state',
          hover_data={'state': True, 'количество покупателей': True, 'количество продавцов': True},
          scope="south america",
          color='state'
        )
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
