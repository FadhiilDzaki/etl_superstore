# import libraries
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from elasticsearch import Elasticsearch
import pendulum

# set local time
id_tz = pendulum.timezone("Asia/Jakarta")

#default parameter
default_args = {
    'owner' : 'fadhiil',
    'depens_on_past' : False,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'start_date' : id_tz.datetime(2025,1,1)
}

# import data from postgres
def extract_data(**context):
    '''
    Fungsi ini digunakan untuk mengambil data dari postgresql.
    '''
    # create connection
    source_hook = PostgresHook(postgres_conn_id='superstore_db')
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()

    # read in pandas
    data_raw = pd.read_sql('SELECT * FROM superstore_raw', source_conn)

    # save df (temporary)
    temp_file = '/tmp/superstore_raw_data.csv'
    data_raw.to_csv(temp_file, index=False)

    # push dile path to Xcom
    context['ti'].xcom_push(key='raw_data_path', value=temp_file)

# cleaning raw data
def clean_data(**context):
    '''
    Fungsi ini digunakan untuk melakukan cleaning data berupa merubah nama kolom menjadi lower case, spasi menjadi underscore,
    membuat kolom baru yang berisi unique identifier, merubah nama kolom holiday menjadi event, merubah value menjadi lowercase,
    drop duplicate, handling missing value dan merubah date menjadi datetime.
    '''
    # get task instance
    ti = context['ti']

    # get raw data
    temp_file = ti.xcom_pull(task_ids='extract_data', key='raw_data_path')

    # transform to df
    df = pd.read_csv(temp_file)

    # define categorical & numerical columns    
    # before
    print(df)

    # cleaning process
    # lowercase column name
    df.columns = df.columns.str.lower()

    # change column name to snake case
    df.columns = df.columns.str.lower().str.replace('[ -]', '_', regex=True)

    # value to lowercase
    for i in df:
        if df[i].dtype == 'object':
            df[i] = df[i].str.lower()

    # handling duplicate
    df = df.drop_duplicates(keep='last')

    # group col
    id = ['row_id','order_id','customer_id','product_id']
    cat = ['ship_mode','segment','country', 'postal_code']
    num = ['sales','quantity','discount','profit']
    date_col = ['order_date', 'ship_date']

    # loop handling missing value
    for i in df.columns:
        # if i is an id column
        if i in id:
            # drop row
            df[i] = df[i].dropna()

        # if i is a date column
        elif i in date_col:
            # fill with previous date
            df[i] = df[i].fillna(method='ffill')
        
        # if i is a categorical column
        elif i in cat:
            # searc mode
            modus = df[i].mode()[0]
            # fill with mode
            df[i] = df[i].fillna(modus)

        # if i is a numerical column
        elif i in num:
            # calculate median
            med = df[i].median()
            # fill with median
            df[i] = df[i].fillna(med)

    # handling missing value customer_name
    id_to_name = df.dropna().drop_duplicates(subset='customer_id').set_index('customer_id')['customer_name'].to_dict()
    # fill name by customer_id
    df['customer_name'] = df['customer_id'].map(id_to_name).fillna(df['customer_name'])

    # handling missing value product_name
    id_to_product = df.dropna().drop_duplicates(subset='product_id').set_index('product_id')['product_name'].to_dict()
    # fill name by product_id
    df['product_name'] = df['product_id'].map(id_to_product).fillna(df['product_name'])

    # hancling missing value geography
    code_to_city= df.dropna(subset=['city']).drop_duplicates('postal_code').set_index('postal_code')['city'].to_dict()
    city_to_state= df.dropna(subset=['state']).drop_duplicates('city').set_index('city')['state'].to_dict()
    state_to_region= df.dropna(subset=['region']).drop_duplicates('state').set_index('state')['region'].to_dict()
    # fill missing value
    df['city'] = df['postal_code'].map(code_to_city).fillna(df['city'])
    df['state'] = df['city'].map(city_to_state).fillna(df['state'])
    df['region'] = df['state'].map(state_to_region).fillna(df['region'])

    # delete sisa missing value
    df = df.dropna()

    # change date to datetime
    for col in date_col:
        df[col] = pd.to_datetime(df[col])
    
    # clean data
    data_clean = df.copy()

    # after
    print(data_clean)
    print(data_clean.info())

    # save df to csv (temporary)
    temp_file = '/tmp/clean_data.csv'
    data_clean.to_csv(temp_file, index=False)

    # push dile path to Xcom
    context['ti'].xcom_push(key='clean_data_path', value=temp_file)

# upload clean data to elastic
def upload_postgres(**context):
    '''
    Fungsi ini digunakan untuk melakukan upload data ke elasticsearch.
    '''
    # get task instance
    ti = context['ti']

    # get raw data
    temp_file = ti.xcom_pull(task_ids='clean_data', key='clean_data_path')

    # read data
    data_clean = pd.read_csv(temp_file)

    # create connection
    source_hook = PostgresHook(postgres_conn_id='superstore_db')
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()

    for i, row in data_clean.iterrows():
        source_cursor.execute("""
            INSERT INTO superstore_clean 
                              (row_id, order_id, order_date, ship_date, ship_mode, customer_id, customer_name, segment, country, city, state, 
                              postal_code, region, product_id, category, sub_category, product_name , sales, quantity, discount, profit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['row_id'], row['order_id'], row['order_date'], row['ship_date'], row['ship_mode'], row['customer_id'], row['customer_name'], 
              row['segment'], row['country'], row['city'], row['state'], row['postal_code'], row['region'], row['product_id'], row['category'], 
              row['sub_category'], row['product_name'] , row['sales'], row['quantity'], row['discount'], row['profit']))

    source_conn.commit()
    source_conn.close()
    source_conn.close()


# DAG for otomation
with DAG(
    'superstore',
    description = 'import & clean raw data from PostgreSQL (raw table) and upload it to postgreSQL (clean table)',
    schedule_interval = '0 2 * * 1',
    default_args = default_args,
    catchup = False
    ) as dag:

    # task 1: import raw data from postgres
    get = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract_data,
        provide_context = True
    )

    # task 2: do data clean
    clean = PythonOperator(
        task_id = 'clean_data',
        python_callable = clean_data,
        provide_context = True
    )

    # task 3: upload cleaned data to postgres
    upload = PythonOperator(
        task_id = 'upload_postgres',
        python_callable = upload_postgres,
        provide_context=True
    )

get >> clean >> upload