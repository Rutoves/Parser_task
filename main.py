import pandas as pd
import datetime
import requests
import io
import zipfile
from sqlalchemy import create_engine
from Databse_settings import USER, PASSWORD, IP, PORT, DATABASE_NAME


CHUNK_SIZE = 100000

NUMBER_OF_EPOCHS = 1920000 // CHUNK_SIZE


def check_for_first_start(engine):
    res = engine.execute('''SELECT EXISTS(SELECT relname from pg_class 
    WHERE relname = 'all_data' AND relkind='r');''')
    res = not res.fetchone()[0]
    return res


def connection_close(con, trans):
    trans.commit()
    con.close()


def date_for_cur_month():
    cur_date = datetime.datetime.now()
    first_day = datetime.date(cur_date.year, cur_date.month, 1)
    return first_day.strftime('%Y-%m-%d')


def create_table(engine):
    engine.execute('''CREATE TABLE all_data (
            Date_received DATE,
            Product TEXT,
            Sub_product TEXT,
            Issue TEXT,
            Sub_issue TEXT,
            Consumer_complaint_narrative TEXT,
            Company_public_response TEXT,
            Company TEXT,
            State TEXT,
            ZIP_code TEXT,
            Tags TEXT,
            Consumer_consent_provided TEXT,
            Submitted_via TEXT,
            Date_sent_to_company DATE,
            Company_response_to_consumer TEXT,
            Timely_response TEXT,
            Consumer_disputed TEXT,
            Complaint_ID INT NOT NULL,
            Update_stamp TIMESTAMP
        )''')


def columns_parse(columns):
    return [i.replace(' ', '_')
                .replace('-', '_')
                .replace('?', '')
                .lower() for i in columns]


def load_database(engine):
    create_table(engine)

    print('Start downloading data...')
    r = requests.get("http://files.consumerfinance.gov/ccdb/complaints.csv.zip")
    print('Data received...')
    print('Starting uploading data to database...')
    with zipfile.ZipFile(io.BytesIO(r.content)) as archive:
        csv_file = archive.namelist()[0]
        with archive.open(csv_file) as f:
            TextFileReader = pd.read_csv(f, chunksize=CHUNK_SIZE, na_values='None')
            columns = None
            column_flag = True
            epoch = 0
            for chunk in TextFileReader:
                if column_flag:
                    columns = columns_parse(chunk.columns)
                    columns.append('update_stamp')
                    column_flag = False
                chunk['update_stamp'] = datetime.datetime.now()
                chunk.columns = columns
                print(f'Now is approximately {epoch} out of {NUMBER_OF_EPOCHS}')
                chunk.to_sql('all_data', engine, if_exists='append', index=False)
                epoch += 1
    print('Data have been successfully loaded')

def date_parse(data):
    data['date_received'] = pd.to_datetime(data['date_received'])
    data['date_sent_to_company'] = pd.to_datetime(data['date_sent_to_company'])
    data.fillna(value='', inplace=True)

def data_parse(data):
    data.columns = columns_parse(data.columns)
    data['consumer_disputed'] = data['consumer_disputed'].astype(object)
    date_parse(data)

def this_month_actual_data(day, engine):
    res = pd.read_sql_query(f'''SELECT *
         FROM all_data AS T1 
         WHERE date_received >= '{day}' AND NOT EXISTS(
              SELECT *
              FROM all_data AS T2
              WHERE T2.complaint_id = T1.complaint_id AND T2.update_stamp > T1.update_stamp
          )''', con=engine)
    res.drop('update_stamp', axis='columns', inplace=True)
    date_parse(res)
    return res

def update_database(engine):
    first_day = date_for_cur_month()

    print('Downloading complaints for current month...')

    r = requests.get(f'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?'
                     f'date_received_min={first_day}&field=all&format=csv')
    file = r.content.decode('utf-8')

    print('Data received...')

    received_data = pd.read_csv(io.StringIO(file), na_values='None')
    data_parse(received_data)

    old_data = this_month_actual_data(first_day, engine)

    not_null_data = old_data[pd.notnull(old_data['date_received'])][['date_received', 'complaint_id']]

    received_data = received_data.merge(not_null_data, how='outer')

    received_data = received_data.merge(old_data, indicator=True, how='left')\
        .loc[lambda x: x['_merge'] == 'left_only']
    received_data.drop(columns=['_merge'], inplace=True)

    if received_data.empty:
        print('No updates have been found')
        return
    received_data['update_stamp'] = datetime.datetime.now()
    print('New updates have been found')
    received_data.to_sql('all_data', engine, if_exists='append', index=False)

    print('Data have been successfully updated')


if __name__ == '__main__':
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{IP}:{PORT}/{DATABASE_NAME}')

    if check_for_first_start(engine):
        # first time
        load_database(engine)
    else:
        # second time
        update_database(engine)
