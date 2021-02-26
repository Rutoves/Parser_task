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
            Complaint_ID INT NOT NULL
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
            TextFileReader = pd.read_csv(f, chunksize=CHUNK_SIZE)
            columns = None
            column_flag = True
            epoch = 0
            for chunk in TextFileReader:
                if column_flag:
                    columns = columns_parse(chunk.columns)
                    column_flag = False
                chunk.columns = columns
                print(f'Now is approximately {epoch} out of {NUMBER_OF_EPOCHS}')
                chunk.to_sql('all_data', engine, if_exists='append', index=False)
                epoch += 1
    engine.execute('ALTER TABLE all_data ADD update_stamp timestamp')
    print('Data have been successfully loaded')

def data_parse(data):
    data.columns = columns_parse(data.columns)
    data['date_received'] = pd.to_datetime(data['date_received'])
    data['date_sent_to_company'] = pd.to_datetime(data['date_sent_to_company'])

def update_database(engine):
    first_day = date_for_cur_month()

    print('Downloading complaints for current month...')

    r = requests.get(f'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?'
                     f'date_received_min={first_day}&field=all&format=csv')
    file = r.content.decode('utf-8')

    print('Data received...')

    received_data = pd.read_csv(io.StringIO(file))
    data_parse(received_data)

    old_complaint_id = pd.read_sql_query(f"SELECT complaint_id FROM all_data WHERE"
                                         f" date_received > '{first_day}'",
                                         con=engine)

    received_data = received_data.merge(old_complaint_id, on='complaint_id', how='outer')
    received_data['update_stamp'] = datetime.datetime.now()
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
