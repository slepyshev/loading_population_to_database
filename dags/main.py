import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.empty import EmptyOperator


def load_data_wiki(**context):
    # загружаем данные с источника
    url = 'https://ru.wikipedia.org/wiki/%D0%9D%D0%B0%D1%81%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5_%D1%81%D1%83%D0%B1%D1%8A%D0%B5%D0%BA%D1%82%D0%BE%D0%B2_%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D0%B9%D1%81%D0%BA%D0%BE%D0%B9_%D0%A4%D0%B5%D0%B4%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D0%B8'
    a = pd.read_html(url)
    df = a[0][['Субъект РФ', 'Всё населе- ние, чел', 'Городское население, чел.[3]', 'Сельское население, чел.[3]', 'Пло- щадь, км²[2]', 'Плотность населения, чел/км²', 'ФО']]

    # очищаем данные
    df.rename(columns={'Субъект РФ':'Регион',
                   'Всё населе- ние, чел':'Всё население',
                   'Городское население, чел.[3]': 'Городское население',
                   'Сельское население, чел.[3]' : 'Сельское население',
                   'Пло- щадь, км²[2]' : 'Площадь',
                   'Плотность населения, чел/км²': 'Плотность населения'
                  }, inplace=True)
    df['Всё население'] = df['Всё население'].str.strip('↗↘ ')
    df['Всё население'] = df['Всё население'].str.replace(u'\xa0', '')
    df['Городское население'] = df['Городское население'].str.replace(u'\xa0', '')
    df['Сельское население'] = df['Сельское население'].str.replace(u'\xa0', '')
    df['Площадь'] = df['Площадь'].str.replace(u'\xa0', '')
    df = df.set_index('Регион')
    df = df.drop(['Ханты-Мансийский автономный округ — Югра', 'Тюменская область без ХМАО и ЯНАО', 'Архангельская область без НАО', 'Ямало-Ненецкий автономный округ', 'Ненецкий автономный округ', 'РФ'], axis=0)
    df.reset_index(inplace=True)

    # сохраняем в файл csv в папку data
    os.makedirs('data', exist_ok=True)
    df.to_csv('data/testdf.csv', sep='|')
    context['ti'].xcom_push(key='pwd_my_data', value='data/testdf.csv')
    print('Данные успешно загружены с источника')

def loading_into_database(**context):
    # подключаемся к БД используя данные из connection
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # создаем таблицу если она не существует
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS population(
                            Region VARCHAR,
                            All_population Int,
                            Urban Int,
                            Rural Int,
                            Area Int,
                            Density Int,
                            District VARCHAR
                                );""")
        # очистим таблицу от данных (чтобы при повторном запуске данные были только свежие)
        cursor.execute('truncate population')
        # заберем из XCom путь расположения нашего csv файла с данными
        pwd_data = context['ti'].xcom_pull(key='pwd_my_data')
        # загрузим данные в БД
        df = pd.read_csv(pwd_data, header=0, delimiter='|', index_col=0)
        for i in df.index:
            cursor.execute(f"""INSERT INTO population (Region, All_population, Urban, Rural, Area, Density, District)
                        VALUES ('{df.iloc[i, 0]}', {df.iloc[i, 1]}, {df.iloc[i, 2]}, {df.iloc[i, 3]}, {df.iloc[i, 4]}, {df.iloc[i, 5]}, '{df.iloc[i, 6]}')
                        """)
        conn.commit()

        print('Данные успешно загружены в БД')

        # выгрузим данные из БД для проверки
        cursor.execute('select * from population')
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        res = pd.DataFrame(result, columns=columns)
        print('Данные в таблице:', res)

        cursor.close()
        conn.close()

    except Exception as error:
        conn.rollback()
        raise Exception(f'Загрузить данные в БД не получилось: {error}!')


# заворачиваем все в dag
with DAG(
    dag_id = "loading_population",
    start_date = datetime(2024, 11, 5),
    schedule_interval = "@daily",
    catchup = False,
    ) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    run_load_data_wiki = PythonOperator(
    task_id = "load_data_wiki",
    python_callable = load_data_wiki)

    run_loading_into_database = PythonOperator(
    task_id = "loading_into_database",
    python_callable = loading_into_database)


    start >> run_load_data_wiki >> run_loading_into_database >> end