# Импорт необходимых модулей
import pandas as pd
import logging
from prophet import Prophet
from sqlalchemy import create_engine, text
import sys
import pyodbc
import oracledb
import re
from datetime import datetime, timedelta
import numpy as np
from urllib.parse import quote_plus
import concurrent.futures  # для многопоточного выполнения
import time

# Инициализация Oracle клиента (укажите свою папку с instantclient)
oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_7")

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#######################################################################
# 0. Функция создания подключения к SQL Server с правильным форматом
#######################################################################
def create_robust_sql_connection(max_retries=3):
    """Создание подключения к SQL Server с повторными попытками"""
    driver = 'ODBC Driver 18 for SQL Server'
    host = ''
    port = ''
    database = ''
    username = ''
    password = ''

    # PyODBC connection string
    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"  
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=60;"
        f"Command Timeout=600;"
    )

    # Правильное экранирование пароля для SQLAlchemy
    password_encoded = quote_plus(password)
    username_encoded = quote_plus(username)
    driver_encoded = quote_plus(driver)
    
    # SQLAlchemy connection string (исправленный формат)
    engine_string = (
        f"mssql+pyodbc://{username_encoded}:{password_encoded}@{host}:{port}/{database}"
        f"?driver={driver_encoded}&TrustServerCertificate=yes&timeout=60"
    )

    for attempt in range(max_retries):
        try:
            print(f"Попытка подключения к SQL Server #{attempt + 1}")
            
            # Сначала тестируем PyODBC
            conn = pyodbc.connect(connection_string)
            print("✅ PyODBC подключение к SQL Server успешно")
            
            # Теперь создаем SQLAlchemy engine
            engine = create_engine(
                engine_string,
                pool_size=5,
                max_overflow=10,
                pool_timeout=60,
                pool_recycle=3600,
                connect_args={
                    "timeout": 60,
                    "autocommit": True
                }
            )
            
            # Тест SQLAlchemy подключения
            with engine.connect() as test_conn:
                result = test_conn.execute(text("SELECT 1 AS test"))
                test_result = result.fetchone()
                if test_result[0] != 1:
                    raise Exception("SQLAlchemy тест не прошел")
            
            print("✅ SQLAlchemy подключение к SQL Server успешно")
            return conn, engine, connection_string
            
        except Exception as e:
            print(f"❌ Попытка #{attempt + 1} не удалась: {e}")
            
            # Закрываем частично созданные подключения
            try:
                if 'conn' in locals():
                    conn.close()
                if 'engine' in locals():
                    engine.dispose()
            except:
                pass
            
            if attempt < max_retries - 1:
                print(f"⏳ Ожидание 10 секунд перед повтором...")
                time.sleep(10)
            else:
                print("💥 Все попытки подключения исчерпаны!")
                
                # Пробуем альтернативный способ подключения
                print("🔄 Пробуем альтернативный способ подключения...")
                try:
                    return create_alternative_connection()
                except Exception as alt_error:
                    print(f"❌ Альтернативный способ тоже не сработал: {alt_error}")
                    raise

def create_alternative_connection():
    """Альтернативный способ подключения через чистый PyODBC"""
    driver = 'ODBC Driver 18 for SQL Server'
    host = ''
    port = ''
    database = ''
    username = ''
    password = ''

    print("🔧 Использование альтернативного способа подключения...")
    
    # Пробуем разные варианты connection string
    connection_strings = [
        # Вариант 1: с фигурными скобками
        (
            f"DRIVER={{{driver}}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=no;"
        ),
        # Вариант 2: без порта
        (
            f"DRIVER={{{driver}}};"
            f"SERVER={host};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=no;"
        ),
        # Вариант 3: старый стиль
        (
            f"DRIVER={{{driver}}};"
            f"Server={host}\\SQLEXPRESS,{port};"
            f"Database={database};"
            f"Uid={username};"
            f"Pwd={password};"
            f"TrustServerCertificate=yes;"
        )
    ]
    
    for i, conn_str in enumerate(connection_strings, 1):
        try:
            print(f"   Пробуем вариант {i}...")
            conn = pyodbc.connect(conn_str)
            
            # Создаем простой engine через create_engine с raw подключением
            def get_connection():
                return pyodbc.connect(conn_str)
            
            # Создаем "псевдо-engine" для совместимости
            class SimpleEngine:
                def __init__(self, connection_func):
                    self.get_conn = connection_func
                    
                def connect(self):
                    return self.get_conn()
                    
                def execute(self, query):
                    conn = self.get_conn()
                    cursor = conn.cursor()
                    cursor.execute(str(query))
                    result = cursor.fetchall()
                    cursor.close()
                    conn.close()
                    return result
                    
                def dispose(self):
                    pass
                    
                def begin(self):
                    return self.connect()
            
            engine = SimpleEngine(get_connection)
            
            print(f"✅ Альтернативное подключение (вариант {i}) успешно!")
            return conn, engine, conn_str
            
        except Exception as e:
            print(f"   ❌ Вариант {i} не сработал: {e}")
            continue
    
    raise Exception("Все альтернативные способы подключения не сработали")

def execute_query_with_retry(query, engine, connection_string, username, password, host, database, driver, max_retries=3):
    """Выполнение запроса с повторными попытками"""
    for attempt in range(max_retries):
        try:
            print(f"🔄 Выполнение запроса... (попытка {attempt + 1}/{max_retries})")
            
            # Если у нас простой engine, используем PyODBC напрямую
            if hasattr(engine, 'get_conn'):
                conn = engine.get_conn()
                df = pd.read_sql_query(query, con=conn)
                conn.close()
            else:
                # Стандартный SQLAlchemy
                df = pd.read_sql_query(query, con=engine)
            
            print(f"✅ Запрос выполнен успешно. Получено {len(df):,} строк")
            return df
            
        except Exception as e:
            print(f"❌ Попытка {attempt + 1} не удалась: {e}")
            
            if attempt < max_retries - 1:
                print("⏳ Ожидание 15 секунд перед повтором...")
                time.sleep(15)
                
                # Пересоздаем подключение при проблемах
                print("🔄 Пересоздание подключения к SQL Server...")
                try:
                    if hasattr(engine, 'dispose'):
                        engine.dispose()
                    
                    # Пересоздаем подключение
                    new_conn, engine, _ = create_robust_sql_connection(max_retries=1)
                    print("✅ Подключение пересоздано")
                        
                except Exception as reconnect_error:
                    print(f"⚠️ Не удалось пересоздать подключение: {reconnect_error}")
            else:
                print("💥 Все попытки исчерпаны!")
                raise

#######################################################################
# 1. ФУНКЦИЯ ВЫБОРА ДАТЫ ПРОГНОЗА
#######################################################################
def select_forecast_date():
    """
    Функция для выбора даты прогноза с проверкой корректности ввода
    """
    print("\n" + "="*50)
    print("ВЫБОР ДАТЫ ПРОГНОЗА")
    print("="*50)
    
    # Показываем текущую дату
    current_date = datetime.now()
    print(f"Текущая дата: {current_date.strftime('%Y-%m-%d (%A)')}")
    
    while True:
        print("\nВарианты выбора:")
        print("1. Использовать текущую дату")
        print("2. Ввести дату вручную")
        print("3. Выбрать дату из последних 7 дней")
        
        choice = input("\nВыберите опцию (1-3): ").strip()
        
        if choice == "1":
            # Используем текущую дату
            selected_date = current_date
            break
            
        elif choice == "2":
            # Ввод даты вручную
            print("\nВведите дату в формате YYYY-MM-DD (например, 2024-12-15):")
            date_input = input("Дата: ").strip()
            
            try:
                selected_date = datetime.strptime(date_input, "%Y-%m-%d")
                
                # Проверка на будущую дату
                if selected_date > current_date:
                    print("⚠️  Нельзя выбрать будущую дату!")
                    continue
                    
                # Проверка на слишком старую дату
                if selected_date < current_date - timedelta(days=30):
                    confirm = input("⚠️  Выбранная дата старше 30 дней. Продолжить? (y/n): ").strip().lower()
                    if confirm != 'y':
                        continue
                
                break
                
            except ValueError:
                print("❌ Неверный формат даты! Используйте формат YYYY-MM-DD")
                continue
                
        elif choice == "3":
            # Выбор из последних 7 дней
            print("\nПоследние 7 дней:")
            for i in range(7):
                date_option = current_date - timedelta(days=i)
                weekday_ru = {
                    0: "Понедельник", 1: "Вторник", 2: "Среда", 3: "Четверг",
                    4: "Пятница", 5: "Суббота", 6: "Воскресенье"
                }
                print(f"{i+1}. {date_option.strftime('%Y-%m-%d')} ({weekday_ru[date_option.weekday()]})")
            
            try:
                day_choice = int(input("\nВыберите день (1-7): ").strip())
                if 1 <= day_choice <= 7:
                    selected_date = current_date - timedelta(days=day_choice-1)
                    break
                else:
                    print("❌ Выберите число от 1 до 7")
                    continue
            except ValueError:
                print("❌ Введите корректное число!")
                continue
        else:
            print("❌ Выберите опцию от 1 до 3")
            continue
    
    return selected_date

#######################################################################
# 2. Подключение к SQL Server с улучшенной обработкой ошибок
#######################################################################
print("🔧 Инициализация подключения к SQL Server...")
try:
    conn, engine, connection_string = create_robust_sql_connection()
    
    # Сохраняем параметры для retry функций
    driver = 'ODBC Driver 18 for SQL Server'
    host = '10.10.120.6'
    port = '1433'
    database = 'eho_export2acc'
    username = 'powerbi'
    password = 'De$!gn2025_@'
    driver_encoded = driver
    
except Exception as e:
    logging.error(f"Критическая ошибка при подключении к SQL Server: {e}")
    print(f"💥 Критическая ошибка при подключении к SQL Server: {e}")
    sys.exit(1)

#######################################################################
# 3. Определение даты прогноза и настройка
#######################################################################
# Выбираем дату прогноза
forecast_datetime = select_forecast_date()
FORECAST_DATE = forecast_datetime.strftime("%Y-%m-%d")

# Определяем, является ли выбранная дата пятницей (пятница имеет weekday=4 в Python)
IS_FRIDAY = forecast_datetime.weekday() == 4

# Определяем горизонт прогноза в зависимости от дня недели
if IS_FRIDAY:
    FORECAST_HOURS = 71  # 3 дня (72 часа минус 1)
    forecast_period = "3 дня (71 час)"
else:
    FORECAST_HOURS = 47  # 2 дня (48 часов минус 1)
    forecast_period = "2 дня (47 часов)"

# Показываем итоговую информацию
print("\n" + "="*50)
print("ПАРАМЕТРЫ ПРОГНОЗА")
print("="*50)
print(f"Дата прогноза: {FORECAST_DATE}")
print(f"День недели: {['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'][forecast_datetime.weekday()]}")
print(f"Горизонт прогноза: {forecast_period}")
print(f"Период прогноза: {forecast_datetime.strftime('%Y-%m-%d %H:%M')} - {(forecast_datetime + timedelta(hours=FORECAST_HOURS)).strftime('%Y-%m-%d %H:%M')}")
print("="*50)

# Подтверждение
confirm = input("\nПродолжить с этими параметрами? (y/n): ").strip().lower()
if confirm != 'y':
    print("Операция отменена.")
    sys.exit(0)

#######################################################################
# 4. Чтение CSV с мёртвыми остатками
#######################################################################
print("📄 Чтение данных из CSV...")
try:
    deadstock_data = pd.read_csv('deadstock_info_new.csv')

    # Переименование столбцов в соответствии с вашими нуждами
    deadstock_data.rename(columns={
        'Gas_Station_Name': 'Gas_Station_Name',
        'City': 'City',
        'Branch': 'Branch',
        'ObjectCode': 'OBJECTCODE',
        'Tank_Number': 'Tank_Number',
        'Deadstock_Level': 'Level_cm',
        'Deadstock_Volume': 'Volume_liters',
        'Max_Level': 'Max_Level',
        'Max_Volume': 'Max_Volume'
    }, inplace=True)

    # Обрабатываем обе локации: Астана и ВКО
    # Создаем пустой DataFrame для сбора данных из обеих локаций
    filtered_data = pd.DataFrame()

    # Фильтрация по городу
    city_data = deadstock_data[deadstock_data['City'] == "Астана"].copy()
    if not city_data.empty:
        filtered_data = pd.concat([filtered_data, city_data])
        print(f"✅ Данные для города Астана загружены. Кол-во строк: {len(city_data)}")

    # Фильтрация по ветке ВКО
    branch_data = deadstock_data[deadstock_data['Branch'] == "ВКО"].copy()
    if not branch_data.empty:
        filtered_data = pd.concat([filtered_data, branch_data])
        print(f"✅ Данные для ветки ВКО загружены. Кол-во строк: {len(branch_data)}")

    # Проверка, есть ли данные для обработки
    if filtered_data.empty:
        print("❌ Нет данных для города Астана или ветки ВКО в CSV. Останавливаем скрипт.")
        sys.exit(1)

    # Переименуем для совместимости с остальным кодом
    city_data = filtered_data
    
except Exception as e:
    logging.error(f"Ошибка при чтении CSV файла: {e}")
    print(f"❌ Ошибка при чтении CSV файла: {e}")
    sys.exit(1)

#######################################################################
# 5. Подключение к Oracle и чтение статусов АЗС
#######################################################################
print("🔧 Подключение к Oracle...")
oracle_username = "alikhan"
oracle_password = "C0n$ul25"
oracle_host = "10.10.120.96"
oracle_port = "1521"
oracle_service_name = "ORCL"

oracle_password_encoded = quote_plus(oracle_password)
oracle_connection_string = (
    f"oracle+oracledb://{oracle_username}:{oracle_password_encoded}"
    f"@{oracle_host}:{oracle_port}/?service_name={oracle_service_name}"
)

try:
    oracle_engine = create_engine(oracle_connection_string)
    print("✅ Успешное подключение к Oracle.")
except Exception as e:
    logging.error(f"Ошибка при создании SQLAlchemy движка для Oracle: {e}")
    print(f"❌ Ошибка при создании SQLAlchemy движка для Oracle: {e}")
    sys.exit(1)

# Собираем список уникальных OBJECTCODE из CSV
unique_objcodes = city_data['OBJECTCODE'].unique().tolist()

# Функция для формирования части IN(...)
def prepare_in_clause(values, is_string=False):
    if is_string:
        return ','.join(f"'{v}'" for v in values)
    else:
        return ','.join(str(v) for v in values)

objcodes_str = prepare_in_clause(unique_objcodes, is_string=True)

# Запрос к Oracle для получения статусов АЗС
status_query = f"""
SELECT
    t.OBJECTCODE,
    t.STATUS
FROM GS.AZS t
WHERE t.OBJECTCODE IN ({objcodes_str})
"""

try:
    status_df = pd.read_sql(status_query, con=oracle_engine)
    status_df.columns = [col.upper().strip() for col in status_df.columns]
    print("✅ Список статусов АЗС загружен из Oracle.")
except Exception as e:
    logging.error(f"Ошибка при чтении статусов из Oracle: {e}")
    print(f"❌ Ошибка при чтении статусов из Oracle: {e}")
    sys.exit(1)

if status_df.empty:
    print("❌ Не найдено никаких АЗС в Oracle по тем OBJECTCODE, что есть в CSV.")
    sys.exit(1)

#######################################################################
# 6. Мерджим CSV-данные и статусы, оставляем только STATUS=1
#######################################################################
print("🔄 Обработка и фильтрация данных...")
city_data = city_data.merge(status_df, on='OBJECTCODE', how='left')

# Фильтруем только те, у которых статус = 1 (in progress)
city_data = city_data[city_data['STATUS'] == 1]

if city_data.empty:
    print("❌ После фильтрации по STATUS=1 не осталось данных. Останавливаем скрипт.")
    sys.exit(1)

#######################################################################
# 7. Предварительная обработка (извлечение TANK, FuelType)
#######################################################################
def extract_tank_and_fuel(tank_value):
    # Ищем "Резервуар + число"
    tank_match = re.search(r'Резервуар[ау]?\s*(\d+)', str(tank_value), re.IGNORECASE)
    if (tank_match):
        tank_number = int(tank_match.group(1))
    else:
        tank_number = None

    # Ищем тип топлива с более точным сопоставлением
    fuel_match = re.search(
        r'(АИ-80|АИ-92|АИ-95|АИ-98|ДТ-З-32|ДТ-З-25|ДТ-Л|ДТЗ|ДТ|СУГ|АИ-95-IMPORT)',
        str(tank_value),
        re.IGNORECASE
    )
    if fuel_match:
        fuel_name = fuel_match.group(1).upper()
    else:
        fuel_name = None

    return pd.Series({'TANK': tank_number, 'FuelType': fuel_name})

# Применяем функцию к полю 'Tank_Number'
city_data[['TANK', 'FuelType']] = city_data['Tank_Number'].apply(extract_tank_and_fuel)

# Удаляем строки с отсутствующими данными
city_data.dropna(subset=['TANK', 'FuelType'], inplace=True)

# Приводим TANK к int
city_data['TANK'] = city_data['TANK'].astype(int)

# Словарь соответствия FuelType -> GASNUM
fuel_mapping = {
    'АИ-80': '3300000000',
    'АИ-92': '3300000002',
    'АИ-95': '3300000005',
    'АИ-98': '3300000008',
    'ДТ': '3300000010',
    'ДТ-Л': '3300000010',
    'ДТ-3-25': '3300000029',
    'ДТ-3-32': '3300000038',
    'СУГ': '3400000000',
    'АИ-95-IMPORT': '3300000095',
    'ДТЗ': '3300000010'
}

print(f"✅ Количество строк после подготовки и фильтрации: {len(city_data)}")

# Получаем уникальные комбинации
unique_combinations = city_data[['OBJECTCODE', 'TANK', 'FuelType']].drop_duplicates()
print(f"✅ Количество уникальных комбинаций: {len(unique_combinations)}")

#######################################################################
# 8. Подготовка к чтению данных из ord_salesbyhour (SQL Server)
#######################################################################
# Собираем параметры (OBJECTCODE, GASNUM, TANK) для запроса
params_list = []
for _, row in unique_combinations.iterrows():
    object_code = row['OBJECTCODE']
    tank_number = row['TANK']
    fuel_name   = row['FuelType']
    gasnum      = fuel_mapping.get(fuel_name)
    if gasnum:
        params_list.append((object_code, gasnum, tank_number))

object_codes = list({p[0] for p in params_list})
gasnums      = list({p[1] for p in params_list})
tanks        = list({p[2] for p in params_list})

object_codes_str = prepare_in_clause(object_codes, is_string=True)
gasnums_str      = prepare_in_clause(gasnums,      is_string=True)
tanks_str        = prepare_in_clause(tanks,        is_string=False)

# Запрос к ord_salesbyhour
query_sales = f"""
SELECT
    CAST(r_day AS DATE) AS DATE,
    r_hour AS R_HOUR,
    objectcode AS OBJECTCODE,
    gasnum AS GASNUM,
    tank AS TANK,
    ISNULL(RECEIPTS_VOLUME, 0) AS RECEIPTS_VOLUME
FROM ord_salesbyhour
WHERE
    objectcode IN ({object_codes_str})
    AND gasnum IN ({gasnums_str})
    AND tank IN ({tanks_str})
"""

print(f"📊 Загрузка данных продаж для {len(object_codes)} АЗС, {len(gasnums)} видов топлива, {len(tanks)} резервуаров...")

try:
    df_all = execute_query_with_retry(
        query_sales, 
        engine, 
        connection_string, 
        username, 
        password, 
        host, 
        database, 
        driver, 
        max_retries=5
    )
    print(f"✅ Данные по продажам из ord_salesbyhour успешно загружены: {len(df_all):,} записей")
except Exception as e:
    logging.error(f"Критическая ошибка при чтении из ord_salesbyhour: {e}")
    print(f"💥 Критическая ошибка при чтении из ord_salesbyhour: {e}")
    sys.exit(1)

# Преобразование DATE + R_HOUR -> datetime
df_all['DATE'] = pd.to_datetime(df_all['DATE'], errors='coerce')
df_all.loc[df_all['R_HOUR'] == 24, 'R_HOUR'] = 0
df_all.loc[df_all['R_HOUR'] == 0, 'DATE'] = df_all['DATE'] + pd.Timedelta(days=1)

df_all['ds'] = pd.to_datetime(
    df_all['DATE'].dt.strftime('%Y-%m-%d') + ' ' +
    df_all['R_HOUR'].astype(str) + ':00:00'
)
df_all['weekday'] = df_all['ds'].dt.weekday
df_all['GASNUM']  = df_all['GASNUM'].astype(str)

# Группируем
grouped = df_all.groupby(['OBJECTCODE', 'TANK', 'GASNUM'])

print(f"📈 Данные обработаны. Период: {df_all['ds'].min()} - {df_all['ds'].max()}")

#######################################################################
# 9. Загрузка текущих объёмов из Oracle (BI.tigmeasurements)
#######################################################################
print("📊 Загрузка текущих объемов из Oracle...")
oracle_query_current = f"""
SELECT t.OBJECTCODE, t.TANK, t.GASNUM, t.VOLUME
FROM BI.tigmeasurements t
WHERE t.ID IN (
    SELECT MAX(ID)
    FROM BI.tigmeasurements
    WHERE TRUNC(POSTIMESTAMP) = TO_DATE('{FORECAST_DATE}', 'YYYY-MM-DD')
    GROUP BY OBJECTCODE, TANK, GASNUM
)
AND t.OBJECTCODE IN ({object_codes_str})
AND t.TANK IN ({tanks_str})
AND t.GASNUM IN ({gasnums_str})
"""

try:
    oracle_df = pd.read_sql(oracle_query_current, con=oracle_engine)
    print(f"✅ Текущие объемы из BI.tigmeasurements загружены: {len(oracle_df)} записей")
except Exception as e:
    logging.error(f"Ошибка при чтении текущих объемов из Oracle: {e}")
    print(f"❌ Ошибка при чтении текущих объемов из Oracle: {e}")
    sys.exit(1)

# Приводим названия к верхнему регистру для удобства
oracle_df.columns = [col.upper() for col in oracle_df.columns]
oracle_df['GASNUM'] = oracle_df['GASNUM'].astype(str)

#######################################################################
# 10. Определяем функцию обработки одной комбинации (Prophet)
#######################################################################
def find_similar_days(target_weekday, target_hour, data, window_size=7):
    """Функция для поиска похожих дней (weekday + hour) в исторических данных."""
    similar_days = data[
        (data['weekday'] == target_weekday) &
        (data['ds'].dt.hour == target_hour)
    ]
    if not similar_days.empty:
        rolling_mean = (similar_days['RECEIPTS_VOLUME']
                        .rolling(window=window_size, min_periods=1)
                        .mean()
                        .iloc[-1])
    else:
        rolling_mean = data['RECEIPTS_VOLUME'].mean()
    return rolling_mean

def process_combination(group_key):
    object_code, tank_number, gasnum_str = group_key
    
    # Находим все возможные типы топлива для данного GASNUM
    possible_fuels = [k for k, v in fuel_mapping.items() if v == gasnum_str]
    if not possible_fuels:
        print(f"❌ Не найден вид топлива для GASNUM {gasnum_str}")
        return None
        
    # Пробуем найти строку в city_data для каждого возможного типа топлива
    dead_stock_row = None
    for fuel_name in possible_fuels:
        temp_row = city_data[
            (city_data['OBJECTCODE'] == object_code) &
            (city_data['TANK'] == tank_number) &
            (city_data['FuelType'] == fuel_name)
        ]
        if not temp_row.empty:
            dead_stock_row = temp_row
            break
            
    if dead_stock_row is None or dead_stock_row.empty:
        print(f"❌ Не найден мёртвый остаток для OBJECTCODE={object_code}, TANK={tank_number}, возможные типы топлива: {possible_fuels}")
        return None

    # Получим «человеческое» название топлива (если нужно для отладки)
    fuel_name = None
    for k, v in fuel_mapping.items():
        if v == gasnum_str:
            fuel_name = k
            break

    if not fuel_name:
        print(f"❌ Не найден FuelType для GASNUM={gasnum_str}, пропускаем.")
        return None

    print(f"\n🔄 Обработка: OBJECTCODE={object_code}, TANK={tank_number}, Fuel={fuel_name}")

    # Берём строку из city_data, где хранятся мёртвые остатки
    dead_row = city_data[
        (city_data['OBJECTCODE'] == object_code) &
        (city_data['TANK'] == tank_number) &
        (city_data['FuelType'] == fuel_name)
    ]
    if dead_row.empty:
        print("❌ Не найден мёртвый остаток в CSV для этой комбинации.")
        return None

    dead_stock = dead_row['Volume_liters'].iloc[0]
    print(f"📍 Мёртвый остаток: {dead_stock}")

    # Текущий объём из oracle_df
    current_volume_row = oracle_df[
        (oracle_df['OBJECTCODE'] == object_code) &
        (oracle_df['TANK'] == tank_number) &
        (oracle_df['GASNUM'] == gasnum_str)
    ]
    if current_volume_row.empty:
        # Если нет свежего объёма, берём среднее за последние 30 дней (fallback)
        fallback_query = f"""
        SELECT OBJECTCODE, TANK, GASNUM, AVG(VOLUME) AS VOLUME
        FROM BI.tigmeasurements
        WHERE 
            POSTIMESTAMP < TO_DATE('{FORECAST_DATE} 23:59:59','YYYY-MM-DD HH24:MI:SS')
            AND TRUNC(POSTIMESTAMP) >= TO_DATE('{FORECAST_DATE}','YYYY-MM-DD') - 30
            AND OBJECTCODE='{object_code}'
            AND TANK={tank_number}
            AND GASNUM='{gasnum_str}'
        GROUP BY OBJECTCODE, TANK, GASNUM
        """
        try:
            fallback_df = pd.read_sql(fallback_query, con=oracle_engine)
            if not fallback_df.empty:
                initial_volume = fallback_df['VOLUME'].iloc[0]
                print(f"⚠️ Текущий объём не найден, используем средний: {initial_volume}")
            else:
                print("❌ Нет данных для среднего объёма, пропускаем.")
                return None
        except Exception as e:
            logging.error(f"Ошибка запроса fallback к Oracle: {e}")
            return None
    else:
        initial_volume = current_volume_row['VOLUME'].iloc[0]
        print(f"📊 Текущий объём: {initial_volume}")

    # Забираем исторические продажи
    df_temp = grouped.get_group((object_code, tank_number, gasnum_str)).copy()
    # Суммируем, если вдруг дубликаты по одному часу
    df_temp = df_temp.groupby('ds', as_index=False).agg({
        'RECEIPTS_VOLUME': 'sum', 
        'weekday': 'first'
    })

    # Создаём полный часовой индекс
    df_temp = df_temp.set_index('ds')
    start_date = df_temp.index.min().normalize()
    end_date = df_temp.index.max().normalize() + pd.Timedelta(days=1)
    full_index = pd.date_range(start=start_date, end=end_date, freq='h')

    # Ресемплим
    df_temp = df_temp.reindex(full_index).reset_index()
    df_temp.rename(columns={'index': 'ds'}, inplace=True)
    df_temp['weekday'] = df_temp['weekday'].fillna(df_temp['ds'].dt.weekday)
    df_temp.set_index('ds', inplace=True)

    # Заполнение пропусков
    if df_temp['RECEIPTS_VOLUME'].isnull().any():
        df_temp['RECEIPTS_VOLUME'] = df_temp['RECEIPTS_VOLUME'].interpolate(method='time')
        df_temp['RECEIPTS_VOLUME'] = df_temp['RECEIPTS_VOLUME'].bfill().ffill()

    df_temp.reset_index(inplace=True)
    forecast_date_cutoff = pd.to_datetime(f"{FORECAST_DATE} 00:00:00")
    historical_df = df_temp[df_temp['ds'] < forecast_date_cutoff]

    prophet_df = historical_df[['ds', 'RECEIPTS_VOLUME']].rename(columns={'RECEIPTS_VOLUME': 'y'})
    prophet_df = prophet_df[prophet_df['y'] >= 0]

    if len(prophet_df) < 24:
        print("❌ Недостаточно исторических данных для обучения Prophet.")
        return None

    # Создаём и обучаем модель
    model = Prophet(
        seasonality_mode='additive',
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=False,
        changepoint_prior_scale=0.05,
        seasonality_prior_scale=10.0
    )
    model.add_seasonality(name='weekly', period=7, fourier_order=3)
    model.add_seasonality(name='daily', period=1, fourier_order=5)
    model.add_seasonality(name='hourly', period=24, fourier_order=15)

    try:
        model.fit(prophet_df)
    except Exception as e:
        logging.error(f"Ошибка обучения Prophet: {e}")
        return None

    # Прогноз на FORECAST_HOURS часов (динамически, в зависимости от дня недели)
    forecast_start_date = pd.to_datetime(f"{FORECAST_DATE} 00:00:00")
    forecast_end_date   = forecast_start_date + pd.Timedelta(hours=FORECAST_HOURS)
    future_dates        = pd.date_range(start=forecast_start_date, end=forecast_end_date, freq='h')

    future_df = pd.DataFrame({'ds': future_dates})
    forecast = model.predict(future_df)[['ds','yhat']]
    forecast['yhat'] = forecast['yhat'].clip(lower=0)

    # Скользящее среднее
    forecast['weekday'] = forecast['ds'].dt.weekday
    forecast['hour']    = forecast['ds'].dt.hour
    forecast['rolling_mean'] = forecast.apply(
        lambda row: find_similar_days(row['weekday'], row['hour'], historical_df),
        axis=1
    )

    # Предположим, вы хотите нижнюю границу в 20% от среднего Rolling Mean:
    min_fraction = 0.2
    forecast['combined_forecast'] = 0.3 * forecast['rolling_mean'] + 0.7 * forecast['yhat']

    # Посчитаем некий порог:
    forecast['min_threshold'] = forecast['rolling_mean'] * min_fraction
    forecast['combined_forecast'] = forecast.apply(
        lambda row: max(row['combined_forecast'], row['min_threshold']),
        axis=1
    )

    # Если нужно, умножаем дальше
    forecast['combined_forecast'] = forecast['combined_forecast'] * 1.65

    # Добавим логирование после получения исторических данных
    print(f"📊 Диагностика для {object_code}, tank={tank_number}, fuel={fuel_name}:")
    print(f"   Количество исторических записей: {len(historical_df)}")

    # После прогноза Prophet добавим проверку
    print(f"📈 Прогноз Prophet:")
    print(f"   Min yhat: {forecast['yhat'].min():.2f}")
    print(f"   Max yhat: {forecast['yhat'].max():.2f}")
    print(f"   Mean yhat: {forecast['yhat'].mean():.2f}")

    # После расчета rolling_mean добавим проверку
    print(f"📉 Скользящее среднее:")
    print(f"   Min rolling_mean: {forecast['rolling_mean'].min():.2f}")
    print(f"   Max rolling_mean: {forecast['rolling_mean'].max():.2f}")
    print(f"   Mean rolling_mean: {forecast['rolling_mean'].mean():.2f}")

    # Модифицируем расчет combined_forecast с дополнительными проверками
    forecast['combined_forecast'] = 0.3 * forecast['rolling_mean'] + 0.7 * forecast['yhat']
    
    # Если combined_forecast содержит нули, используем rolling_mean
    zero_forecasts = forecast['combined_forecast'] == 0
    if zero_forecasts.any():
        print(f"⚠️ Обнаружены нулевые прогнозы! Заменяем на rolling_mean")
        forecast.loc[zero_forecasts, 'combined_forecast'] = forecast.loc[zero_forecasts, 'rolling_mean']

    # Проверка после всех корректировок
    print(f"🎯 Итоговый прогноз:")
    print(f"   Min combined_forecast: {forecast['combined_forecast'].min():.2f}")
    print(f"   Max combined_forecast: {forecast['combined_forecast'].max():.2f}")
    print(f"   Mean combined_forecast: {forecast['combined_forecast'].mean():.2f}")

    # Добавим минимальный порог для прогноза
    min_acceptable_forecast = forecast['rolling_mean'].mean() * 0.1  # 10% от среднего
    forecast['combined_forecast'] = forecast['combined_forecast'].clip(lower=min_acceptable_forecast)

    # Считаем изменение объёма
    forecast['cumulative_sales'] = np.cumsum(forecast['combined_forecast'])
    forecast['current_volume']   = initial_volume - forecast['cumulative_sales']

    # Проверяем, когда достигнем мёртвого остатка
    below_dead = forecast[forecast['current_volume'] <= dead_stock]
    if not below_dead.empty:
        first_reach = below_dead.iloc[0]
        deadstock_date = first_reach['ds']
        print(f"⚠️ Deadstock достигнут: {deadstock_date}")
    else:
        deadstock_date = None
        print(f"✅ Deadstock не достигнут в горизонте {FORECAST_HOURS+1} часов.")

    # Формируем DataFrame для вставки
    forecast['objectcode'] = object_code
    forecast['gasnum']     = gasnum_str
    forecast['tank']       = tank_number
    forecast['date_time']  = forecast['ds']
    forecast['forecast_volume_sales'] = forecast['combined_forecast']
    forecast['date_time_deadstock']   = deadstock_date
    forecast['forecast_current_volume'] = forecast['current_volume']

    insert_df = forecast[[
        'objectcode','gasnum','tank','date_time',
        'forecast_volume_sales','date_time_deadstock','forecast_current_volume'
    ]]
    return insert_df


#######################################################################
# 11. Основной блок исполнения + вставка в ord_forecast
#######################################################################
if __name__ == '__main__':
    # Проверим диапазон дат в исходных данных
    print(f"\n📊 Диагностика исходных данных:")
    print(f"   Минимальная дата в данных: {df_all['ds'].min()}")
    print(f"   Максимальная дата в данных: {df_all['ds'].max()}")
    print(f"   Количество уникальных дат: {df_all['ds'].dt.date.nunique()}")
    
    # Проверим данные за последний месяц
    last_month = pd.to_datetime(FORECAST_DATE) - pd.Timedelta(days=30)
    recent_data = df_all[df_all['ds'] >= last_month]
    print(f"\n📈 Статистика за последний месяц:")
    print(f"   Количество записей: {len(recent_data):,}")
    print(f"   Среднее значение продаж: {recent_data['RECEIPTS_VOLUME'].mean():.2f}")
    
    print(f"\n🚀 Начинаем обработку {len(grouped.groups)} комбинаций...")
    
    # Параллельно обрабатываем каждую комбинацию (OBJECTCODE, TANK, GASNUM)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_combination, grouped.groups.keys()))

    insert_dfs = [df for df in results if df is not None]
    if not insert_dfs:
        print("❌ Нет данных для вставки в ord_forecast.")
    else:
        final_insert_df = pd.concat(insert_dfs, ignore_index=True)

        # Формируем временные границы для удаления старых записей
        forecast_date    = pd.to_datetime(FORECAST_DATE)
        forecast_start   = forecast_date.strftime('%Y-%m-%d 00:00:00')
        forecast_end     = (forecast_date + pd.Timedelta(hours=FORECAST_HOURS)).strftime('%Y-%m-%d %H:%M:%S')

        print(f"\n🗑️ Удаление старых прогнозов из ord_forecast...")
        
        # Удаляем старые записи из ord_forecast
        delete_query = f"""
        DELETE FROM ord_forecast
        WHERE
            objectcode IN ({object_codes_str})
            AND gasnum IN ({gasnums_str})
            AND tank IN ({tanks_str})
            AND date_time BETWEEN '{forecast_start}' AND '{forecast_end}'
        """
        
        try:
            # Если у нас простой engine, используем PyODBC
            if hasattr(engine, 'get_conn'):
                conn_temp = engine.get_conn()
                cursor = conn_temp.cursor()
                cursor.execute(delete_query)
                row_count = cursor.rowcount
                conn_temp.commit()
                cursor.close()
                conn_temp.close()
                print(f"✅ Удалено {row_count} старых записей из ord_forecast.")
            else:
                # Стандартный SQLAlchemy
                with engine.begin() as connection:
                    result = connection.execute(text(delete_query))
                    print(f"✅ Удалено {result.rowcount} старых записей из ord_forecast.")
        except Exception as e:
            logging.error(f"Ошибка при удалении старых записей из ord_forecast: {e}")
            print(f"❌ Ошибка при удалении старых записей из ord_forecast: {e}")
            sys.exit(1)

        print(f"\n💾 Сохранение новых прогнозов в ord_forecast...")
        
        # Вставляем новые прогнозы с retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Если у нас простой engine, используем PyODBC
                if hasattr(engine, 'get_conn'):
                    conn_temp = engine.get_conn()
                    final_insert_df.to_sql('ord_forecast', con=conn_temp, if_exists='append', index=False)
                    conn_temp.close()
                else:
                    # Стандартный SQLAlchemy
                    final_insert_df.to_sql('ord_forecast', con=engine, if_exists='append', index=False)
                    
                print(f"\n✅ Новые прогнозные данные успешно вставлены в ord_forecast!")
                print(f"📊 Количество записей: {len(final_insert_df):,}")
                print(f"📅 Период: {forecast_start} - {forecast_end}")
                break
                
            except Exception as e:
                print(f"❌ Попытка {attempt + 1} вставки не удалась: {e}")
                if attempt < max_retries - 1:
                    print("⏳ Ожидание 10 секунд перед повтором...")
                    time.sleep(10)
                else:
                    logging.error(f"Ошибка при вставке данных в ord_forecast: {e}")
                    print(f"💥 Критическая ошибка при вставке данных в ord_forecast: {e}")

    # Закрываем коннекты
    try:
        if hasattr(oracle_engine, 'dispose'):
            oracle_engine.dispose()
        if hasattr(engine, 'dispose'):
            engine.dispose()
        conn.close()
        print("\n🔒 Все подключения закрыты")
    except Exception as e:
        print(f"⚠️ Ошибка при закрытии подключений: {e}")
    
    print("\n🎉 Все операции завершены!")
