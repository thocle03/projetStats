import os
import pandas as pd
import json
import mysql.connector
import numpy as np

def infer_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DECIMAL(10, 2)" 
    else:
        return "VARCHAR(255)" 

def generate_sql_create_table(file_name, df):
    table_name = os.path.splitext(file_name)[0].lower()
    sql_columns = []

    for col in df.columns:
        sql_type = infer_sql_type(df[col].dtype)
        column_definition = f"{col.lower()} {sql_type}"
        sql_columns.append(column_definition)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(sql_columns)}
    );
    """
    return create_table_query

def create_json_queries(directory):
    sql_queries = {}
    for file_name in os.listdir(directory):
        if file_name.endswith('.csv'):
            file_path = os.path.join(directory, file_name)
            try:
                df = pd.read_csv(file_path, low_memory=False)

                create_table_query = generate_sql_create_table(file_name, df)

                sql_queries[file_name] = create_table_query

            except Exception as e:
                print(f"Erreur lors de la lecture de {file_name} : {e}")

    with open('create_tables.json', 'w') as f:
        json.dump(sql_queries, f, indent=4)

    print("Les requêtes SQL ont été générées et sauvegardées dans 'create_tables.json'.")

def connect_to_db():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
    )
    
def create_database_if_not_exists(connection, db_name):
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.close()
    connection.database = db_name

def create_tables_from_json(json_file, connection):
    with open(json_file, 'r') as f:
        sql_queries = json.load(f)

    cursor = connection.cursor()
    for table, create_query in sql_queries.items():
        try:
            cursor.execute(create_query)
            print(f"Table '{table}' créée avec succès.")
        except mysql.connector.Error as err:
            print(f"Erreur lors de la création de la table '{table}': {err}")
    connection.commit()

def insert_data_from_csv(file_name, df, table_name, connection, batch_size=1000):
    df.columns = [col.lower() for col in df.columns]
    df = df.replace({np.nan: None})

    cursor = connection.cursor()

    cols = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    data = df.values.tolist()

    for i in range(0, len(data), batch_size):
        batch_data = data[i:i+batch_size]
        try:
            cursor.executemany(insert_query, batch_data)
        except mysql.connector.Error as err:
            print(f"Erreur lors de l'insertion dans la table '{table_name}': {err}")
            for row in batch_data:
                print(f"Ligne problématique : {row}")
                break
            raise Exception(f"Programme stoppé à cause d'une erreur dans la table '{table_name}'")

    connection.commit()

directory = os.path.join(os.getcwd(), 'data')

create_json_queries(directory)

connection = connect_to_db()
create_database_if_not_exists(connection, 'statsdb18')

create_tables_from_json('create_tables.json', connection)

for file_name in os.listdir(directory):
    if file_name.endswith('.csv'):
        file_path = os.path.join(directory, file_name)
        table_name = os.path.splitext(file_name)[0].lower()
        try:
            df = pd.read_csv(file_path, low_memory=False)
            insert_data_from_csv(file_name, df, table_name, connection)
        except Exception as e:
            print(f"Erreur lors du traitement de {file_name} : {e}")

connection.close()
print("Processus terminé.")
