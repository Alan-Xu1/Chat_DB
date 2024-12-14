from sqlalchemy import create_engine, text
import pymysql

connection_parameters = {
    "host": "chatdb.clwu228s6zcd.us-west-2.rds.amazonaws.com",
    "user": "admin",
    "password": "DSCI-551",
    "port": 3306
}

def df_to_sql(dfs, table_name, db_name):
    base_connection = f"mysql+pymysql://{connection_parameters['user']}:{connection_parameters['password']}@{connection_parameters['host']}:{connection_parameters['port']}"
    engine = create_engine(base_connection)
    with engine.connect() as connection:
        connection.execute(text(f"DROP DATABASE IF EXISTS {db_name}"))
        connection.execute(text(f"CREATE DATABASE {db_name}"))
        connection.commit()
    engine = create_engine(f"{base_connection}/{db_name}")
    index = 0
    for i in dfs:
        i.to_sql(name=table_name[index],
                 con=engine,
                 if_exists='replace',
                 index=False)
        index = index + 1
    engine.dispose()