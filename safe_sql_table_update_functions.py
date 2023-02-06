from sqlalchemy import create_engine, event
import pandas as pd
from datetime import datetime

# Create Engines

enginecredentials='postgresql://<username>:<password>'+'<servername>:<port>/<databasename>'

read_schema='live'
write_schema='data_warehouse'
archive_schema='archive'

class engine:
    
    def set_search_path(self):
        @event.listens_for(self.engine, "connect", insert=True)
        def set_search_path(dbapi_connection, connection_record):
            existing_autocommit = dbapi_connection.autocommit
            dbapi_connection.autocommit = True
            cursor = dbapi_connection.cursor()
            cursor.execute("SET SESSION search_path='%s'" % self.schema)
      
    # init method or constructor
    def __init__(self,enginecredentials, schema):
        self.enginecredentials = enginecredentials
        self.engine=create_engine(self.enginecredentials)
        self.schema = schema
        self.set_search_path()
            
    def change_search_path(self,schema):
        self.schema=schema
        self.engine.dispose()
        self.engine=create_engine(self.enginecredentials)
        self.set_search_path()
        
read=engine(enginecredentials,read_schema)
write=engine(enginecredentials,write_schema)
archive=engine(enginecredentials,archive_schema)

# Create Safe Table Update Functions

def update_historical_table(archive_engine,write_engine,table_name,table_version):
    # Set Table Locations
    table=table_name+table_version
    updating_table='updating_'+table_name+table_version
    # Update Tables
    try:
        archive_engine.engine.execute(f"""drop table if exists {updating_table}""")
        if (write_engine.engine.execute(f"""SELECT EXISTS (select * from information_schema.tables where table_name = '{table_name}');""").fetchone()[0] == True):
            archive_engine.engine.execute(f"""create table {updating_table} as select * from {write_engine.schema+'.'+table_name}""")
            if (archive_engine.engine.execute(f"""SELECT COUNT(*) from {updating_table};""").fetchone()[0] > 0):
                archive_engine.engine.execute(f"""drop table if exists {table}""")
                archive_engine.engine.execute(f"""alter table {updating_table} rename to {table}""")
                print(f"""{archive_engine.schema+'.'+table_name} table updated successfully at {datetime.now()}""")
            else:
                print(f"""{table} failed to update as {updating_table} is empty""")
    except Exception as e:
        print(e)
        print(f"""{table} failed to update""")

def update_table(read_engine,write_engine,table_name,table_version,sql_statement):
    # Set Table Locations
    table=table_name+table_version
    updating_table='updating_'+table_name+table_version
    sql_statement = sql_statement.format(write_engine.schema+'.'+updating_table)
    # Update Tables
    try:
        write_engine.engine.execute(f"""drop table if exists {updating_table}""")
        read_engine.engine.execute(sql_statement)
        if (write_engine.engine.execute(f"""SELECT COUNT(*) from {updating_table};""").fetchone()[0] > 0):
            write_engine.engine.execute(f"""drop table if exists {table}""")
            write_engine.engine.execute(f"""alter table {updating_table} rename to {table}""")
            print(f"""{write_engine.schema+'.'+table_name} table updated successfully at {datetime.now()}""")
        else:
            print(f"""{table} failed to update as {updating_table} is empty""")
    except Exception as e:
        print(e)
        print(f"""{table} failed to update""")