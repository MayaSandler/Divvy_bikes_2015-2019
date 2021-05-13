# import pandas as pd
import numpy as np
import time
import math
import datetime as dt
import psycopg2     
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 


def create_database_divvy(cur):     # Create database
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)    # Connect to PostgreSQL DBMS
    name_Database   = "divvy"


def create_tables(cur):             # Create tables
    """ create tables in the PostgreSQL database"""
    commands = (
        """DROP TABLE IF EXISTS stations;"""
        """CREATE TABLE stations (
            id INT,
            station_id INT,
            name VARCHAR(250),
            city VARCHAR(25),
            latitude decimal(10,8) NOT NULL,
            longitude decimal(10,8) NOT NULL,
            dpcapacity INT NOT NULL,
            online_date DATE NOT NULL,
            UNIQUE (station_id)
        ); 
        """
        ,
        """DROP TABLE IF EXISTS trips;"""
        """CREATE TABLE trips (
            id SERIAL,
            trip_id INT NOT NULL,
            start_time TIMESTAMP NOT NULL, 
            end_time TIMESTAMP NOT NULL, 
            bikeid INT,
            tripduration_secconds INT, 
            from_station_id INT, 
            from_station_name VARCHAR(250), 
            to_station_id INT, 
            to_station_name VARCHAR(250), 
            usertype VARCHAR(250), 
            gender VARCHAR(25) DEFAULT 'Unknown', 
            birthyear INT, 
            start_year INT, 
            age decimal(5,1), 
            revenue decimal(10,1),
            PRIMARY KEY (trip_id),
            FOREIGN KEY (from_station_id) REFERENCES stations (station_id) ON DELETE CASCADE,
            FOREIGN KEY (to_station_id) REFERENCES stations (station_id) ON DELETE CASCADE 
        );
        """
    )
    for command in commands:
        cur.execute(command)
    print('finished creating tables...')


def insert_data(cur):       # Insert Data To Tables
    trips = pd.read_csv('C:/.../clean_trips_2015_2019.csv')
    trips = trips.rename(columns={trips.columns[0]:'id'})    
    
    #Insert trips table data
    print('Inserting trip data to Postgres....')
    for i,row in trips.iterrows():
        trips_sql = "INSERT INTO trips(id, trip_id, start_time, end_time, bikeid, \
            tripduration_secconds, from_station_id, from_station_name, to_station_id, \
            to_station_name, usertype, gender, birthyear, start_year, age, revenue)\
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cur.execute(trips_sql, tuple(row))  
        conn.commit()  
    print('Finished inserting trips data to Postgres')


#######  Run  #######
if __name__ == "__main__":
    try:
        # Establishing the connection
        conn = psycopg2.connect(
        host='localhost',
        database='divvy',
        user='postgres',
        password='...')

        # Creating a cursor object
        cur = conn.cursor()
        
        # #Create database
        create_database_divvy(cur)

        # # Create tables
        create_tables(cur)

        # Insert data to Postgre database
        insert_data(cur)
        
        # Close communication & commit changes
        cur.close()             
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if conn is not None:
            conn.close()