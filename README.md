# Divvy_bikes_2015-2019
### Preprocessing, importing to Postgres and analysis of Divvy bikes' data between 2015-2019 

DIVVY bike's is a bike rental service located around the Chicago area, and lets you pick up a bike at one of their hundreds of stations, bike to your destination and return the bike to any one of their stations. Data was downloaded (open resource) from DIVVY website (https://www.divvybikes.com/), including user types, current pricing policy, bike docking station locations according to date, and trips data. 

I downloaded DIVVY's trip data, added pricing rules, read and cleaned the data via a Python code, validate and analyzed the data via PostgreSQL and visualized it using Tableau Public. 

* DIVVY data preprocessing was done via a Python scrips - DIVVY_preprocessing.py
* Creating the database nad tables, and inserting the data was done via a python script as well - import_divvy_to_postgres.py
* Queries for data analytics were done via a PostgreSQL - postgres.sql

My data preprocessing, results and insights can be found here: https://mayasandler.wixsite.com/mysite/divvy-bikes.
