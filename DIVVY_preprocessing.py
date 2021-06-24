import pandas as pd
import numpy as np
import time
import math
import datetime as dt


def preprocessing_stations():
    
    print('******** station 2015 *********')
    col_list_1 = ['id', 'name', 'latitude', 'longitude', 'dpcapacity']      # not include 'landmark' column
    stations_2015 = pd.read_csv('Divvy_Stations_2015.csv', usecols = col_list_1)
    stations_2015['online_date'] = '01/01/2015'
    stations_2015['online_date'] = pd.to_datetime(stations_2015['online_date'])
    stations_2015['city'] = None
    stations_2015 = stations_2015[['id', 'name', 'city', 'latitude', 'longitude', 'dpcapacity', 'online_date']]        

    print('******* station 2016_1 *********')
    stations_2016_1 = pd.read_csv('Divvy_Stations_2016_Q1Q2.csv')
    stations_2016_1['online_date'] = pd.to_datetime(stations_2016_1['online_date'])
    stations_2016_1['city'] = None
    stations_2016_1 = stations_2016_1[['id', 'name', 'city', 'latitude', 'longitude', 'dpcapacity', 'online_date']]          

    print('******* station 2016_2 *********')
    stations_2016_2 = pd.read_csv('Divvy_Stations_2016_Q3.csv')
    stations_2016_2['online_date'] = pd.to_datetime(stations_2016_2['online_date'])
    stations_2016_2['city'] = None
    stations_2016_2 = stations_2016_2[['id', 'name', 'city', 'latitude', 'longitude', 'dpcapacity', 'online_date']]          

    print('******* station 2016_3 *********')
    stations_2016_3 = pd.read_csv('Divvy_Stations_2016_Q4.csv')
    stations_2016_3['online_date'] = pd.to_datetime(stations_2016_3['online_date'])
    stations_2016_3['city'] = None
    stations_2016_3 = stations_2016_3[['id', 'name', 'city', 'latitude', 'longitude', 'dpcapacity', 'online_date']]          

    print('******* station 2017_1 ********')
    stations_2017_1 = pd.read_csv('Divvy_Stations_2017_Q1Q2.csv')
    stations_2017_1['online_date'] = pd.to_datetime(pd.to_datetime(stations_2017_1['online_date']).dt.date)

    print('******* station 2017_2 ********')
    col_list = ['id', 'name', 'city', 'latitude', 'longitude', 'dpcapacity', 'online_date'] # not include 'Unnamed: 7' column
    stations_2017_2 = pd.read_csv('Divvy_Stations_2017_Q3Q4.csv', usecols = col_list)
    stations_2017_2['online_date'] = pd.to_datetime(pd.to_datetime(stations_2017_2['online_date']).dt.date)

    # Union all stations data
    stations_2015_2019 = pd.concat([stations_2015, stations_2016_1, stations_2016_2, \
        stations_2016_3, stations_2017_1, stations_2017_2], ignore_index=True)

    # Sort stations so the updated data will be on top  
    sorted_stations = stations_2015_2019.sort_values(by=['city','id','online_date'], \
        ascending=[True, True, False], na_position='last')

    # Drop duplicate stations
    clean_stations_2015_2019 = sorted_stations.drop_duplicates(subset=['id'], \
        keep='first', ignore_index=False)

    print(f'Total stations data: {stations_2015_2019.shape[0]} rows')
    print(f'Unique stations data: {clean_stations_2015_2019.shape[0]} rows')
    print(f'Null cities count: {clean_stations_2015_2019["city"].isna().sum()}')

    # Drop old index
    stations = clean_stations_2015_2019.reset_index(drop = True, inplace=False)
    clean_stations__2015_2019 = stations.rename(columns={'id':'station_id'})    

    print('Creating a list of stations...')
    stations_list = clean_stations__2015_2019.station_id.unique()

    # Save to csv file
    clean_stations__2015_2019.to_csv('C:/Users/maya/clean_stations_2015_2019.csv')
    print('***** Finished preprocessing stations dataset ******')

    return stations_list



def preprocessing_trips(stations_list):
    print(' Reading trips data...')
    starttime = time.time()

    #2015
    trips_2015_q1 = pd.read_csv('Divvy_Trips_2015-Q1.csv') 
    trips_2015_q2 = pd.read_csv('Divvy_Trips_2015-Q2.csv') 
    trips_2015_07 = pd.read_csv('Divvy_Trips_2015_07.csv') 
    trips_2015_08 = pd.read_csv('Divvy_Trips_2015_08.csv') 
    trips_2015_09 = pd.read_csv('Divvy_Trips_2015_09.csv') 
    trips_2015_q4 = pd.read_csv('Divvy_Trips_2015_Q4.csv') 

    trips2015 = pd.concat([trips_2015_q1, trips_2015_q2, trips_2015_07, trips_2015_08, trips_2015_09, trips_2015_q4], ignore_index=True)
    trips2015.iloc[:, 1:3] = trips2015.iloc[:, 1:3].apply(pd.to_datetime)
    trips_2015 = trips2015.rename(columns={'stoptime':'end_time', 'starttime':'start_time'})
    print('**** finished 2015 trips ****')

    #2016
    trips_2016_q1 = pd.read_csv('Divvy_Trips_2016_Q1.csv') 
    trips_2016_04 = pd.read_csv('Divvy_Trips_2016_04.csv') 
    trips_2016_05 = pd.read_csv('Divvy_Trips_2016_05.csv') 
    trips_2016_06 = pd.read_csv('Divvy_Trips_2016_06.csv') 
    trips_2016_q3 = pd.read_csv('Divvy_Trips_2016_Q3.csv') 
    trips_2016_q4 = pd.read_csv('Divvy_Trips_2016_Q4.csv') 
    trips2016 = pd.concat([trips_2016_q1, trips_2016_04, trips_2016_05, trips_2016_06, trips_2016_q3, trips_2016_q4], ignore_index=True)
    trips2016.iloc[:, 1:3] = trips2016.iloc[:, 1:3].apply(pd.to_datetime)
    trips_2016 = trips2016.rename(columns={'stoptime':'end_time', 'starttime':'start_time'})  
    print('**** finished 2016 trips ****')

    #2017
    trips_2017_q1 = pd.read_csv('Divvy_Trips_2017_Q1.csv') 
    trips_2017_q2 = pd.read_csv('Divvy_Trips_2017_Q2.csv') 
    trips_2017_q3 = pd.read_csv('Divvy_Trips_2017_Q3.csv') 
    trips_2017_q4 = pd.read_csv('Divvy_Trips_2017_Q4.csv') 
    trips_2017 = pd.concat([trips_2017_q1, trips_2017_q2, trips_2017_q3, trips_2017_q4], ignore_index=True)
    trips_2017.iloc[:, 1:3] = trips_2017.iloc[:, 1:3].apply(pd.to_datetime)
    print('**** finished 2017 trips ****')

    # 2018
    trips2018_q1 = pd.read_csv('Divvy_Trips_2018_Q1.csv') 
    trips_2018_q1 = trips2018_q1.rename(columns={'01 - Rental Details Rental ID':'trip_id', '01 - Rental Details Local Start Time':'start_time', \
        '01 - Rental Details Local End Time':'end_time' , '01 - Rental Details Bike ID':'bikeid' , '01 - Rental Details Duration In Seconds Uncapped':'tripduration' , \
        '03 - Rental Start Station ID':'from_station_id' , '03 - Rental Start Station Name':'from_station_name' , '02 - Rental End Station ID':'to_station_id' , \
        '02 - Rental End Station Name':'to_station_name' , 'User Type':'usertype' , 'Member Gender':'gender' , '05 - Member Details Member Birthday Year':'birthyear' })  
    trips_2018_q2 = pd.read_csv('Divvy_Trips_2018_Q2.csv') 
    trips_2018_q3 = pd.read_csv('Divvy_Trips_2018_Q3.csv') 
    trips_2018_q4 = pd.read_csv('Divvy_Trips_2018_Q4.csv') 
    trips_2018 = pd.concat([trips_2018_q1, trips_2018_q2, trips_2018_q3, trips_2018_q4], ignore_index=True)
    trips_2018['tripduration'] = trips_2018['tripduration'].str.replace(',', '')            
    trips_2018['tripduration'] = trips_2018['tripduration'].astype(float)       
    trips_2018['tripduration'] = trips_2018['tripduration'].apply(np.int64)  
    trips_2018.iloc[:, 1:3] = trips_2018.iloc[:, 1:3].apply(pd.to_datetime)
    print('**** finished 2018 trips ****')

    #2019
    trips_2019_q1 = pd.read_csv('Divvy_Trips_2019_Q1.csv') 
    trips_2019q2 = pd.read_csv('Divvy_Trips_2019_Q2.csv') 
    trips_2019_q2 = trips_2019q2.rename(columns={'01 - Rental Details Rental ID':'trip_id', '01 - Rental Details Local Start Time':'start_time', \
        '01 - Rental Details Local End Time':'end_time' , '01 - Rental Details Bike ID':'bikeid' , '01 - Rental Details Duration In Seconds Uncapped':'tripduration' , \
        '03 - Rental Start Station ID':'from_station_id' , '03 - Rental Start Station Name':'from_station_name' , '02 - Rental End Station ID':'to_station_id' , \
        '02 - Rental End Station Name':'to_station_name' , 'User Type':'usertype' , 'Member Gender':'gender' , '05 - Member Details Member Birthday Year':'birthyear' })  
    trips_2019_q3 = pd.read_csv('Divvy_Trips_2019_Q3.csv') 
    trips_2019_q4 = pd.read_csv('Divvy_Trips_2019_Q4.csv') 
    trips_2019 = pd.concat([trips_2019_q1, trips_2019_q2, trips_2019_q3, trips_2019_q4], ignore_index=True)   
    trips_2019['tripduration'] = trips_2019['tripduration'].str.replace(',', '')            
    trips_2019['tripduration'] = trips_2019['tripduration'].astype(float)       
    trips_2019['tripduration'] = trips_2019['tripduration'].apply(np.int64)  
    trips_2019.iloc[:, 1:3] = trips_2019.iloc[:, 1:3].apply(pd.to_datetime)
    print('**** finished 2019 trips ****')

    # Union all years:
    print('Union trips......')
    trips_2015_2019 = pd.concat([trips_2015, trips_2016, trips_2017, trips_2018, trips_2019], ignore_index=True)
    print(f'Total trips data with duplicates is: {trips_2015_2019.shape[0]} rows')

    # Cleaning the table for duplicates. keep current info:
    clean_trips2015_2019 = trips_2015_2019.drop_duplicates(subset=['trip_id'], keep='first')
    
    #Include only trips with to and from station id that exist in stations dataset
    print('Subset trips dataset to match list of stations...')
    temp = clean_trips2015_2019[(clean_trips2015_2019['from_station_id'].isin(stations_list)) & clean_trips2015_2019['to_station_id'].isin(stations_list)]
    clean_trips_2015_2019 = temp.rename(columns={'tripduration':'tripduration_secconds'})

    print('Cleaned trips data without duplicates is:  ', clean_trips_2015_2019.shape[0])    
    exectime = (time.time() - starttime)
    print(f'Execution cleaning data time was {exectime} sec')


    #### calculated new column age:  
    starttime = time.time()

    # Create "start_year" column and transfer birthyear to from float to integer. 
    clean_trips_2015_2019['start_year'] = pd.DatetimeIndex(clean_trips_2015_2019['start_time']).year
    clean_trips_2015_2019['start_year'] = clean_trips_2015_2019['start_year'].astype(float)       
    clean_trips_2015_2019['start_year'] = clean_trips_2015_2019['start_year'].apply(np.int64)  
    clean_trips_2015_2019['birthyear'] = clean_trips_2015_2019['birthyear'].fillna(0).astype(int)
    
    # Calculated "age" column
    print("*** Calculating user's age... ***")
    age_calc = []
    for i in range(clean_trips_2015_2019["trip_id"].count()):
        birthyear = clean_trips_2015_2019.iloc[i, 11] 
        tripyear = clean_trips_2015_2019.iloc[i, 12]
        if birthyear == 0: 
            age_calc.append(None)       # if birthyear is NaN in original 'birthyear' column -> null          
        else:
            age_calc.append(tripyear - birthyear) 

    print('** Finished age calculations')
    clean_trips_2015_2019['age'] = age_calc
    exectime = (time.time() - starttime)
    print(f'Execution new column "age" time was {exectime} sec') 


    #### calculated new column revenue:  
    print("*** Calculating revenue... ***")
    starttime = time.time()

    RIDE_45_MIN = 45*60     # Subscriber 45 min rides
    RIDE_3_HOUR = 3*60*60   # Customer 3 hour rides
    SEC_TO_HOUR = 60*60     # Convert from seconds to hours

    revenue_calc = []
    for i in range(clean_trips_2015_2019["trip_id"].count()):
        val = clean_trips_2015_2019.iloc[i, 9]
        tripduration_seconds = clean_trips_2015_2019.iloc[i, 4]
        if type(tripduration_seconds) == str: 
                print(tripduration_seconds)
        if val == "Customer": 
            if tripduration_seconds /SEC_TO_HOUR > 3:
                r = 15+ 0.2 * (tripduration_seconds - RIDE_3_HOUR)
                revenue_calc.append(r)
            else:
                revenue_calc.append(15)
        elif val == "Subscriber":
            if tripduration_seconds > RIDE_45_MIN:
                r = 9 + 0.15 * (tripduration_seconds - RIDE_45_MIN)
                revenue_calc.append(r)  
            else:
                revenue_calc.append(9) 
        else:
            revenue_calc.append(0) 

    print('** Finished revenue calculations')
    clean_trips_2015_2019['revenue'] = revenue_calc
    exectime = (time.time() - starttime)
    print(f'Execution new column "revenue" time was {exectime} sec') 

    # Drop old index
    trips = clean_trips_2015_2019.reset_index(drop = True, inplace=False)
    clean_trips_2015__2019 = trips.reset_index().rename(columns={'index':'id'})

    # Save to csv file
    clean_trips_2015__2019.head(5).to_csv('C:/Users/maya/trips_test.csv', index=False)
    clean_trips_2015__2019.to_csv('C:/Users/maya/clean_trips_2015_2019.csv', index=False)
    print('***** Finished preprocessing trips dataset ******')


#######  Run  #######
if __name__ == "__main__":
    stations_list = preprocessing_stations()
    preprocessing_trips(stations_list)