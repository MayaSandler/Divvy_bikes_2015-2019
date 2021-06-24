-- Revenue by product along years - revenueVsYear&Usertype.csv
SELECT 
	start_year,
	usertype,
	SUM(revenue) AS revenue
FROM trips
WHERE usertype != 'Dependent'
GROUP BY (start_year, usertype)
ORDER BY start_year, revenue DESC;


-- Number of ALL ride time acording to year and product - Num&time_trips&ratioVsYear&Usertype.csv:	
SELECT 
	start_year,
	usertype,
	SUM(tripduration_secconds)/3600 AS total_trip_duration_hours,
FROM trips
WHERE usertype != 'Dependent'
GROUP BY (start_year, usertype)
ORDER BY start_year, total_num_trips DESC;


-- To understand the tripduration distribution, I binned the tripduration data:
SELECT
	start_year, 
	usertype, 
	CASE 
		WHEN tripduration_secconds <= 45*60 THEN '<45min'
		WHEN tripduration_secconds <= 3*60*60 THEN '45min-3h'
		WHEN tripduration_secconds <= 5*60*60 THEN '3h-5h'
		WHEN tripduration_secconds <= 12*60*60 THEN '5h-12h'
		WHEN tripduration_secconds <= 24*60*60 THEN '12h-24h'
		ELSE '>24h'
	END AS tripduration_bins,
	COUNT(trip_id)
FROM trips
WHERE usertype != 'Dependent'
GROUP BY start_year, usertype, tripduration_bins;


------------------------------------------------------------------------
SELECT max(tripduration_secconds)/3600, max(revenue) FROM trips
WHERE usertype != 'Dependent';
------------------------------------------------------------------------


-- Number of ALL trips and usage time acording to year and product - Num&time_trips&ratioVsYear&Usertype.csv:	
SELECT 
	start_year,
	usertype,
	count(trip_id) AS total_num_trips,
	ROUND(((SUM(tripduration_secconds)/3600):: NUMERIC / count(trip_id)), 2) AS use_hour_per_trip
FROM trips
WHERE usertype != 'Dependent'
GROUP BY (start_year, usertype)
ORDER BY start_year, total_num_trips DESC;



-- Revenue by year AFTER placing a max bound of trip duration of 24 hours - FixedRevenueVsYear&Usertype_24h_max.csv :
WITH max_trip_revenue AS (
	SELECT 
		trip_id,
		start_year,
		usertype,
		CASE
			WHEN tripduration_secconds <= 86400 THEN revenue	-- 24 hours
			ELSE 250  
		END AS new_revenue
	FROM trips
	WHERE usertype != 'Dependent'
)

SELECT 
	start_year,
	usertype,
	SUM(new_revenue) AS revenue
FROM max_trip_revenue
GROUP BY (start_year, usertype)
ORDER BY start_year, revenue DESC;



-- Revenue by day of the week AFTER placing a max bound of trip duration of 24 hours - RevenueVsWeekday_24h_max.csv :
WITH max_trip_revenue AS (
	SELECT 
		trip_id,
		CASE
			WHEN tripduration_secconds <= 86400 THEN revenue	-- 24 hours
			ELSE 250  
		END AS new_revenue
	FROM trips
	WHERE usertype != 'Dependent'
)

SELECT 
	CASE
          WHEN day = 1 THEN 'Monday'
          WHEN day = 2 THEN 'Tuesday'
          WHEN day = 3 THEN 'Wednesday'
          WHEN day = 4 THEN 'Thursday'
          WHEN day = 5 THEN 'Friday'
          WHEN day = 6 THEN 'Saturday'
		  WHEN day = 7 THEN 'Sunday'
    END day_name,
	usertype,
	Revenue
FROM (
	SELECT
		EXTRACT(ISODOW FROM start_time) AS day,		-- ISODOW: Monday = 1
		t.usertype,
		(SUM(mtr.new_revenue)) AS Revenue
	FROM trips t
	LEFT JOIN max_trip_revenue mtr 
		ON t.trip_id = mtr.trip_id
	WHERE t.usertype != 'Dependent'
	GROUP BY 1, 2
) AS temp
ORDER BY 2	;


-- AVG trip duration by year AFTER placing a max bound of trip duration of 24 hours - avg_tripduration_24h_max.csv :
WITH max_trip_duration AS (
	SELECT 
		trip_id,
		start_year,
		usertype,
		CASE
			WHEN tripduration_secconds <= 86400 THEN tripduration_secconds	-- 24 hours
			ELSE 86400
		END AS new_trip_duration
	FROM trips
	WHERE usertype != 'Dependent'
)
SELECT 
	start_year,
	usertype,
	ROUND(AVG(new_trip_duration)/3600, 2) AS AVG_trip_duration_h
FROM max_trip_duration
GROUP BY (start_year, usertype)
ORDER BY start_year, usertype;


-- Number of trips and usage time acording to day of the week and product - numtrips&triptime_per_dayofweek.csv:	
WITH max_trip_duration AS (
	SELECT 
		trip_id,
		start_year,
		usertype,
		CASE
			WHEN tripduration_secconds <= 86400 THEN tripduration_secconds	-- 24 hours
			ELSE 86400
		END AS new_trip_duration
	FROM trips
	WHERE usertype != 'Dependent'
)
SELECT 
	CASE
          WHEN day = 1 THEN 'Monday'
          WHEN day = 2 THEN 'Tuesday'
          WHEN day = 3 THEN 'Wednesday'
          WHEN day = 4 THEN 'Thursday'
          WHEN day = 5 THEN 'Friday'
          WHEN day = 6 THEN 'Saturday'
		  WHEN day = 7 THEN 'Sunday'
    END day_name,
	usertype,
	total_num_trips,
	avg_trip_duration_h
FROM (
	SELECT
		EXTRACT(ISODOW FROM start_time) AS day,		-- ISODOW: Monday = 1
		t.usertype,
		count(mtd.trip_id) AS total_num_trips,
		ROUND(((SUM(mtd.new_trip_duration)/3600)::NUMERIC / count(mtd.trip_id)),2) AS avg_trip_duration_h
	FROM trips t
	LEFT JOIN max_trip_duration mtd 
		ON t.trip_id = mtd.trip_id
	WHERE t.usertype != 'Dependent'
	GROUP BY 1, 2
	ORDER BY 1
) AS temp
ORDER BY 2	;



-- The most popular stations - 20_popular_stations.csv
SELECT
	s.name AS station_name,
	s.city,
	s.longitude,
	s.latitude,
	COUNT(t.trip_id) AS total_num_trips,
	
	() OVER (ORDER BY COUNT(t.trip_id) DESC) AS rank_by_num_trips
FROM trips t	
JOIN stations s
	on t.from_station_name = s.name
WHERE usertype != 'Dependent'
GROUP BY t.usertype, s.name, s.city, s.longitude, s.latitude
ORDER BY rank_by_num_trips ;


-- The most popular stations by product -  20_popular_stations_byUser
SELECT
	s.name AS station_name,
	s.city,
	s.longitude,
	s.latitude,
	t.usertype,
	--t.gender,
	COUNT(t.trip_id) AS total_num_trips,
	RANK() OVER (PARTITION BY t.usertype ORDER BY COUNT(t.trip_id) DESC) AS rank_by_num_trips
FROM trips t	
JOIN stations s
	on t.from_station_name = s.name
WHERE usertype != 'Dependent'
GROUP BY t.usertype, s.id, s.name, s.city, s.longitude, s.latitude
ORDER BY rank_by_num_trips ;


-- To understand the age distribution, gender and main station of the users, I binned the tripduration age data  - age&gender_trips&stations.csv:
SELECT
	t.usertype, 
	t.gender,
	CASE 
		WHEN t.age <= 17 THEN 'Minor'
		WHEN t.age <= 28 THEN '18-28'
		WHEN t.age <= 29 THEN '19-29'
		WHEN t.age <= 39 THEN '30-39'
		WHEN t.age <= 49 THEN '40-49'
		WHEN t.age <= 59 THEN '50-59'
		WHEN t.age <= 69 THEN '60-69'
		WHEN t.age <= 79 THEN '70-79'
		WHEN t.age <= 89 THEN '80-89'
		WHEN t.age <= 99 THEN '90-99'
		When t.age >= 100 THEN 'Unknown' -- Fake age
		ELSE 'Unknown' -- Null Values, like Day Pass users
	END AS age_bins,
	t.from_station_id,
	s.name,
	s.city, 
	s.latitude,
	s.longitude,
	s.dpcapacity,
	COUNT(t.trip_id)
	
    
-- 	RANK() OVER (PARTITION BY t.usertype ORDER BY COUNT(t.trip_id) DESC) AS rank_by_num_trips
FROM trips t
LEFT JOIN stations s
	ON t.from_station_id = s.station_id
WHERE usertype != 'Dependent'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;




-- Hour of trips in each product and gender - Product_GenderVsHour.csv
SELECT
	usertype,
	CASE
          WHEN EXTRACT(ISODOW FROM start_time) = 1 THEN 'Monday'  -- ISODOW: Monday = 1
          WHEN EXTRACT(ISODOW FROM start_time) = 2 THEN 'Tuesday'
          WHEN EXTRACT(ISODOW FROM start_time) = 3 THEN 'Wednesday'
          WHEN EXTRACT(ISODOW FROM start_time) = 4 THEN 'Thursday'
          WHEN EXTRACT(ISODOW FROM start_time) = 5 THEN 'Friday'
          WHEN EXTRACT(ISODOW FROM start_time) = 6 THEN 'Saturday'
		  WHEN EXTRACT(ISODOW FROM start_time) = 7 THEN 'Sunday'
    END day_name,
	TO_CHAR(start_time, 'HH') ||' '|| TO_CHAR(start_time, 'am') AS hour,
	gender,
	COUNT(trip_id)
FROM trips
WHERE usertype != 'Dependent'
GROUP BY 1, 2, 3, 4;



-- Trip Duration according to gender in Annual Membership product   -- AnnualMem_Day_Gender_AvgDuration_Avg&SumRevenue.csv
WITH max_24h AS (
	SELECT 
		trip_id,
		start_time,
		gender,
		CASE
			WHEN tripduration_secconds <= 86400 THEN tripduration_secconds	-- 24 hours
			ELSE 86400
		END AS new_trip_duration,
		CASE
			WHEN tripduration_secconds <= 86400 THEN revenue	-- 24 hours
			ELSE 250  
		END AS new_revenue
	FROM trips
	WHERE usertype = 'Subscriber'
)
SELECT
	CASE
          WHEN EXTRACT(ISODOW FROM start_time) = 1 THEN 'Monday'  -- ISODOW: Monday = 1
          WHEN EXTRACT(ISODOW FROM start_time) = 2 THEN 'Tuesday'
          WHEN EXTRACT(ISODOW FROM start_time) = 3 THEN 'Wednesday'
          WHEN EXTRACT(ISODOW FROM start_time) = 4 THEN 'Thursday'
          WHEN EXTRACT(ISODOW FROM start_time) = 5 THEN 'Friday'
          WHEN EXTRACT(ISODOW FROM start_time) = 6 THEN 'Saturday'
		  WHEN EXTRACT(ISODOW FROM start_time) = 7 THEN 'Sunday'
    END day_name,
	gender,
	AVG(new_trip_duration) AS avg_trip_duration_sec,
	AVG(new_revenue) AS avg_revenue,
	SUM(new_revenue) AS total_revenue
FROM max_24h
GROUP BY 1, 2;
