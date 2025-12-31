USE NetflixProject;

SELECT * FROM Netflix_movies_tv_shows;

----------------------------------------------------------------------------------------------------
-- We are recreating this table to improve performance and data quality.
-- The current table uses NVARCHAR(MAX) / VARCHAR(MAX) for almost all columns,
-- which causes performance issues during filtering, sorting, DISTINCT, and JOIN operations.
-- MAX datatypes consume more memory, limit indexing options, and increase storage usage.
-- This design is suitable for raw or staging data, but not for analytical or reporting purposes.
-- Therefore, we will create a new empty table with proper and optimized data types
-- and then insert data using append mode instead of replace.
----------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------
-- Step 1:
-- First, the existing table will be deleted using the DROP TABLE command.
-- This is required because the current table is not optimized and uses
-- VARCHAR(MAX) / NVARCHAR(MAX) data types, which negatively affect performance
-- and indexing.

-- Step 2:
-- After dropping the table, a new empty table will be created using the
-- CREATE TABLE command.
-- In this step, the table structure will be defined properly, including:
--   - Column names
--   - Optimized data types (instead of MAX datatypes)
--   - Necessary constraints such as PRIMARY KEY and NOT NULL
-- At this stage, only the table structure will exist in the SQL database
-- and no data will be present in the table.

-- Step 3:
-- Data insertion will be performed from a Python file where the data was
-- already converted into a Pandas DataFrame.
-- The DataFrame.to_sql() method will be used to insert data into SQL Server.

-- Step 4:
-- While inserting data from Python, the if_exists argument will be set to
-- 'append' instead of 'replace'.
-- This ensures that:
--   - Data is appended into the structured table
--   - The existing table structure remains unchanged
--   - No table or schema is replaced or dropped
-- Since the SQL table already exists and is empty, only values will be
-- inserted and nothing will be replaced.
---------------------------------------------------------------------------------

DROP TABLE [dbo].[Netflix_movies_tv_shows];

SELECT * FROM Netflix_movies_tv_shows;

CREATE TABLE [dbo].[Netflix_movies_tv_shows](
	[show_id] [varchar](10) PRIMARY KEY,
	[type] [varchar](10) NULL,
	[title] [nvarchar](200) NULL,
	[director] [varchar](250) NULL,
	[cast] [varchar](1000) NULL,
	[country] [varchar](150) NULL,
	[date_added] [varchar](20) NULL,
	[release_year] [int] NULL,
	[rating] [varchar](10) NULL,
	[duration] [varchar](10) NULL,
	[listed_in] [varchar](100) NULL,
	[description] [varchar](500) NULL
)

SELECT * FROM Netflix_movies_tv_shows;

-- After Insersion

SELECT * FROM Netflix_movies_tv_shows;

SELECT title FROM Netflix_movies_tv_shows;

SELECT * FROM Netflix_movies_tv_shows
WHERE show_id = 's5023';

---------------------------------------------------------------------------------------------
-- Handling duplicates ----------------------------------------
---------------------------------------------------------------------------------------------


SELECT title FROM Netflix_movies_tv_shows
GROUP BY title 
HAVING COUNT(*) > 1;

SELECT title, COUNT(*) FROM Netflix_movies_tv_shows
GROUP BY title 
HAVING COUNT(*) > 1;

SELECT * FROM Netflix_movies_tv_shows
WHERE title IN (
SELECT title FROM Netflix_movies_tv_shows
GROUP BY title 
HAVING COUNT(*) > 1
)
ORDER BY title;

-- TV SHOW + Movies (Accept this duplicate)
-- TV Show + TV Show (Not accepted this duplicate)
-- Movies + Movies (Not accepted this duplicate)

SELECT title, type FROM Netflix_movies_tv_shows
GROUP BY title, type
HAVING COUNT(*) > 1

SELECT * FROM Netflix_movies_tv_shows
WHERE concat(title, type) IN (
SELECT concat(title, type) FROM Netflix_movies_tv_shows
GROUP BY title, type 
HAVING COUNT(*) > 1
)
ORDER BY title;



---------------------------------------------------------------------------------------------
-- To remove duplicate-----------------------------------------------------------
---------------------------------------------------------------------------------------------


SELECT *, ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
FROM Netflix_movies_tv_shows
ORDER BY show_id;


WITH cte AS (
SELECT
	*, 
	ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
FROM Netflix_movies_tv_shows
)
SELECT * INTO NETFLIX_raw
FROM cte WHERE rn = 1;



---------------------------------------------------------------------------------------------
-- Our New Filtered Data After Removing Duplicate Is: ----------------------
---------------------------------------------------------------------------------------------

SELECT * FROM NETFLIX_raw;



---------------------------------------------------------------------------------------------
-- Director Names are in same cell -----------------------------------------
---------------------------------------------------------------------------------------------

SELECT show_id AS Show_ID, trim(Value) AS Directors
INTO NETFLIX_directors
FROM NETFLIX_raw
CROSS APPLY string_split (director, ',');

SELECT * FROM NETFLIX_directors;



---------------------------------------------------------------------------------------------
-- Country Names are in same cell -----------------------------------------
---------------------------------------------------------------------------------------------

SELECT show_id AS Show_ID, trim(Value) AS Country
INTO NETFLIX_country
FROM NETFLIX_raw
CROSS APPLY string_split (country, ',');

SELECT * FROM NETFLIX_country;




-- Cast Names are in same cell --

SELECT show_id AS Show_ID, trim(Value) AS Cast
INTO NETFLIX_cast
FROM NETFLIX_raw
CROSS APPLY string_split (cast, ',');

SELECT * FROM NETFLIX_cast;



---------------------------------------------------------------------------------------------
-- Listed_in Names are in same cell ---------------------------------------
---------------------------------------------------------------------------------------------


SELECT show_id AS Show_ID, type, trim(Value) AS Listed_in
INTO NETFLIX_listed_in
FROM NETFLIX_raw
CROSS APPLY string_split (listed_in, ',');

SELECT * FROM NETFLIX_listed_in;


DROP TABLE NETFLIX_listed_in-

---------------------------------------------------------------------------------------------
-- [date_added] column has a datatype [varchar](20)... So fix this into DATE ------
SELECT * FROM NETFLIX_raw WHERE duration IS NULL;
-- 3 rows of Duration are Missing and these values are in rating column -----------
SELECT * FROM NETFLIX_filtered_data WHERE rating = '66 min';
---------------------------------------------------------------------------------------------

SELECT 
	show_id,
	type, 
	title, 
	CAST(date_added AS DATE) AS date_added, 
	release_year, 
	rating, 
	CASE
		WHEN duration IS NULL THEN rating 
		ELSE duration
	END AS duration, 
	description
INTO NETFLIX_filtered_data
FROM NETFLIX_raw
ORDER BY show_id;


SELECT * FROM NETFLIX_filtered_data

---------------------------------------------------------------------------------------------
-- Handling NULL values of Country Column ------------------------
---------------------------------------------------------------------------------------------


SELECT show_id, director, Country FROM NETFLIX_raw
WHERE country IS NULL;



SELECT Directors, Country
FROM NETFLIX_directors nd
INNER JOIN NETFLIX_country nc
ON nd.Show_ID = nc.Show_ID
GROUP BY Directors, Country;




SELECT show_id, map.Country FROM NETFLIX_raw nr

INNER JOIN (

SELECT  nd.Directors, nc.Country
FROM NETFLIX_directors nd
INNER JOIN NETFLIX_country nc
ON nd.Show_ID = nc.Show_ID
GROUP BY Directors, Country

) AS map

ON nr.director = map.Directors

WHERE nr.country IS NULL;



INSERT INTO NETFLIX_country
SELECT show_id, map.Country FROM NETFLIX_raw nr
INNER JOIN (
SELECT  nd.Directors, nc.Country
FROM NETFLIX_directors nd
INNER JOIN NETFLIX_country nc
ON nd.Show_ID = nc.Show_ID
GROUP BY Directors, Country
) AS map
ON nr.director = map.Directors
WHERE nr.country IS NULL;

-- Woh Rows insert hui hain jo main (NETFLIX_raw) table mein Country ki value NULL thin aur director ka kuch naam tha...
-- To woh saare NULL Values ki value Find kri hai... Director aur ShowID ki base pe

SELECT * FROM NETFLIX_country
GROUP BY Show_ID, Country;


---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------
/*								   NETFLIX DATA ANALYSIS						     	   *\
---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------

-- 1. for each director count the no of movies and tv shows created by them in separate columns for directors who have created tv shows and movies both */

SELECT  
	nd.Directors,
	COUNT(type) AS Total_No_of_Movies_and_Tv_shows,
	COUNT(CASE
			WHEN type = 'Movie' THEN n.show_id
		  END) AS No_of_Movies,
	COUNT(CASE
			WHEN type = 'TV Show' THEN n.show_id
		  END) AS No_of_Tv_Shows
FROM NETFLIX_filtered_data n
INNER JOIN NETFLIX_directors nd ON n.Show_ID = nd.Show_ID
GROUP BY nd.Directors
HAVING COUNT(DISTINCT type) = 2
ORDER BY COUNT(type) DESC


-- 2. Which country has highest number of comedy movies

SELECT 
	Country,
	COUNT(Listed_in)
FROM
	NETFLIX_listed_in nl
INNER JOIN 
	NETFLIX_country nc
ON
	nl.Show_ID = nc.Show_ID
WHERE
	Listed_in = 'Comedies'
GROUP BY
	Country
ORDER BY
	COUNT(Listed_in) DESC



-- 3. For each year (as per date added to netflix), which director has maximum number of movies released ?


WITH cte AS
(
SELECT
	Directors,
	YEAR(date_added) AS Released_Year,
	COUNT(*) AS No_of_Movies
FROM
	NETFLIX_directors nd
INNER JOIN
	NETFLIX_filtered_data nf
ON
	nd.Show_ID = nf.show_id
WHERE
	type = 'Movie'
GROUP BY
	Directors, YEAR(date_added)
)
SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY Released_Year ORDER BY Released_Year, No_of_Movies) AS Rank_of_Movies
INTO NETFLIX_rank_of_movies
FROM cte




SELECT  
	Directors,
	Released_Year,
	No_of_Movies
FROM
	NETFLIX_rank_of_movies n1
WHERE
	Rank_of_Movies IN (
		SELECT
			MAX(n2.Rank_of_Movies)
		FROM
			NETFLIX_rank_of_movies n2
		WHERE
			n1.Released_Year = n2.Released_Year
		GROUP BY
			n2.Released_Year
	)
ORDER BY
	Released_Year ASC,
	No_of_Movies DESC




-- 4. What is the Average duration of movies in each genre?

SELECT
	Listed_in, 
    AVG(CAST(REPLACE(duration, ' min', '') AS INT)) AS Avg_Duration_Minutes
FROM
	NETFLIX_filtered_data nf
INNER JOIN 
	NETFLIX_listed_in nl
ON 
	nf.show_id = nl.show_id
WHERE
	type = 'Movie'
GROUP BY 
	Listed_in



-- 5. What is the Average duration of movies and TV shows in each genre?


SELECT
	Listed_in, 
    AVG(duration_in_minutes) AS Avg_Duration_Minutes
FROM (
	SELECT
		show_id,
		type,
		CASE 
			-- Movies
			WHEN duration LIKE '%min' THEN
				TRY_CAST(REPLACE(duration, ' min', '') AS INT)

			-- TV Shows (Season / Seasons)
			WHEN duration LIKE '%Season%' THEN
				TRY_CAST(LEFT(duration, CHARINDEX(' ', duration) - 1) AS INT) * 450

			ELSE NULL
		END AS duration_in_minutes
	FROM NETFLIX_filtered_data
) t
INNER JOIN 
	NETFLIX_listed_in nl
ON 
	t.show_id = nl.show_id
GROUP BY 
	Listed_in


-- 6. Find the list of directors who have created horror and comedy movies both.
-- Display driector names along with number of comedy and horror movies directed by them.


SELECT 
	DISTINCT Directors,
	COUNT(CASE
			WHEN Listed_in = 'comedies'
			THEN nl.Show_ID
		  END) AS count_of_comedies,
	COUNT(CASE
			WHEN Listed_in = 'Horror Movies'
			THEN nl.Show_ID
		  END) AS count_of_Horror_Movies
FROM
	NETFLIX_listed_in nl
INNER JOIN
	NETFLIX_directors nd
ON
	nl.show_id = nd.Show_ID
WHERE
	type = 'Movie' AND
	Listed_in IN ('comedies', 'Horror Movies')
GROUP BY
	Directors
HAVING
	COUNT(DISTINCT Listed_in) = 2

