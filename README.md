# Data Warehousing Project with Redshift
## David Miller

## Intro + How-To
* The entire ETL process/logic was created in `sql_queries.py` specifically the `COPY` and `INSERT` queries. The other py files simply call and run the queries
* To run the pipeline from your command line tool, first make sure you're in the same directory as the project
* Then run `python create_tables.py`. Once that is complete run `python etl.py`


## Part 1 - Table Creation
* The first stage in this project was to create each table and thereby establish the schema for the both the staging and final tables
* The staging tables did not have any constraints or relationships put on it so as to pull in all of the data first. Below is the schema:

![staging_schema](staging_sechema.PNG)
* However, the final tables required many more constraints and to be organized in a more normalized (albeit not 3NF) fashion, which you can see below:

![schema][schema.png]
* Writing the CREATE queries for all of these was pretty straightforward, but knowing when and when not to put constraints on the final schema was essential

## Part 2 - Extracting and Loading the Data into Staging
* The most complex aspect of this part of the project was making sure AWS credentials were entered in correctly as well as using the right syntax for loading data from a JSON file, something I had not done before with any flavor of SQL (much more often in Python)
* There was no transformation performed as in this stage we just wanted to move the data from Udacity's S3 bucket to my Redshift cluster

## Part 3 - Transforming and Loading the Data into Final Tables
* This was probably the most difficult/time consuming part of the project as writing the logic particularly for the `songplays` table proved cumbersome at times
    * In particular converting UNIX times to timestamp objects was very manual, requiring this line in the query: `TIMESTAMP 'epoch' + e.ts/1000*INTERVAL '1 second' as start_time`
* Overall the rest was pretty straightforward it was mostly a matter of making sure to select distinct records and filter the events table for only `NextSong` logs
* However, there is an outstanding issue with the songplays table. The two staging tables are joined on artist name, song name and duration of the song. Not only is this not ideal, but has actually caused a huge amount of data to be lost, going from several thousand records to just a few hundred. Ideally the log data would have `song_id` and `artist_id` (the two columns being brought in), but in lieu of that this was the best option.
