# Spotify-Summer-Warped
An ETL pipeline that extracts data regarding the songs and artists in my Summer 2023 playlist on Spotify and stores it in a PostgreSQL database

This ETL pipeline was written using Python, Amazon Web Services, and PostgreSQL. It was orchestrated by Apache Airflow and run using a Docker container.

Below is a diagram for the enitre ETL process.

<img width="755" alt="Screenshot 2023-05-11 at 5 19 08 PM" src="https://github.com/AnantaMoharana/Spotify-Summer-Warped/assets/48960503/2941f455-c9cb-4381-b750-8c232fa39949">

The ETL process consists of the following steps 

**Extract:** 
In this step, we extract data from the Spotify API. We first use the get playlist items endpoint to get all the songs present in the playlist. We then extract the artist information from each song payload we get, after this we use the get artist information endpoint to get the necessary information on the artists. All this information is recieved as a JSON payload.

**Transform:** 
In this step, we transform the raw JSON data and get the needed information. We do this buy extracting the needed fields and then storing this data in a pandas dataframe. We then use the necessary transformations to create our tables and then store them as CSVs in a S3 bucket on Amazon Web Services, after we validate that the data has no nulls and that we have the necessary information. 

**Load:** 
In this step, we load the transformed data into the Postgres database. We do this buy using a Postgre hook which we create from a PostegreSQL connection we make in Airflow.



