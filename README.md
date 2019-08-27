<p align="center">
  <img width="530" height="270" src="https://upload.cc/i1/2019/08/28/1cL7o6.jpeg">
</p>

# Data modeling with PostgresSQL (By using Sparkify data)
#### PROJECT BACKGROUND AND SUMMARY
- ###### BACKGROUND
Sparkify is a startup company which provides the music streaming app. Recently, the analytics team in this company is interested in understanding their user activity on its music streaming app in order to provide better user experience for their user. This project aims for creating a SQL database which hosts all user activity data, song data, and user’s personal data. By having this SQL database, the analytics team can analyze the data, give suggestions to the APP development team, and improve the product.  

- ###### DETAILS AND DATA MODELING
In this project, it will create an ETL data pipeline to extract data from log dataset and song dataset, transform these data into a format which analytics team prefers, and put data into a Postgres database. As the amount of data is huge and access speed is important for analytics team, this project decides to use star schema to store the data and improve access to data. 
The fact table is songplay, it includes information about songplay history. The dimension tables are user, song, artist, and time. User table includes the user's personal information. Song table includes the song's information. Time table includes when a song is played. The structure can be seen in the below picture.

<p align="center">
  <img src="https://upload.cc/i1/2019/08/25/gM9qd6.jpg">
</p>

------------
#### FILES IN THE REPOSITORY
1. **ETL.ipynb**: a jupyter notebook file which detail how access to Postgres database, extract data from the song and log file, transform song and log data into the format the analytics team wants, and dump data into Postgres database.

2. **test.ipynb**: a jupyter notebook file which is written for test whether song and log data is stored in Postgres successfully or not

3. **sql_queries.py**: a python script which details all SQL queries are used in "create_tables.py" and "etl.py"

4. **create_tables.py**: a python script which is used for creating database, creating table, and dropping tables in Postgres database'

5. **etl.py**: a python script which is used for ETL process and putting data into Postgres database

6. **data**: the song and log datafile
------------
#### HOW TO RUN THE PROJECT
To start the project, you may need **create_tables.py** and **etl.py**. Steps are below.
1. copy the who project to your local machine

2. type `python3 create_tables.py` in your terminal to create database and table in Postgres database

3. type `python3 etl.py` in your terminal to start the ETL process and dump data into Postgres database

4. use jupyter notebook to open **test.ipynb** and run the command in this .ipynb file. Then, check each command can operate properly. If it is, congratulations, all data are successfully stored in Postgres and you can do further analysis

