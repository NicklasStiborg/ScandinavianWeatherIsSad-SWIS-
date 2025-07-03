# 1. Introduction
SWIS gathers data from the [Norwegian](https://frost.met.no) and [Danish](https://www.dmi.dk) meteorological institutes into a medallion data architecture. The project repo is built with scalable principles in mind (pyspark for clustering, postgreSQL for ACID transactions, config-file for better management etc.), while also being simple with only two data sources and two tables in the gold layer. 

# 2. Installation
In order to get SWIS running you will need to install [Python](https://www.python.org/downloads/), Java from e.g. [Oracle](https://www.oracle.com/java/technologies/downloads/?er=221886) for running PySpark, as well as a database system e.g. [PostgreSQL](https://www.postgresql.org/download/). You can use other libraries and systems as long as you edit the options-methods when writing to the database in the `import_gold`-file. Please bear in mind that the installation and troubleshooting can be a little much, whereas [SQLite](https://sqlite.org) or [DuckDB](https://duckdb.org) can be more straightforward. In case of switching database systems, its suggested refactoring the code from pySpark to Pandas. In addition it is recommended installing [git](https://git-scm.com) for easier cloning (copying) of the project.

## 2.1. API and database access
The bronze layer (`import_bronze`-file) uses the APIs of [Frost Met](https://frost.met.no) and [DMI](https://www.dmi.dk), with registered API keys/users. You can register for free keys here: [DMI user signup](https://opendatadocs.dmi.govcloud.dk/en/Authentication), [Frost met user signup](https://frost.met.no/howto.html). After getting your API keys, please insert them in the `.env_sample` and rename the file to `.env` or alternatively write them in manually in the notebook if you do not plan on publishing it anywhere. 

## 2.2. Getting started
To getting started it is suggested to:
+ **Installing the above-mentioned software (required)**
+ **Create a working directory (folder) (required)**
+ **Open terminal/cmd** \
Go into directory using `cd <path to folder>` and then use `git clone https://github.com/NicklasStiborg/ScandinavianWeatherIsSad-SWIS-` (optional, can be downloaded as zip instead)
+ **Creating virtual environment (optional)** 
`python3 -m venv .venv`
+ **Activate the virtual environment (optional)** \
Mac/Linux: `source .venv/bin/activate` \
Windows; `.venv\Scripts\activate.bat`
+ **Install pip3 by opening the terminal/cmd and typing:** \
Mac/Linux: `python3 -m ensurepip --upgrade` \
Windows: `python -m ensurepip --upgrade`
+ **Install dependencies/libraries** \
Open terminal and go into directory using `cd <path to folder>` \
followed by `pip install -r requirements.txt`



# 3. Purpose
SWIS is great for getting a basic understanding of a medallion architecture, without being dependandent of any cloud providers. I have also added a few data files as examples for you to see how it looks in practice. 
SWIS consists of the three layers:

## 3.1. Medallion architecture

### 3.1.1. Bronze layer: Requests and storing JSON
In this layer requests are made using the `Requests`-library, and the responses of weather stations and their readings are saved raw as `JSON`. In the bronze layer, the main focus is to store the raw data without doing any manipulation; _what you see in this layer is what the datasources provides_. 

### 3.1.2. Silver layer: Cleaning, filtering and storing in Parquet
In this layer the `JSON`-files are imported, cleaned and filtered using `PySpark` and exported to `Parquet`-files in their respective folders with timestamp of creation time. In the silver layer, the main focus is to have clean, filtered but unaggregated data, thus the data is still seperated between data sources.

### 3.1.3. Gold layer: Aggregation and storing in SQL
In this layer the Parquet files from both datasources are combined into a single `SQL` table stored in a `PostgreSQL` database. By keeping the data in a relational database it makes it easy to use in e.g. semantic models in `PowerBI` or perform. If the data pipeline should be truly scalable it would be advised to switching to a database system that is better optimised for OLAP.