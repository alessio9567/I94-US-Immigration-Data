# US Immigration Data and GDP per capita 
### Data Engineering Capstone Project

#### Project Summary
For this project I will use I94 US immigration data,a GDP per capita by US state dataset and a GDP per capita by Country in the world dataset to create a database for analytics purpose on immigration events related to economics.
In fact GDP (Gross Domestic Product) is the monetary value of all finished goods and services made within a country during a specific period. GDP provides an economic snapshot of a country, used to estimate the size of an economy and growth rate.
An ETL pipeline is to be build with these three data sources to create the database.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Clean the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up


### Step 1: Scope the Project and Gather Data

#### Scope 

We would be creating 2 dimension tables and 1 fact table. 
I94 immigration data will be joined with the other two datasets and aggregated by year and month. Thise will be the fact table. 
A final database will be created to query on immigration events to determine if gdp affects the selection of destination cities for immigration. 

#### Describe and Gather Data 

I94 immigration data comes from the [US National Tourism and Trade Office website](https://travel.trade.gov/research/reports/i94/historical/2016.html). It is provided in SAS7BDAT format which is a binary database storage format.

**Key Notes:**
- i94yr = 4 digit arrival year in US,
- i94mon = numeric arrival month  in US,
- i94res = 3 digit code of origin country, 
- i94port = 3 character code of destination US city,
- i94mode = 1 digit travel code,
- i94visa = reason for immigration
- i94ddr = arrival US state

GDP World data is a dataset taken from Kaggle. It contains GDP per capita of all countries from 1990 to 2018

GDP by US state is taken from this US governmental website:
https://www.bea.gov/data/gdp/gdp-state

### Step 2: Clean the Data

In this step I cleaned the immigration dataset using UDFs to decode certain columns


### Step 3: Define the Data Model

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model

**Fact Table** - I94 immigration data joined with the US and World GDP per capita datasets

Columns:
   - arrival_year
   - arriva_month
   - origin_country 
   - arrival_us_state
   - arrival_port
   - birth_year
   - gender
   - visa_type
   - arrival_mode
   - origin_country_gdp_pro_capita
   - arrival_us_state_gdp_pro_capita


**Dimension Table** - GDP by Country in the World per capite

Columns:
   - Country 
   - Country Code
   - gdp_1990
   - gdp_...
   ...
   - gdp_2019
   

**Dimension Table** - GDP by US state dataset 

Columns:
   - Area (US state)
   - gdp_2013 
   - gdp_2014
   - gdp_2015
   - gdp_2016
   - gdp_2017



* Step 4: Run ETL to Model the Data

Pipeline Steps:

1. Clean I94 data as described in step 2 to create Spark dataframe df_immigration for each month.
2. Create gdp_world dimension table using df_gdp_world and write to Amazon Redshift
4. Create gdp_us_state dimension table by selecting relevant columns from df_us_gdp_by_state and write to Amazon Redshift
5. Create fact table by joining immigration data and gdp dimension tables and write to Amazon Redshift partitioned by arrival_year and arrival_month


* Step 5: Complete Project Write Up

#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project:
  1. I used Spark since it can easily handle multiple file formats (SAS, csv, etc) that contain large amounts of data. 
     Spark dataframe API was used to process the input files into dataframes and manipulated via standard join operations to create the tables.
    
* Propose how often the data should be updated and why.
    1. Since the format of the raw files are monthly, we should continue pulling the data monthly ( for each SAS immigration data file )
    
### Scenarios
* Write a description of how you would approach the problem differently under the following scenarios:
    1. the data was increased by 100x.
        - Use Amazon Redshift: It is an analytical database that is optimized for aggregation and read-heavy workloads
    2. The data populates a dashboard that must be updated on a daily basis by 7am every day.
        - Airflow can be used here, create DAG retries or send emails on failures.
        - Have daily quality checks; if fail, send emails to operators and freeze dashboards
    3. The database needed to be accessed by 100+ people.
        - Redshift can help us here since it has auto-scaling capabilities and good read performance




