# Create Spark session with SAS7BDAT jar
from pyspark.sql import SparkSession
from itertools import chain
import os
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
import configparser 


config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
     '''
     Description: This function can be used to create a spark session.
     Arguments:
        None
     Returns:
        SparkSession
    '''
    spark = SparkSession.builder\
                        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
                        .config("spark.jars", "/home/workspace/RedshiftJDBC42-no-awssdk-1.2.53.1080.jar")\
                        .enableHiveSupport().getOrCreate()

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    return spark

#Udfs for cleaning Data

def isvalidcountry(country):
    '''
    Parameters
    ----------
    country : string
        country string to validate

    Returns
    -------
    int
        1 if country is a valid country,else 0
    '''
    
    if ('Invalid' in country or 
        'Collapsed' in country or
        'No country code' in country):
        return 0
    else:
        return 1
    
IsValidCountry=udf(isvalidcountry,IntegerType())


def decode_mode(mode_code):
    if mode_code == 1.0:
        return 'Air'
    elif mode_code == 2.0:
        return 'Sea'
    elif mode_code == 3.0:
        return 'Land'
    elif mode_code == 9.0:
        return 'Not Reported'
      
DecodeMode=udf(decode_mode,StringType())

def decode_visa(visa_code):
    if visa_code == 1.0:
        return 'Business'
    elif visa_code == 2.0:
        return 'Pleasure'
    elif visa_code == 3.0:
        return 'Student'
      
DecodeVisa=udf(decode_visa,StringType())


import Levenshtein

def comparecountry(country1, country2):
    '''
    Parameters
    ----------
    country1 : string
        country string from whr dataset
    country2 : string
        country string from immigration dataset

    Returns
    -------
    int
    measures the distance measure between the 2 strings
    '''
    # This udf is for the join condition . You'll need to install python-Levenshtein library
    if country1 in country2:
        return 0
    else:
        return Levenshtein.distance(country1,country2)
      
CompareCountry=udf(comparecountry,IntegerType())


                                                    


def quality_check(df, description):
    '''
    Input: Spark dataframe, description of Spark datafram
    Output: Print outcome of data quality check
    '''
    
    result = df.count()
    if result == 0:
        print("Data quality check failed for {} with zero records".format(description))
    else:
        print("Data quality check passed for {} with {} records".format(description, result))
    return 0

def main():

    spark = create_spark_session()

    KEY                    = config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET                 = config.get('AWS','AWS_SECRET_ACCESS_KEY')

    session = boto3.Session( aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET,
                             region_name='us-west-2' )

    s3_path="s3a://capstoneprojectudacity/"

    # Create Spark Dataframe
    immigration_dataset_file = '18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat'

    df_immigration = spark.read.format("com.github.saurfang.sas.spark").load(s3_path+immigration_dataset_file)
    df_gdp_world = spark.read.option("header", "true").format("csv").load(s3_path+"GDP.csv")
    df_us_gdp_by_state = spark.read.option("header", "true").format("csv").load(s3_path+"bea-gdp-by-state.csv")

    # Set year = 2016
    year = 2016

    # Preparation step for cleaning Immigration Data

    # Create dictionary for decoding residence country in immigration dataset using i94res.txt
    dict_res_country = {}
    i94res_file = spark.sparkContext.textFile(s3_path+"i94res.txt")

    for line in i94res_file.collect():
        line = line.split('=')
        code = float(line[0])
        country = line[1].strip().strip("'").capitalize()
        dict_res_country[code] = country

    mapping_expr = create_map([lit(x) for x in chain(*dict_res_country.items())])

    # Create dictionary for decoding arrival US port in immigration dataset using i94port.txt
    dict_port = {}
    i94port_file = spark.sparkContext.textFile(s3_path+"i94port.txt")

    for line in i94port_file.collect():
        line = line.split('=')
        code = line[0].strip('\t').strip("''")
        port = line[1].strip().strip("'")
        dict_port[code] = port

    mapping_expr1 = create_map([lit(x) for x in chain(*dict_port.items())])

    # Create dictionary for decoding arrival US state in immigration dataset using i94addr.txt
    dict_us_states = {}
    i94addr_file = spark.sparkContext.textFile(s3_path+"i94addr.txt")

    for line in i94addr_file.collect():
        line = line.split('=')
        code = line[0].strip("'")
        us_state = line[1].strip().strip("'").title()
        dict_us_states[code] = us_state

    mapping_expr2 = create_map([lit(x) for x in chain(*dict_us_states.items())])

    # Clean and decode immigration dataframe

    df_immigration = df_immigration.withColumn( 'i94res',  mapping_expr[df_immigration['i94res']] )\
                                   .withColumn( 'i94port', mapping_expr1[df_immigration['i94port']])\
                                   .withColumn( 'i94addr', mapping_expr2[df_immigration['i94addr']])\
                                   .withColumn( 'i94mode', DecodeMode(col('i94mode')))\
                                   .withColumn( 'i94visa', DecodeVisa(col('i94visa')))\
                                   .withColumn( 'port', split(col("i94port"),",").getItem(0))\
                                   .withColumn( 'isvalidcountry', IsValidCountry(col('i94res')))\
                                   .where( (col('isvalidcountry') == 1) & \
                                           (col('i94addr') != '99'))\
                                   .select( col("i94yr").alias("arrival_year").cast("int"),\
                                            col("i94mon").alias("arriva_month").cast("int"),\
                                            col("i94res").alias("origin_country"),\
                                            col("i94addr").alias("arrival_us_state"),\
                                            col("port").alias("arrival_port"),\
                                            col("biryear").alias("birth_year").cast("int"),\
                                            col("gender"),\
                                            col("i94visa").alias("visa_type"),\
                                            col("i94mode").alias("arrival_mode"))

    #Build Fact table

    # Join between immigration data and gdp datasets                

    join_condition =[ CompareCountry( df_gdp_world.Country, df_immigration.origin_country ) < 3 ]
      
    df_fact_table = df_immigration.alias("immigration_data").join( df_gdp_world.alias("gdp_world"), join_condition, how="cross")\
                                                            .join( df_us_gdp_by_state.alias("gdp_by_state"), col("arrival_us_state")==col("Area"), how="inner")\
                                                            .select( col("arrival_year"),\
                                                                     col("arriva_month"),\
                                                                     col("origin_country"),\
                                                                     col("arrival_us_state"),\
                                                                     col("arrival_port"),\
                                                                     col("birth_year"),\
                                                                     col("gender"),\
                                                                     col("visa_type"),\
                                                                     col("arrival_mode"),\
                                                                     col("gdp_world.gdp_{}".format(str(year))).alias("origin_country_gdp_pro_capita"),\
                                                                     col("gdp_by_state.gdp_{}".format(str(year))).alias("arrival_us_state_gdp_pro_capita"))

    #Write dataframes to Redshift cluster

    df_gdp_world.write\
                .format("jdbc")\
                .options( url=config.get('AWS','JDBC_REDSHIFT_URL'), dbtable="public.gdp_world",\
                          driver="com.amazon.redshift.jdbc42.Driver", user=config.get('AWS','REDSHIFT_USER'),\
                          password=config.get('AWS','REDSHIFT_PASSWORD'))\
                .mode("overwrite")\
                .save()

    df_us_gdp_by_state.select(col("Area"),\
                              col("gdp_2013"),\
                              col("gdp_2014"),\
                              col("gdp_2015"),\
                              col("gdp_2016"),\
                              col("gdp_2017"))\
                      .write\
                      .format("jdbc")\
                      .options( url=config.get('AWS','JDBC_REDSHIFT_URL'), dbtable="public.gdp_us",\
                                driver="com.amazon.redshift.jdbc42.Driver", user=config.get('AWS','REDSHIFT_USER'),\
                                password=config.get('AWS','REDSHIFT_PASSWORD'))\
                      .mode("overwrite")\
                      .save()

    df_fact_table.write\
                 .partitionBy("arrival_year","arrival_month")\
                 .format("jdbc")\
                 .option(url=config.get('AWS','JDBC_REDSHIFT_URL'), dbtable="public.gdp_us",\
                                driver="com.amazon.redshift.jdbc42.Driver", user=config.get('AWS','REDSHIFT_USER'),\
                                password=config.get('AWS','REDSHIFT_PASSWORD'))\
                 .mode("append")\
                 .save()

    # Perform data quality check
    quality_check(df_fact_table, "immigration table")

if __name__ == "__main__":
    main()