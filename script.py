import pandas as pd
import requests
import json 
import psycopg2
import csv
import os

import requests

# url = "https://api.rentcast.io/v1/properties/random"

# headers = {"accept": "application/json",
#           "X-API-Key": "24e92d2548fa413b8f0451e55de0d6b1"}

# querystring ={"limit": "10000"}

# response = requests.get(url, headers=headers, params = querystring)

# data = response.json()

# # print(response.json())

# filename = "property.json"

# with open(filename, 'w', encoding='utf-8') as file:
#     json.dump(data, file, indent=4)


# reading the data
df = pd.read_json("property.json")


#transforming the data

# extracting the names and types from the owner column
df['owner_type'] = df['owner'].apply(lambda x: x.get('type') if isinstance(x,dict) else x if isinstance(x,list) else None)
df['owners_name'] = df['owner'].apply(lambda x: x.get('names') if isinstance(x,dict) else x if isinstance(x,list) else None)

df['features']= df['features'].apply(json.dumps)

# extracting the taxassessment values for year 2022 and 2023 
df['taxAssessments_2022_value'] = df['taxAssessments'].apply(lambda x :x.get('2022', {}).get('value') if isinstance(x,dict) else None)
df['taxAssessments_2023_value'] = df['taxAssessments'].apply(lambda x :x.get('2023', {}).get('value') if isinstance(x,dict) else None)

# extracting the propertyTaxes values for year 2022 and 2023
df['propertyTaxes_2022_total'] = df['propertyTaxes'].apply(lambda x :x.get('2022', {}).get('total') if isinstance(x,dict) else None)
df['propertyTaxes_2023_total'] = df['propertyTaxes'].apply(lambda x :x.get('2023', {}).get('total') if isinstance(x,dict) else None)

# changing the lastSaleDate data type
df['lastSaleDate'] = pd.to_datetime(df['lastSaleDate'], errors="coerce", utc=True)

df['year'] = df['lastSaleDate'].dt.year
df['month_name'] = df['lastSaleDate'].dt.month_name()
df['quarter'] = df['lastSaleDate'].dt.quarter

df['year'] = df['year'].fillna(0).astype(int)
df['quarter'] = df['quarter'].fillna(0).astype(int)


# using the fillna to replace NaN values

df.fillna({
        'bedrooms': 0,
        'owners_name':  'Unknown', 
        'squareFootage': 0,
        'yearBuilt': 0,
        'features': 'None',
        'assessorID': 'Unknown',
        'legalDescription': 'Not available',
        'subdivision': 'Not available', 
         'zoning': 'Unknown', 
         'bathrooms': 0, 
         'lotSize': 0,
         'propertyType': 'Unknown', 
         'taxAssessments_2022_value': 0,
         'taxAssessments_2023_value':0,
         'propertyTaxes_2022_total':0,
         'propertyTaxes_2023_total':0,
         'lastSalePrice': 0,
        'ownerOccupied': 0,
        'county': 'Not available'}, inplace =True)


# Normalizing the data

# creating the date dimension table
date_dim = df[['year', 'month_name', 'quarter','lastSaleDate']].drop_duplicates().reset_index(drop=True)
date_dim['date_id'] = date_dim.index +1

df = df.merge(date_dim[['year', 'month_name', 'quarter','lastSaleDate', 'date_id']],
              on=['year', 'month_name', 'quarter','lastSaleDate'],
              how='left'
             )

# location_dim table

location_dim = df[['addressLine1','city','state', 'zipCode', 'county', 'latitude', 'longitude','zoning','subdivision']].drop_duplicates().reset_index(drop=True)
location_dim['location_id'] = location_dim.index +1

df = df.merge(location_dim[['addressLine1','city','state', 'zipCode', 'county', 'latitude', 'longitude','zoning','subdivision', 'location_id']],
              on=['addressLine1','city','state', 'zipCode', 'county', 'latitude', 'longitude','zoning','subdivision'],
              how='left'
             )

# owner_dim table
# The owner column contains a list of dictionaries, so we need to check if the value is a list or a dictionary
for col in ['owners_name', 'owner_type', 'ownerOccupied']:
    df[col] = df[col].apply(lambda x:str(x) if isinstance(x,list) else x)


owner_dim = df[['owners_name', 'owner_type', 'ownerOccupied']].drop_duplicates().reset_index(drop=True)
owner_dim['owner_id'] = owner_dim.index +1

df = df.merge(owner_dim[['owners_name', 'owner_type', 'ownerOccupied', 'owner_id']],
              on=['owners_name', 'owner_type', 'ownerOccupied'],
              how='left'
             )

# property_dim table
property_dim = df[['propertyType','features', 'bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'yearBuilt','assessorID', 'legalDescription']].drop_duplicates().reset_index(drop=True)
property_dim['property_id'] = property_dim.index +1

df = df.merge(property_dim[['propertyType','features', 'bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'yearBuilt','assessorID', 'legalDescription','property_id']],
              on=['propertyType','features', 'bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'yearBuilt','assessorID', 'legalDescription'],
              how='left'
             )

# property_fact table
property_fact = df[['date_id', 'location_id', 'owner_id', 'property_id', 'taxAssessments_2022_value','taxAssessments_2023_value', 'propertyTaxes_2022_total', 'propertyTaxes_2023_total','lastSalePrice']]

# create a folder
os.makedirs('data', exist_ok=True)

# save the dataframes to csv files
date_dim.to_csv('data/date_dim.csv', index=False)
owner_dim.to_csv('data/owner_dim.csv', index=False)
location_dim.to_csv('data/location_dim.csv', index=False)
property_dim.to_csv('data/property_dim.csv', index=False)
property_fact.to_csv('data/property_fact.csv', index=False)

def get_connection():
    try:
        connection = psycopg2.connect(
            host='localhost',
            port=5432,
            user='postgres',
            database='zapco_db',
            password='chichi'
        )
        print('database connection successful')
        return connection
    except Exception as e:
        print(f' connection failed due to: {e}')
    return None

get_connection()

def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    # Create schema
    create_schema = '''
        CREATE SCHEMA IF NOT EXISTS zapco;
    '''

    # Drop existing tables and create new ones
    create_tables = '''
        DROP TABLE IF EXISTS zapco.date_dim CASCADE;
        DROP TABLE IF EXISTS zapco.location_dim CASCADE;
        DROP TABLE IF EXISTS zapco.owner_dim CASCADE;
        DROP TABLE IF EXISTS zapco.property_dim CASCADE;
        DROP TABLE IF EXISTS zapco.property_fact CASCADE;

        CREATE TABLE zapco.date_dim (
            year INT,
            month_name TEXT,
            quarter INT,
            lastSaleDate DATE,
            date_id INT PRIMARY KEY
        );

        CREATE TABLE zapco.location_dim (
            addressLine1 TEXT,
            city TEXT,
            state TEXT,
            zipCode INT,
            county TEXT,
            latitude FLOAT,
            longitude FLOAT,
            zoning TEXT,
            subdivision TEXT,
            location_id INT PRIMARY KEY
        );

        CREATE TABLE zapco.owner_dim (
            owners_name TEXT,
            owner_type TEXT,
            ownerOccupied FLOAT, 
            owner_id INT PRIMARY KEY
        );

        CREATE TABLE zapco.property_dim (
            propertyType TEXT,
            features TEXT,
            bathrooms FLOAT,
            bedrooms FLOAT,
            squareFootage FLOAT,
            lotSize FLOAT,
            yearBuilt FLOAT,
            assessorID TEXT,
            legalDescription TEXT,
            property_id INT PRIMARY KEY
        );
        CREATE TABLE zapco.property_fact(
            fact_id SERIAL PRIMARY KEY,
            date_id INT REFERENCES zapco.date_dim(date_id),
            location_id INT REFERENCES zapco.location_dim(location_id),
            owner_id INT REFERENCES zapco.owner_dim(owner_id),
            property_id INT REFERENCES zapco.property_dim(property_id),
            taxAssessments_2022_value NUMERIC,
            taxAssessments_2023_value NUMERIC,
            propertyTaxes_2022_total NUMERIC,
            propertyTaxes_2023_total NUMERIC,
            lastSalePrice NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
    
    '''
    
    try:
        print("Creating Schema....")
        cursor.execute(create_schema)
        
        print("Creating tables...")
        cursor.execute(create_tables)

        conn.commit()
        print("All tables created successfully")
              
    except Exception as e:
        print(f"Error occurred during the table creation: {e}")
    
    finally:
        cursor.close()
        conn.close()
        print("Database connection closed")

create_tables()

# Loading data into the Database
# Function to load data into the database
def load_data(csv_file_path, table_name, column_names):
    conn= get_connection()
    cursor= conn.cursor()
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        reader= csv.reader(file)
        next(reader)
        for row in reader:
            row=[None if cell in ('', 'Not available', ' ')else cell for cell in row]

            placeholder =', '.join(['%s'] *len(row))
            query= f' INSERT INTO {table_name} ({", ".join(column_names)}) VALUES({placeholder});'

            cursor.execute(query, row)
    conn.commit()
    cursor.close()
    conn.close()
    print(f' dataset loaded successfully into the {table_name}')


#loading date
date_file_path = 'data/date_dim.csv'
load_data(date_file_path, 'zapco.date_dim', ['year', 'month_name', 'quarter', 'lastSaleDate', 'date_id'])

# loading location dim data
location_file_path = 'data/location_dim.csv' 
load_data(location_file_path, 'zapco.location_dim', ['addressLine1', 'city', 'state', 'zipCode', 'county', 'latitude', 'longitude', 'zoning', 
                                           'subdivision', 'location_id'])


# loading owner dim data

owner_file_path = 'data/owner_dim.csv' 
load_data(owner_file_path, 'zapco.owner_dim', ['owners_name', 'owner_type', 'ownerOccupied', 'owner_id'])
          


# loading property dim data

property_file_path = 'data/property_dim.csv' 
load_data(property_file_path, 'zapco.property_dim', ['propertyType', 'features', 'bathrooms', 'bedrooms',
                                                     'squareFootage', 'lotSize', 'yearBuilt','assessorID', 'legalDescription', 'property_id'])


#lolading the fact data
fact_file_path = 'data/property_fact.csv' 
load_data(fact_file_path, 'zapco.property_fact', [ 'date_id', 'location_id', 'owner_id', 'property_id', 'taxAssessments_2022_value',
                                                  'taxAssessments_2023_value', 'propertyTaxes_2022_total','propertyTaxes_2023_total',
                                                  'lastSalePrice'])
