import io
import csv
import math
import psycopg2
import numpy as np
import pandas as pd
import simplejson as json
from pymongo import MongoClient
from sqlalchemy import create_engine


#function for renaming the columns
def renamecolumn(df,old,new):
    df=df[old].copy()
    j=0
    for i in old:
        #df.rename(columns = {i:new[j]})
        df.rename(columns = {i:new[j]},inplace=True)
        j=j+1
    return df

"""
#individual information dataset
#df_individual_information = dd.read_csv('Motor_Vehicle_Crashes_-_Individual_Information__Three_Year_Window.csv')
df_individual_information = pd.read_csv('datasets/Motor_Vehicle_Crashes_-_Individual_Information__Three_Year_Window.csv')
#dropping the columns not needed
df_individual_information = df_individual_information.drop(
    columns = ['Victim Status', 'Role Type', 'Seating Position', 'Ejection', 'License State Code',
                'Transported By', 'Injury Descriptor', 'Injury Location'])
#renaming columns
old = ['Year', 'Case Individual ID', 'Case Vehicle ID', 'Sex', 'Safety Equipment', 'Injury Severity', 'Age']
new = ['accident_year',	'case_individual_id', 'case_vehicle_id', 'sex',	'safety_equipment',	'injury_severity',
	    'age']
df_individual_information = renamecolumn(df_individual_information,old,new)
df_individual_information = df_individual_information.drop_duplicates(subset=['case_individual_id'])
df_individual_information = df_individual_information.set_index('case_individual_id', drop=True)


#vehicle information dataset
#df_vehicle_information = dd.read_csv('Motor_Vehicle_Crashes_-_Vehicle_Information__Three_Year_Window.csv')
df_vehicle_information = pd.read_csv('datasets/Motor_Vehicle_Crashes_-_Vehicle_Information__Three_Year_Window.csv')
#dropping the columns not needed
df_vehicle_information = df_vehicle_information.drop(
    columns = ['Action Prior to Accident', 'Type / Axles of Truck or Bus', 'Direction of Travel', 'Fuel Type'])
#renaming columns
old = ['Year', 'Case Vehicle ID', 'Vehicle Body Type', 'Registration Class',
       'Vehicle Year', 'State of Registration', 'Number of Occupants',
       'Engine Cylinders', 'Vehicle Make', 'Contributing Factor 1',
       'Contributing Factor 1 Description', 'Contributing Factor 2',
       'Contributing Factor 2 Description', 'Event Type', 'Partial VIN']
new = ['accident_year',	'case_vehicle_id',	'vehicle_body_type', 'registration_class', 'vehicle_year',
	    'state_of_registration', 'number_of_occupants',	'engine_cylinders',	'vehicle_make',
	    'contributing_factor_1', 'contributing_factor_1_description', 'contributing_factor_2',
	    'contributing_factor_2_description', 'event_type', 'partial_vin']
df_vehicle_information = renamecolumn(df_vehicle_information,old,new)
df_vehicle_information = df_vehicle_information.drop_duplicates(subset=['case_vehicle_id'])
df_vehicle_information = df_vehicle_information.set_index('case_vehicle_id',drop=True)


#violation information dataset
#df_violation_information = dd.read_csv('Motor_Vehicle_Crashes_-_Violation_Information__Three_Year_Window.csv')
df_violation_information = pd.read_csv('datasets/Motor_Vehicle_Crashes_-_Violation_Information__Three_Year_Window.csv')
#normalizing violation code and violation description
df_violation_code_description = df_violation_information[['Violation Code', 'Violation Description']].copy()
df_violation_code_description = df_violation_code_description.drop_duplicates()
df_violation_code_description = renamecolumn(df_violation_code_description,['Violation Code', 
    'Violation Description'],['violation_code', 'violation_description'])
df_violation_code_description = df_violation_code_description.drop_duplicates(subset=['violation_code'])
df_violation_code_description = df_violation_code_description.set_index('violation_code',drop=True)
#dropping colums that are not needed
df_violation_information = df_violation_information.drop(columns = ['Violation Description'])
#renaming columns
old = ['Year', 'Violation Code', 'Case Individual ID']
new = ['accident_year',	'violation_code', 'case_individual_id']
df_violation_information = renamecolumn(df_violation_information,old,new)
df_violation_information = df_violation_information.drop_duplicates(subset=['violation_code'])
df_violation_information = df_violation_information.set_index('violation_code',drop=True)


engine = create_engine('postgresql+psycopg2://dmvadmin:dmvadmin@localhost:5432/motorvehiclecrashes')


df_individual_information.to_sql('individual_information', engine, if_exists='append')
df_vehicle_information.to_sql('vehicle_information', engine, if_exists='append')
df_violation_information.to_sql('violation_information', engine, if_exists='append')
df_violation_code_description.to_sql('violation_code_description', engine, if_exists='append')

#con = engine.connect()
print(engine.table_names())
print("success")
#df_case_information.to_sql('case_information', engine , if_exists='replace')

"""
#inserting case_information data to MongoDB
#csvfile = open('Motor_Vehicle_Crashes_-_Case_Information__Three_Year_Window.csv', 'r')
#reader = csv.DictReader( csvfile )
df_case_information = pd.read_csv('datasets/Motor_Vehicle_Crashes_-_Case_Information__Three_Year_Window.csv')
df_case_information = df_case_information.drop(
    columns = ['Day of Week','Police Report','Traffic Control Device','DOT Reference Marker Location',
                'Pedestrian Bicyclist Action'])
#renaming columns
old = ['Year', 'Crash Descriptor', 'Time', 'Date', 'Lighting Conditions', 'Municipality', 
        'Collision Type Descriptor', 'County Name', 'Road Descriptor', 'Weather Conditions', 
        'Road Surface Conditions', 'Event Descriptor', 'Number of Vehicles Involved']
new = ['accident_year',	'crash_descriptor',	'time_of_accident',	'date_of_accident',	'lighting_conditions',	
	    'municipality',	'collision_type_descriptor', 'county_name',	'road_descriptor',	'weather_conditions',
	    'road_surface_conditions',	'event_descriptor',	'number_of_vehicles_involved']
df_case_information = renamecolumn(df_case_information,old,new)
mongo_client=MongoClient("mongodb://localhost:27017/") 
db=mongo_client.case_information
db.case_data.drop()
data = df_case_information.to_dict('records')
db.case_data.insert_many(data)