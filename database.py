import psycopg2
import psycopg2.extras
from pandas import DataFrame
import pandas as pd
from pymongo import MongoClient
pd.set_option('display.max_rows', None)

conn = psycopg2.connect(user="dmvadmin",
                        password="dmvadmin",
                        host="127.0.0.1",
                        port="5432",
                        database="motorvehiclecrashes")


def most_common_factor():
    cursor1 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = "SELECT contributing_factor_1 AS collision_factor, COUNT(*) num_collisions FROM vehicle_information GROUP BY 1 ORDER BY num_collisions DESC"
    cursor1.execute(query)
    dict_records = cursor1.fetchall()
    res = DataFrame(dict_records, columns = ["collision_factor", "num_collisions"])
    return res


def countyinformation(county_name, accident_year):
    client = MongoClient("mongodb://localhost:27017/")
    db = client.case_information
    collection = db["case_data"]
    query = {"county_name": county_name, "accident_year": accident_year}
    result = list(collection.find(query, {"_id": 0, "accident_year": 0}))
    res = DataFrame(result)
    return res


def male_female_count():
    cursor1 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = "SELECT accident_year, sex, COUNT(sex) AS number_of_crashes FROM individual_information WHERE sex = 'M' OR sex = 'F' GROUP BY accident_year, sex"
    cursor1.execute(query)
    dict_records = cursor1.fetchall()
    res = DataFrame(dict_records ,columns = ['accident_year','sex','number_of_crashes'])
    return res


def crahses_by_vehicle_make(make):
    cursor1 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = "SELECT ind_info.accident_year, veh_info.vehicle_make, COUNT(veh_info.vehicle_make) AS number_of_collisions FROM individual_information AS ind_info LEFT JOIN vehicle_information AS veh_info ON ind_info.case_vehicle_id = veh_info.case_vehicle_id WHERE veh_info.vehicle_make ILIKE %(make)s GROUP BY ind_info.accident_year, veh_info.vehicle_make"
    params = {
        'make': make,
    }
    cursor1.execute(query, params)
    dict_records = cursor1.fetchall()
    res = DataFrame(dict_records, columns = ['accident_year','vehicle_make','number_of_collisions'])
    return res


def insert_into_vehicle(accident_year, case_vehicle_id,	vehicle_body_type, registration_class, vehicle_year,
                        state_of_registration, number_of_occupants, engine_cylinders, vehicle_make, contributing_factor_1,
                        contributing_factor_1_description, contributing_factor_2, contributing_factor_2_description, event_type, partial_vin):
    try:
        cursor1 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        query1 = "INSERT INTO vehicle_information VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor1.execute(query1, (accident_year, case_vehicle_id,	vehicle_body_type, registration_class, vehicle_year, state_of_registration, number_of_occupants, engine_cylinders,
                                vehicle_make, contributing_factor_1, contributing_factor_1_description, contributing_factor_2, contributing_factor_2_description, event_type, partial_vin))
        conn.commit()
        return print("Inserted Successfully")
    except Exception as e:
        print("Error")
        print(str(e))
        return print("There was an error, please check the input variables and constraints.")


def type_of_collision():
    cursor1 = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = "SELECT veh_info.accident_year, veh_info.case_vehicle_id, ind_info.case_individual_id, vio_info.violation_code,	ind_info.sex, ind_info.age,	vio_code_desc.violation_description, veh_info.contributing_factor_1, veh_info.contributing_factor_1_description, veh_info.contributing_factor_2, veh_info.contributing_factor_2_description FROM vehicle_information AS veh_info INNER JOIN individual_information AS ind_info ON veh_info.case_vehicle_id = ind_info.case_vehicle_id INNER JOIN violation_information AS vio_info ON ind_info.case_individual_id = vio_info.case_individual_id	INNER JOIN violation_code_description AS vio_code_desc ON vio_info.violation_code = vio_code_desc.violation_code ORDER BY veh_info.accident_year"
    cursor1.execute(query)
    dict_records = cursor1.fetchall()
    res = DataFrame(dict_records, columns = ['accident_year','case_vehicle_id','case_individual_id','violation_code','sex','age','violation_description','contributing_factor_1','contributing_factor_1_description','contributing_factor_2','contributing_factor_2_description'])
    return res
