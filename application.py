import database
import pandas as pd
#pd.set_option('display.max_columns', None)
#pd.set_option('display.width', 400)


while True:
    print("Select from the following options: ")
    choice = input("1. Most common factor for motor vehicle crashes in New York state \n2. Number of crashes corresponding to  different vehicle manufacturers in the year 2014, 2015, and 2016 \n3. Number of males and females that met with an accident during the years 2014, 2015, and 2016 in New York State \n4. Information corresponding to the individual that is engaged in a particular type of collision \n5. Crashes information based on the county and year \n6. Insert data into the vehicle information table\n")

    # Most common factor for motor vehicle crashes in New York state
    if choice == '1':
        res = database.most_common_factor()
        print(res)

    # Number of crashes corresponding to  different vehicle manufacturers in the year 2014, 2015, and 2016
    elif choice == '2':
        make = input("Enter vehicle make: ")
        res = database.crahses_by_vehicle_make(make)
        print(res)

    # Number of males and females that met with an accident during the years 2014, 2015, and 2016 in New York State
    elif choice == '3':
        res = database.male_female_count()
        print(res)

    # Information corresponding to the individual that is engaged in a particular type of collision
    # This query joins individual_information, vehicle_information, violation_information, and violation_code_description tables.
    elif choice == '4':
        res = database.type_of_collision()
        print(res)

    # Crashes information based on the county and year
    # This query uses MongoDB database
    elif choice == '5':
        county_name = input("Enter a county name: ")
        accident_year = int(input("Enter year (2014/2015/2016): "))
        res = database.countyinformation(county_name, accident_year)
        print(res)


    #Insert data into the vehicle information table
    elif choice == '6':
        accident_year, case_vehicle_id,	vehicle_body_type, registration_class, vehicle_year, state_of_registration,number_of_occupants,	engine_cylinders, vehicle_make, contributing_factor_1, contributing_factor_1_description, contributing_factor_2, contributing_factor_2_description, event_type, partial_vin = input("Enter values in the following order: \n accident_year, case_vehicle_id, vehicle_body_type, registration_class, vehicle_year,  state_of_registration, number_of_occupants, engine_cylinders, vehicle_make,  contributing_factor_1, contributing_factor_1_description, contributing_factor_2, contributing_factor_2_description, event_type, partial_vin \n").split(',')
        accident_year = int(accident_year)
        case_vehicle_id = int(case_vehicle_id)
        number_of_occupants = int(number_of_occupants)
        engine_cylinders = int(engine_cylinders)

        database.insert_into_vehicle(accident_year, case_vehicle_id, vehicle_body_type, registration_class, vehicle_year, 
        state_of_registration, number_of_occupants, engine_cylinders, vehicle_make, contributing_factor_1, contributing_factor_1_description, contributing_factor_2, contributing_factor_2_description, event_type, partial_vin)

    else:
        print("Incorrect choice")
    while True:
        answer = str(input('Run again? (Y/N): '))
        if answer in ('Y', 'N'):
            break
        print("invalid input.")
    if answer == 'Y':
        continue
    else:
        break
