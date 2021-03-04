DROP TABLE IF EXISTS individual_information;
DROP TABLE IF EXISTS vehicle_information;
DROP TABLE IF EXISTS violation_code_description;
DROP TABLE IF EXISTS violation_information;


CREATE TABLE vehicle_information(
	accident_year INT,
	case_vehicle_id INT,
	vehicle_body_type VARCHAR(30),
	registration_class VARCHAR(50),
	vehicle_year INT,
	state_of_registration CHAR(2),
	number_of_occupants INT,
	engine_cylinders INT,
	vehicle_make VARCHAR(30),
	contributing_factor_1 VARCHAR(30),	
	contributing_factor_1_description TEXT,
	contributing_factor_2 VARCHAR(30),
	contributing_factor_2_description TEXT,
	event_type TEXT,
	partial_vin VARCHAR(20),
	PRIMARY KEY(case_vehicle_id)
);

CREATE TABLE individual_information(
	accident_year INT,	
	case_individual_id INT,	
	case_vehicle_id	INT,
	sex CHAR(1),
	safety_equipment TEXT,
	injury_severity	TEXT,
	age INT,
	PRIMARY KEY (case_individual_id),
	CONSTRAINT fk_individual FOREIGN KEY (case_vehicle_id) REFERENCES vehicle_information(case_vehicle_id)
);

CREATE TABLE violation_information (
	accident_year INT,
	violation_code VARCHAR(20),
	case_individual_id INT,
	PRIMARY KEY (violation_code)
);

CREATE TABLE violation_code_description(
	violation_description TEXT,
	violation_code VARCHAR(20),
	PRIMARY KEY (violation_code),
	CONSTRAINT fk_violat_desc FOREIGN KEY (violation_code) REFERENCES violation_description(violation_code)
);