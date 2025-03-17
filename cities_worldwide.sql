-- Drop the database if it already exists
DROP DATABASE IF EXISTS cities_worldwide;

-- Create the database
CREATE DATABASE cities_worldwide;

-- Use the database
USE cities_worldwide;

-- Create the 'countries' table
CREATE TABLE countries (
    country_id INT AUTO_INCREMENT,
    country VARCHAR(255) NOT NULL UNIQUE,
    PRIMARY KEY (country_id)
);

-- Create the 'citiy' table
CREATE TABLE cities (
    city_id INT AUTO_INCREMENT,
    city_name VARCHAR(255) NOT NULL UNIQUE,
    PRIMARY KEY (city_id)
);

-- Create the 'cities' table
CREATE TABLE city_data (
    city_id INT,
    country_id INT,
    population BIGINT NOT NULL,
    population_year INT NOT NULL,
    longitude_decimal DECIMAL(10,6) NOT NULL,
    latitude_decimal DECIMAL(10,6) NOT NULL,
    FOREIGN KEY (country_id) REFERENCES countries(country_id),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

SELECT * FROM countries;
SELECT * FROM cities;
SELECT * FROM city_data;