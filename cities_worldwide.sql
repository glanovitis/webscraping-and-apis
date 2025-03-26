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

-- Create the 'cities' table
CREATE TABLE cities (
    city_id INT AUTO_INCREMENT,
    city_name VARCHAR(255) NOT NULL UNIQUE,
    PRIMARY KEY (city_id)
);

-- Create the 'city_data' table
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

--  Create the airports table
CREATE TABLE `airports` (
  `icao` varchar(255),
  `iata` varchar(255),
  `name` varchar(255),
  `shortName` varchar(255),
  `municipalityName` varchar(255),
  `countryCode` varchar(255),
  `timeZone` varchar(255),
  `location_lat` decimal(10,6) DEFAULT NULL,
  `location_lon` decimal(10,6) DEFAULT NULL,
  `city_id` int DEFAULT NULL,
  KEY `city_id` (`city_id`),
  CONSTRAINT `airports_ibfk_1` FOREIGN KEY (`city_id`) REFERENCES `cities` (`city_id`)
);

-- create the weather_data table
CREATE TABLE `weather_data` (
  `city_id` int DEFAULT NULL,
  `date` datetime DEFAULT NULL,
  `temperature` decimal(10,0) DEFAULT NULL,
  `weather` varchar(255),
  `rain` decimal(10,0) DEFAULT NULL,
  KEY `city_id` (`city_id`),
  CONSTRAINT `weather_data_ibfk_1` FOREIGN KEY (`city_id`) REFERENCES `cities` (`city_id`)
);

-- create the arrivals table
CREATE TABLE `arrivals` (
  `number` varchar(255),
  `status` varchar(255),
  `arrival_scheduledTime_utc` DATETIME,
  `arrival_scheduledTime_local` DATETIME,
  `arrival_revisedTime_utc` DATETIME,
  `arrival_revisedTime_local` DATETIME,
  `arrival_terminal` varchar(255),
  `arrival_gate` varchar(255),
  `callSign` varchar(255),
  `departure_scheduledTime_utc` DATETIME,
  `departure_scheduledTime_local` DATETIME,
  `departure_revisedTime_utc` DATETIME,
  `departure_revisedTime_local` DATETIME,
  `departure_runwayTime_utc` DATETIME,
  `departure_runwayTime_local` DATETIME,
  `iata` varchar(255),
  `arrival_runwayTime_utc` DATETIME,
  `arrival_runwayTime_local` DATETIME
);

-- create the departures table
CREATE TABLE `departures` (
  `number` varchar(255),
  `status` varchar(255),
  `departure_scheduledTime_utc` DATETIME,
  `departure_scheduledTime_local` DATETIME,
  `departure_revisedTime_utc` DATETIME,
  `departure_revisedTime_local` DATETIME,
  `callSign` varchar(255),
  `iata` varchar(255)
);
