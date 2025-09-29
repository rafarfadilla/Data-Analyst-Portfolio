-- DATA CLEANING

SELECT *
FROM project.mineral_ores;

-- 1. Remove Duplicates
-- 2. Standardize Data
-- 3. NULL Values or Blank Values
-- 4. Remove Any Columns

-- Remove Duplicates

CREATE TABLE project.mineral_ores2
LIKE project.mineral_ores;

SELECT *
FROM project.mineral_ores3;

INSERT project.mineral_ores2
SELECT *
FROM project.mineral_ores;

SELECT site_name, latitude, longitude, region, country, state, county,
         com_type, commod1, commod2, commod3, oper_type, dep_type,
         prod_size, dev_stat, ore, gangue, work_type, names,
         ore_ctrl, hrock_type, arock_type, 
         COUNT(*) AS cnt
FROM project.mineral_ores2
GROUP BY site_name, latitude, longitude, region, country, state, county,
         com_type, commod1, commod2, commod3, oper_type, dep_type,
         prod_size, dev_stat, ore, gangue, work_type, names,
         ore_ctrl, hrock_type, arock_type
HAVING COUNT(*) > 1;

SELECT site_name, COUNT(*) AS cnt
FROM project.mineral_ores2
GROUP BY site_name
HAVING COUNT(*) > 1;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY site_name, latitude, longitude, region, country, state, county,
         com_type, commod1, commod2, commod3, oper_type, dep_type,
         prod_size, dev_stat, ore, gangue, work_type, names,
         ore_ctrl, hrock_type, arock_type) AS row_num
FROM project.mineral_ores2;

ALTER TABLE project.mineral_ores2
DROP COLUMN id;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY site_name, latitude, longitude, region, country, state, county,
         com_type, commod1, commod2, commod3, oper_type, dep_type,
         prod_size, dev_stat, ore, gangue, work_type, names,
         ore_ctrl, hrock_type, arock_type) AS row_num
FROM project.mineral_ores2
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM project.mineral_ores2
WHERE site_name = 'Unknown'
AND latitude = 33.79896
AND longitude = -113.72267 
AND county = 'La Paz'
AND commod1 = 'Copper, Gold, Silver'
AND dev_stat = 'Past Producer';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY site_name, latitude, longitude, region, country, state, county,
         com_type, commod1, commod2, commod3, oper_type, dep_type,
         prod_size, dev_stat, ore, gangue, work_type, names,
         ore_ctrl, hrock_type, arock_type) AS row_num
FROM project.mineral_ores2
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE project.mineral_ores3 (
  `site_name` text,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `region` text,
  `country` text,
  `state` text,
  `county` text,
  `com_type` text,
  `commod1` text,
  `commod2` text,
  `commod3` text,
  `oper_type` text,
  `dep_type` text,
  `prod_size` text,
  `dev_stat` text,
  `ore` text,
  `gangue` text,
  `work_type` text,
  `names` text,
  `ore_ctrl` text,
  `hrock_type` text,
  `arock_type` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM project.mineral_ores3
WHERE country = 'Indonesia';

INSERT INTO project.mineral_ores3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY site_name, latitude, longitude, region, country, state, county,
         com_type, commod1, commod2, commod3, oper_type, dep_type,
         prod_size, dev_stat, ore, gangue, work_type, names,
         ore_ctrl, hrock_type, arock_type) AS row_num
FROM project.mineral_ores2;

SET SQL_SAFE_UPDATES = 0;

SELECT country, state, latlong, COUNT(*) AS cnt
FROM project.mineral_ores3
GROUP BY country, state, latlong
HAVING COUNT(*) = 1;

ALTER TABLE project.mineral_ores3
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;

DELETE FROM project.mineral_ores3
WHERE id NOT IN (
  SELECT id FROM (
    SELECT id,
           ROW_NUMBER() OVER (
             PARTITION BY country, state, latlong
             ORDER BY id
           ) AS rn
    FROM project.mineral_ores3
  ) x
  WHERE rn = 1
);

SELECT 
    commodities,
    ore,
    prod_size,
	country, state,
    oper_type, 
    COUNT(*) AS jumlah
FROM project.mineral_ores3
WHERE ore = 'Unknown' OR oper_type = 'Unknown'
GROUP BY commodities, ore, prod_size,country, state, oper_type
ORDER BY jumlah DESC;

UPDATE project.mineral_ores3
SET ore = 'Galena'
WHERE commodities = 'Lead' AND oper_type = 'Underground' AND ore = 'Unknown';

-- Standardizing Data

SELECT *
FROM project.mineral_ores3
WHERE country = 'Australia';

UPDATE project.mineral_ores3
SET country = 'South Korea'
WHERE country = 'Korea, South';

UPDATE project.mineral_ores3
SET country = 'North Korea'
WHERE country = 'Korea, North';

UPDATE project.mineral_ores3
SET country = 'The Bahamas'
WHERE country = 'Bahamas, the';

UPDATE project.mineral_ores3
SET country = 'The Gambia'
WHERE country = 'Gambia, the';

UPDATE project.mineral_ores3
SET country = 'Myanmar'
WHERE country = 'Burma';

UPDATE project.mineral_ores3
SET country = 'Eswatini'
WHERE country = 'Swaziland';

UPDATE project.mineral_ores3
SET country = 'Ivory Coast'
WHERE country = 'Cote D''Ivoire';

UPDATE project.mineral_ores3
SET country = 'North Macedonia'
WHERE country = 'Macedonia';

UPDATE project.mineral_ores3
SET country = 'Democratic Republic of the Congo'
WHERE country = 'Congo (Kinshasa)';

UPDATE project.mineral_ores3
SET country = 'Republic of the Congo'
WHERE country = 'Congo (Brazzaville)';

UPDATE project.mineral_ores3
SET country = 'Kazakhstan'
WHERE country = 'Russia, Kazakhstan';

UPDATE project.mineral_ores3
SET region = 'AS'
WHERE region = 'EU, AS';

UPDATE project.mineral_ores3
SET region = 'NA'
WHERE region = '';

UPDATE project.mineral_ores3
SET country = 'United States'
WHERE country = '';

UPDATE project.mineral_ores3
SET state = 'Unknown'
WHERE state IS NULL OR state = '';

UPDATE project.mineral_ores3
SET site_name = 'Unknown'
WHERE site_name IS NULL OR site_name = '';

UPDATE project.mineral_ores3
SET country = TRIM(country);

UPDATE project.mineral_ores3
SET com_type = UPPER(com_type);

-- NULL Values or Blank Values

SELECT DISTINCT commodities
FROM project.mineral_ores3;

UPDATE project.mineral_ores3
SET com_type = 'N'
WHERE com_type = '' AND commod1 = 'Diatomite';

UPDATE project.mineral_ores3
SET gangue = 'Unknown'
WHERE gangue = '';

UPDATE project.mineral_ores3
SET com_type = 'M'
WHERE com_type = '' AND commod1 LIKE '%Zinc%';

-- Drop Columns

ALTER TABLE project.mineral_ores3
DROP COLUMN dep_type,
DROP COLUMN county,
DROP COLUMN names,
DROP COLUMN ore_ctrl,
DROP COLUMN row_num;

ALTER TABLE project.mineral_ores3
ADD COLUMN commodities VARCHAR(255);

UPDATE project.mineral_ores3
SET commodities = TRIM(BOTH ', ' FROM REPLACE(commodities, ', ,', ','));

ALTER TABLE project.mineral_ores3
ADD COLUMN latlong VARCHAR(100);

UPDATE project.mineral_ores3
SET latlong = CONCAT(latitude, ',', longitude);

DELETE
FROM project.mineral_ores3
WHERE com_type = 'U';

-- Exploratory Data Analysis

DESCRIBE project.mineral_ores3;
SELECT COUNT(*) AS total_rows FROM project.mineral_ores3; 

SELECT oper_type, COUNT(*) AS jumlah
FROM project.mineral_ores3
GROUP BY oper_type
ORDER BY jumlah DESC;

SELECT prod_size, COUNT(*) AS jumlah
FROM project.mineral_ores3
GROUP BY prod_size
ORDER BY jumlah DESC;

SELECT region, country, state, commodities, COUNT(*) AS jumlah
FROM project.mineral_ores3
GROUP BY region, country, state, commodities
ORDER BY jumlah DESC;

SELECT com_type, prod_size, COUNT(*) AS jumlah
FROM project.mineral_ores3
GROUP BY com_type, prod_size
ORDER BY com_type, jumlah DESC;

SELECT prod_size, COUNT(*) AS jumlah
FROM project.mineral_ores3
WHERE dev_stat = 'Producer'
GROUP BY prod_size;

INSERT INTO project.mineral_ores3 (site_name, latitude, longitude, region, country, state, com_type, commod1, oper_type, prod_size, dev_stat, ore, gangue, work_type, hrock_type, arock_type, commodities, latlong)
VALUES ('Woods and Party', -42.66279, 146.33176, 'OC',	'Australia', 'Tasmania', 'M', 'Iridium, Chromium, Osmium', 'Unknown', 'N', 'Prospect', 'Chromite', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Iridium, Chromium, Osmium', '-42.66279,146.33176');