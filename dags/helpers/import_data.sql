INSERT INTO dim_age_group(age)
SELECT DISTINCT ISNULL("AGE_GROUP", 'UNKNOWN') AS age FROM staging_arrests;

INSERT INTO dim_race(race)
SELECT DISTINCT ISNULL("PERP_RACE", 'UNKNOWN') AS dim_race FROM staging_arrests;

INSERT INTO dim_radio_code(code, description, category)
SELECT DISTINCT ON (code)
    ISNULL("RADIO_CODE"::TEXT, 'UNKNOWN') AS code,
    ISNULL("TYP_DESC"::TEXT, 'UNKNOWN') AS description,
    ISNULL("CIP_JOBS"::TEXT, 'UNKNOWN') AS category
FROM staging_calls;

INSERT INTO dim_precinct_patrol(patrol)
SELECT DISTINCT ON(a."PATRL_BORO_NM") ISNULL(a."PATRL_BORO_NM", 'UNKNOWN') AS patrol 
FROM staging_calls AS a ORDER BY a."PATRL_BORO_NM"
;

INSERT INTO dim_precinct_patrol(patrol)
SELECT DISTINCT ON (a."Patrol Borough Name") ISNULL(a."Patrol Borough Name", 'UNKNOWN') AS patrol 
FROM staging_hate AS a
WHERE ISNULL(a."Patrol Borough Name", 'UNKNOWN') NOT IN (SELECT patrol FROM dim_precinct_patrol)
ORDER BY "Patrol Borough Name";

INSERT INTO dim_legal_code (code) 
SELECT DISTINCT ISNULL("LAW_CODE"::TEXT, 'UNKNOWN') AS code FROM staging_arrests;

--->>> INSERT HATE RECORDS ------------------------------------------------------------
INSERT INTO dim_legal_category (description, category) SELECT 'UNKNOWN', 'ALL';
INSERT INTO dim_legal_category (description, category)
SELECT description, 'BIAS' FROM 
(
    SELECT DISTINCT ISNULL("Bias Motive Description", 'UNKNOWN') AS description FROM staging_hate
) AS r WHERE ISNULL(r.description, 'UNKNOWN') NOT IN (SELECT description FROM dim_legal_category)
;

INSERT INTO dim_legal_category (description, category)
SELECT description, 'OFFENCE CATEGORY' FROM 
(
    SELECT DISTINCT UPPER(a."Offense Category") AS description FROM staging_hate a
) AS r WHERE r.description NOT IN (SELECT description FROM dim_legal_category)
;

INSERT INTO dim_legal_category (description, category)
SELECT description, 'OFFENSE DESCRIPTION' FROM 
(
    SELECT DISTINCT ISNULL("Offense Description", 'UNKNOWN') AS description FROM staging_hate
) AS r WHERE ISNULL(r.description, 'UNKNOWN') NOT IN (SELECT description FROM dim_legal_category)
;

INSERT INTO dim_legal_category (description, category)
SELECT description, 'LAW CODE' FROM 
(
    SELECT DISTINCT a."Law Code Category Description" AS description FROM staging_hate a
) AS r WHERE r.description NOT IN (SELECT description FROM dim_legal_category)
;

INSERT INTO dim_legal_category (description, category)
SELECT description, 'PD CODE DESC' FROM 
(
    SELECT DISTINCT ISNULL("PD Code Description", 'UNKNOWN') AS description FROM staging_hate
) AS r WHERE ISNULL(r.description, 'UNKNOWN') NOT IN (SELECT description FROM dim_legal_category)
;

--->>> INSERT ARRESTS ---------------------------------------------------------------
INSERT INTO dim_legal_category (description, category)
SELECT description, 'PD CODE DESC' FROM 
(
    SELECT DISTINCT "PD_DESC" AS description FROM staging_arrests
) AS r WHERE ISNULL(r.description, 'UNKNOWN') NOT IN (SELECT description FROM dim_legal_category)
;

INSERT INTO dim_legal_category (description, category)
SELECT description, 'OFFENSE DESCRIPTION' FROM 
(
    SELECT DISTINCT "OFNS_DESC" AS description FROM staging_arrests
) AS r WHERE ISNULL(r.description, 'UNKNOWN') NOT IN (SELECT description FROM dim_legal_category)
;

--->>> INSERT CALLS -----------------------------------------------------------------
INSERT INTO dim_legal_category (description, category)
SELECT description, 'CALL REASON'  FROM 
(
    SELECT DISTINCT ISNULL("TYP_DESC"::TEXT, 'UNKNOWN') AS description FROM staging_calls
) AS r WHERE ISNULL(r.description, 'UNKNOWN') NOT IN (SELECT description FROM dim_legal_category)
;

INSERT INTO dim_precinct
SELECT 
    "OBJECTID"::INTEGER                 AS key, 
    UPPER(ISNULL("Name",    'UNKNOWN')) AS place, 
    UPPER(ISNULL("Borough", 'UNKNOWN')) AS borough,
    "latitude"::TEXT                    AS latitude, 
    "longitude"::TEXT                   AS longitude
FROM staging_suburbs AS a;

--->>> INSERT SHOOTING --------------------------------------------------------------
INSERT INTO dim_location_desc(description)
SELECT DISTINCT ON (a."LOC_CLASSFCTN_DESC") ISNULL(a."LOC_CLASSFCTN_DESC", 'UNKNOWN') AS description 
FROM staging_shooting AS a ORDER BY a."LOC_CLASSFCTN_DESC"
;

INSERT INTO dim_location_desc(description)
SELECT DISTINCT ON(a."LOCATION_DESC") ISNULL(a."LOCATION_DESC", 'UNKNOWN') AS description FROM staging_shooting a
WHERE ISNULL(a."LOCATION_DESC", 'UNKNOWN') NOT IN (SELECT description FROM dim_location_desc)
ORDER BY a."LOCATION_DESC";

--->>> INSERT LOCATION --------------------------------------------------------------
INSERT INTO dim_location (longitude, latitude)
SELECT
    DISTINCT ON (a."Longitude", a."Latitude")
    ISNULL(a."Longitude"::TEXT,  '0') AS longitude,
    ISNULL(a."Latitude"::TEXT,   '0') AS latitude
FROM staging_shooting AS a
WHERE (
    a."Longitude"::TEXT NOT IN (SELECT longitude::TEXT FROM dim_location) AND
    a."Latitude"::TEXT  NOT IN (SELECT latitude::TEXT  FROM dim_location)
);
--->>> INSERT
INSERT INTO dim_location (longitude, latitude)
SELECT
    DISTINCT ON (a."Longitude", a."Latitude")
    ISNULL(a."Longitude"::TEXT,    '0') AS longitude,
    ISNULL(a."Latitude"::TEXT,     '0') AS latitude
FROM staging_arrests AS a
LEFT JOIN dim_precinct AS b ON b.key::TEXT = ISNULL(a."ARREST_PRECINCT"::TEXT, 'UNKNOWN')
WHERE (
    a."Longitude"::TEXT NOT IN (SELECT longitude::TEXT FROM dim_location) AND
    a."Latitude"::TEXT  NOT IN (SELECT latitude::TEXT  FROM dim_location)
);

--->>> INSERT
INSERT INTO dim_location(longitude, latitude)
SELECT DISTINCT ON (a."longitude", a."latitude")
    ISNULL(a."longitude"::TEXT,    '0')  AS longitude,
    ISNULL(a."latitude"::TEXT,     '0')  AS latitude
FROM staging_suburbs a
WHERE (
    a."longitude"::TEXT NOT IN (SELECT longitude::TEXT FROM dim_location) AND
    a."latitude"::TEXT  NOT IN (SELECT latitude::TEXT  FROM dim_location)
);
--->>> INSERT
INSERT INTO dim_location (longitude, latitude)
SELECT
    DISTINCT ON("Longitude", "Latitude")
    ISNULL("Longitude"::TEXT,     '0') AS longitude,
    ISNULL("Latitude"::TEXT,      '0') AS latitude
FROM staging_calls;

--->>> INSERT CALLS -----------------------------------------------------------------
INSERT INTO facts_call
(id, created, incident, patrol_key, radio_key, police_code, recieved, dispached, arrived, closing, location_key, x_coord, y_coord)
SELECT DISTINCT ON ( a."CAD_EVNT_ID")
    a."CAD_EVNT_ID" AS id,
    a."CREATE_DATE"::DATE AS created,
    TO_TIMESTAMP(CONCAT(
        a."INCIDENT_DATE"::TEXT, ' ',
        a."INCIDENT_TIME"::TEXT), 
        'MM/DD/YYYY HH24:MI:SS') AS incident,
    r."key" AS radio_key,
    ISNULL(a."NYPD_PCT_CD"::INTEGER, 0) AS police_code,
    p."key" AS patrol_key,

    a."ADD_TS"::TIMESTAMP AS recieved,
    a."DISP_TS"::TIMESTAMP AS dispached,
    ISNULL(a."ARRIVD_TS"::TIMESTAMP, '01/01/1900'::TIMESTAMP) AS arrived,
    a."CLOSNG_TS"::TIMESTAMP AS closing,

    ISNULL(l."key", 0) AS location_key,
    a."GEO_CD_X"::INTEGER  AS x_coord,
    a."GEO_CD_Y"::INTEGER  AS y_coord
FROM staging_calls AS a
LEFT JOIN dim_location AS l ON 
    l."longitude"::TEXT = a."Longitude"::TEXT AND
    l."latitude"::TEXT = a."Latitude"::TEXT
LEFT JOIN dim_radio_code AS r ON r.code = ISNULL(a."RADIO_CODE", 'UNKNOWN')
LEFT JOIN dim_precinct_patrol AS p ON p."patrol" = ISNULL(a."PATRL_BORO_NM", 'UNKNOWN');

--->>> INSERT ARRESTS----------------------------------------------------------------
INSERT INTO facts_arrest(arrest_key, arrested, offence_key, law_code_key, law_category_code, juris_code, age_key, race_key, sex, borrow_code, precinct_key, x_coord, y_coord)
SELECT 
    a."ARREST_KEY"                    AS arrest_key,  
    a."ARREST_DATE"::DATE             AS arrested,
    
    ISNULL(a."PD_CD"::INTEGER, 0)     AS offence_key,
    l."key"                           AS law_code_key,
    ISNULL(a."LAW_CAT_CD", 'UNKNOWN') AS law_category_code, 
    a."JURISDICTION_CODE"             AS juris_code,

    g."key"                           AS age_key,
    r."key"                           AS race_key,
    a."PERP_SEX"::CHAR(1)             AS sex,

    a."ARREST_BORO"                   AS borrow_code,
    a."ARREST_PRECINCT"               AS precinct_key,

    a."X_COORD_CD"                    AS x_coord,  
    a."Y_COORD_CD"                    AS y_coord
FROM staging_arrests AS a
LEFT JOIN dim_age_group g ON g."age"::TEXT      = ISNULL(a."AGE_GROUP"::TEXT, 'UNKNOWN')
LEFT JOIN dim_legal_code AS l ON l."code"::TEXT = ISNULL(a."LAW_CODE"::TEXT,  'UNKNOWN')
LEFT JOIN dim_race AS r ON r."race"::TEXT       = ISNULL(a."PERP_RACE"::TEXT, 'UNKNOWN')
;