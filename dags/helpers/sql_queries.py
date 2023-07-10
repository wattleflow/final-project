class SqlQueries:

    dim_shooting_location = ("""
TRUNCATE TABLE dim_shooting_location;
INSERT INTO dim_shooting_location(site, class, description)
SELECT
    DISTINCT ON(site, class, description)
    site, class, description
FROM (
    SELECT
        ISNULL("LOC_OF_OCCUR_DESC", 'UKNWON') AS site,
        ISNULL("LOC_CLASSFCTN_DESC", 'UNKWN') AS class, 
        ISNULL("LOCATION_DESC", 'UNKWN') AS description
    FROM staging_shootings
) AS r
ORDER BY site, class, description;
    """)

    dim_shooting_murder_flag = ("""
TRUNCATE TABLE dim_shooting_murder_flag;
INSERT INTO dim_shooting_murder_flag(key, flag)
SELECT
    DISTINCT ON(key, flag)
    key, flag
FROM (
    SELECT
        "INCIDENT_KEY" AS key,
        ISNULL("STATISTICAL_MURDER_FLAG"::BOOLEAN, False) AS flag
    FROM staging_shootings
) AS r
ORDER BY key
;
    """)

    dim_offender = ("""
TRUNCATE TABLE dim_offender;
INSERT INTO dim_offender(key, age, sex, race, source)
SELECT
    DISTINCT ON(key, age, sex, race)
    key, age, sex, race, source
FROM (
    SELECT
        "INCIDENT_KEY"                    AS key,
        ISNULL("PERP_AGE_GROUP", 'UNKWN') AS age, 
        ISNULL("PERP_SEX",           'U') AS sex, 
        ISNULL("PERP_RACE",      'UNKWN') AS race,
        'staging_shootings'               AS source
    FROM staging_shootings
) AS r
ORDER BY key, age, sex, race
;

INSERT INTO dim_offender(key, age, sex, race, source)
SELECT
    DISTINCT ON(key, age, sex, race)
    key, age, sex, race, source
FROM (
    SELECT
        "ARREST_KEY"                 AS key,
        ISNULL("AGE_GROUP", 'UNKWN') AS age,
        ISNULL("PERP_SEX",      'U') AS sex,
        ISNULL("PERP_RACE", 'UNKWN') AS race,
        'staging_arrests'            AS source
    FROM staging_arrests
) AS r
WHERE
    CONCAT(age, sex, race, source) NOT IN (SELECT CONCAT(age, sex, race, source) FROM dim_offender)
;
    """)

    dim_victim = ("""
TRUNCATE TABLE dim_victim;
INSERT INTO dim_victim(key, age, sex, race)
SELECT
    DISTINCT ON(key, age, sex, race)
    key, age, sex, race
FROM (
    SELECT
        "INCIDENT_KEY"                     AS key,
        ISNULL(a."VIC_AGE_GROUP", 'UNKWN') AS age, 
        ISNULL(a."VIC_SEX",           'U') AS sex, 
        ISNULL(a."VIC_RACE",      'UNKWN') AS race
    FROM staging_shootings AS a
) AS r
;
    """)
    
    dim_call_times = ("""
TRUNCATE TABLE dim_call_times;
INSERT INTO dim_call_times(key, added, dispatched, arrived, closed)
SELECT
    DISTINCT ON (key, added, dispatched, arrived, closed)
    key, added, dispatched, arrived, closed
FROM
(
    SELECT
        "CAD_EVNT_ID"          AS key,
        "ADD_TS"::TIMESTAMP    AS added, 
        "DISP_TS"::TIMESTAMP   AS dispatched, 
        "ARRIVD_TS"::TIMESTAMP AS arrived, 
        "CLOSNG_TS"::TIMESTAMP AS closed
    FROM staging_calls
) AS r
ORDER BY key, added
;
    """)
    
    dim_call_code = ("""
TRUNCATE TABLE dim_call_code;
INSERT INTO dim_call_code(code, description, category)
SELECT DISTINCT ON (code)
    ISNULL("RADIO_CODE", 'UNKWN') AS code,
    ISNULL("TYP_DESC",   'UNKWN') AS description,
    ISNULL("CIP_JOBS",   'UNKWN') AS category
FROM staging_calls;
    """)

    dim_precinct_patrol = ("""
TRUNCATE TABLE dim_precinct_patrol;
INSERT INTO dim_precinct_patrol(patrol)
SELECT DISTINCT ON(a."PATRL_BORO_NM") ISNULL(a."PATRL_BORO_NM", 'UNKWN') AS patrol 
FROM staging_calls AS a ORDER BY a."PATRL_BORO_NM";
    """)
    
    dim_arrest_legal = ("""
TRUNCATE TABLE dim_arrest_legal;
INSERT INTO dim_arrest_legal (description,offence,code,category)
SELECT DISTINCT ON (description,offence,code,category) description, offence, code, category FROM (
SELECT
    ISNULL("PD_DESC",   'UNKWN') AS description,
    ISNULL("OFNS_DESC", 'UNKWN') AS offence,
    ISNULL("LAW_CODE",  'UNKWN') AS code,
    ISNULL("LAW_CAT_CD",'UNKWN') AS category
FROM staging_arrests
) AS r
;
    """) 
    
    dim_precinct = ("""
TRUNCATE TABLE dim_precinct;
INSERT INTO dim_precinct
SELECT 
    DISTINCT
    "OBJECTID"::INTEGER               AS key, 
    UPPER(ISNULL("Name",    'UNKWN')) AS place, 
    UPPER(ISNULL("Borough", 'UNKWN')) AS borough,
    "the_geom"::TEXT                  AS point
FROM staging_suburbs AS a;
    """)

    dim_location = ("""
INSERT INTO dim_location(borough, x_coord, y_coord, point)
SELECT
    DISTINCT ON(borough, x_coord, y_coord)
    borough, x_coord, y_coord, point
FROM (
    SELECT
        ISNULL("BORO", 'UNKWN') AS borough, 
        "X_COORD_CD"::INTEGER AS x_coord, 
        "Y_COORD_CD"::INTEGER AS y_coord,
        CASE 
            WHEN ISNULL("Latitude", NULL) IS NULL THEN ''
            ELSE CONCAT('POINT (',"Longitude",' ', "Latitude" ,')')
        END AS point
    FROM staging_shootings
) AS r
ORDER BY borough, x_coord, y_coord;

INSERT INTO dim_location(borough, x_coord, y_coord, point)
SELECT
    DISTINCT ON(borough, x_coord, y_coord)
    borough, x_coord, y_coord, point
FROM (
    SELECT
        ISNULL("ARREST_BORO", 'UNKWN') AS borough,
        "X_COORD_CD"::INTEGER AS x_coord,
        "Y_COORD_CD"::INTEGER AS y_coord,
        ISNULL("New Georeferenced Column", 'UNKWN') AS point
    FROM staging_arrests
) AS r
WHERE
    CONCAT(borough, x_coord, y_coord) NOT IN (SELECT CONCAT(borough, x_coord, y_coord) FROM dim_location)
ORDER BY borough, x_coord, y_coord;

INSERT INTO dim_location(borough, x_coord, y_coord, point)
SELECT
    DISTINCT ON(borough, x_coord, y_coord)
    borough, x_coord, y_coord, point
FROM (
    SELECT
        ISNULL("BORO_NM", 'UNKWN') AS borough,
        "GEO_CD_X"::INTEGER AS x_coord,
        "GEO_CD_Y"::INTEGER AS y_coord,
        CASE 
            WHEN ISNULL("Latitude", NULL) IS NULL THEN ''
            ELSE CONCAT('POINT (',"Longitude",' ', "Latitude" ,')')
        END AS point
    FROM staging_calls
) AS r
WHERE
    CONCAT(borough, x_coord, y_coord) NOT IN (SELECT CONCAT(borough, x_coord, y_coord) FROM dim_location)
ORDER BY borough, x_coord, y_coord;
    """)

    facts_shooting = ("""
INSERT INTO facts_shooting(key, occured, precinct_key, location_key, site_id, jurisdiction_code, murder_flag)
SELECT
    DISTINCT ON(key)
    key, occured, precinct_key, l.id AS location_key, s.id AS site_id, jurisdiction_code, murder_flag
FROM
(

    SELECT
        "INCIDENT_KEY"                        AS key,
        TO_TIMESTAMP(CONCAT(
            "OCCUR_DATE"::TEXT, ' ',
            "OCCUR_TIME"::TEXT), 
            'MM/DD/YYYY HH24:MI:SS')          AS occured,
        ISNULL("LOC_OF_OCCUR_DESC", 'UNKWN')  AS location_site,
        ISNULL("PRECINCT"::INTEGER, -1)       AS precinct_key,
        ISNULL("BORO", 'UNKWN')               AS borough,
        ISNULL("LOC_CLASSFCTN_DESC", 'UNKWN') AS location_class,
        ISNULL("LOCATION_DESC", 'UNKWN')      AS location_descr,
        "X_COORD_CD"::INTEGER                 AS x_coord,
        "Y_COORD_CD"::INTEGER                 AS y_coord,
        ISNULL("Lon_Lat", 'UNKWN')            AS point,
        "JURISDICTION_CODE"::INTEGER          AS jurisdiction_code,
        "STATISTICAL_MURDER_FLAG"::BOOLEAN    AS murder_flag,
        ISNULL("PERP_AGE_GROUP", 'UNKWN')     AS perp_age,
        ISNULL("PERP_SEX", 'UNKWN')           AS perp_sex,
        ISNULL("PERP_RACE", 'UNKWN')          AS perp_race,
        ISNULL("VIC_AGE_GROUP", 'UNKWN')      AS vic_age,
        ISNULL("VIC_SEX", 'UNKWN')            AS vic_sex,
        ISNULL("VIC_RACE", 'UNKWN')           AS vic_race
    FROM staging_shootings 
) AS r
LEFT JOIN dim_location AS l ON 
    l."borough" = r."borough" AND
    l."x_coord" = r."x_coord" AND
    l."y_coord" = r."y_coord"   
LEFT JOIN dim_shooting_location AS s ON
    s.site  = r."location_site" AND
    s.class = r."location_class" AND
    s.description = r."location_descr"
ORDER BY key
;
    """)

    facts_call = ("""
TRUNCATE TABLE facts_call;
INSERT INTO facts_call (key, created, occured, police_code, patrol_key, location_key, call_code_key, call_times_key)
SELECT
    DISTINCT ON (key)
    r.key AS key, r.created AS created, r.occured AS occured, 
    r.police_code AS police_code, p.id AS patrol_key, l.id AS location_key,
    c.id AS call_code_key, t.key AS call_times_key
FROM
(
    SELECT
        "CAD_EVNT_ID"                      AS key,
        "CREATE_DATE"::DATE                AS created,
        TO_TIMESTAMP(CONCAT(
            "INCIDENT_DATE"::TEXT, ' ', 
            "INCIDENT_TIME"::TEXT), 
            'MM/DD/YYYY HH24:MI:SS')       AS occured,
        ISNULL("NYPD_PCT_CD"::INTEGER, -1) AS police_code,
        ISNULL("BORO_NM", 'UNKWN')         AS borough,
        ISNULL("PATRL_BORO_NM", 'UNKWN')   AS borough_patrol,
        "GEO_CD_X"::INTEGER                AS x_coord,
        "GEO_CD_Y"::INTEGER                AS y_coord,
        ISNULL("RADIO_CODE", 'UNKWN')      AS radio_code,
        ISNULL("TYP_DESC", 'UNKWN')        AS type_desc,
        ISNULL("CIP_JOBS", 'UNKWN')        AS cip_jobs,
        "ADD_TS"::DATE                     AS add_ts, 
        "DISP_TS"::DATE                    AS add_ts, 
        "ARRIVD_TS"::DATE                  AS add_ts, 
        "CLOSNG_TS"::DATE                  AS add_ts
    FROM staging_calls
) AS r
LEFT JOIN dim_location AS l ON 
    l."borough" = r."borough" AND
    l."x_coord" = r."x_coord" AND
    l."y_coord" = r."y_coord"
LEFT JOIN dim_call_times AS t ON t.key = r.key
LEFT JOIN dim_call_code AS c ON
    c.code = r.radio_code AND
    c.description = r.type_desc AND
    c.category = r.cip_jobs
LEFT JOIN dim_precinct_patrol AS p ON
    p.patrol = r.borough_patrol
;
    """)

    facts_arrest = ("""
TRUNCATE TABLE facts_arrest;
INSERT INTO facts_arrest (key, arrested, police_key, arrest_legal_key, key_cd, location_key, 
    precinct_key, jurisdiction_code)
SELECT
    DISTINCT ON(r.key)
    r.key, r.arrested, r.police_key, a.id AS arrest_legal_key, r.key_cd, l.id AS location_key, 
    r.precinct_key, r.jurisdiction_code
FROM
(
    SELECT 
        "ARREST_KEY"                  AS key,
        "ARREST_DATE"::DATE           AS arrested,
        ISNULL("PD_CD"::INTEGER, -1)  AS police_key,
        ISNULL("KY_CD"::INTEGER, -1)  AS key_cd,
        ISNULL("PD_DESC",   'UNKWN')  AS description,
        ISNULL("OFNS_DESC", 'UNKWN')  AS offence,
        ISNULL("LAW_CODE",  'UNKWN')  AS code,
        ISNULL("LAW_CAT_CD",'UNKWN')  AS category,
        ISNULL("ARREST_BORO", 'UNKWN') AS borough,
        "ARREST_PRECINCT"::BIGINT     AS precinct_key,
        ISNULL("JURISDICTION_CODE"::INTEGER, -1) AS jurisdiction_code,
        "X_COORD_CD"::INTEGER         AS x_coord,
        "Y_COORD_CD"::INTEGER         AS y_coord
    FROM staging_arrests
) AS r
LEFT JOIN dim_arrest_legal AS a ON 
    a.description = r.description AND
    a.offence = r.offence AND 
    a.code = r.code AND
    a.category = r.category
LEFT JOIN dim_location AS l ON 
    l."borough" = r."borough" AND
    l."x_coord" = r."x_coord" AND
    l."y_coord" = r."y_coord"
ORDER BY key
;
    """)