DROP FUNCTION IF EXISTS ISNULL(anyelement, anyelement);
CREATE OR REPLACE FUNCTION ISNULL(param1 anyelement, param2 anyelement) RETURNS anyelement AS 
$$
BEGIN
    IF param1 IS NULL OR param1::TEXT = '(null)' THEN
        RETURN param2;
    ELSE
        RETURN param1;
    END IF;
END;
$$ 
LANGUAGE plpgsql;
--------------------------------------------------------------------------------
--- STAGING - staging_suburbs --------------------------------------------------
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS staging_suburbs;
CREATE TABLE IF NOT EXISTS staging_suburbs (
     "the_geom"  TEXT,
     "OBJECTID"  BIGINT,
     "Name"      TEXT,
     "Stacked"   BIGINT,
     "AnnoLine1" TEXT,
     "AnnoLine2" TEXT,
     "AnnoLine3" TEXT,
     "AnnoAngle" BIGINT,
     "Borough"   TEXT
);
--- STAGING - staging_shootings ------------------------------------------------
DROP TABLE IF EXISTS staging_shootings;
CREATE TABLE IF NOT EXISTS staging_shootings (
     "INCIDENT_KEY"       BIGINT,
     "OCCUR_DATE"         TEXT,
     "OCCUR_TIME"         TEXT,
     "BORO"               TEXT,
     "LOC_OF_OCCUR_DESC"  TEXT,
     "PRECINCT"           TEXT,
     "JURISDICTION_CODE"  TEXT,
     "LOC_CLASSFCTN_DESC" TEXT,
     "LOCATION_DESC"      TEXT,
     "STATISTICAL_MURDER_FLAG" BOOLEAN,
     "PERP_AGE_GROUP"     TEXT,
     "PERP_SEX"           TEXT,
     "PERP_RACE"          TEXT,
     "VIC_AGE_GROUP"      TEXT,
     "VIC_SEX"            TEXT,
     "VIC_RACE"           TEXT,
     "X_COORD_CD"         TEXT,
     "Y_COORD_CD"         TEXT,
     "Latitude"           TEXT,
     "Longitude"          TEXT,
     "Lon_Lat"            TEXT
);
--- STAGING - staging_calls ----------------------------------------------------
DROP TABLE IF EXISTS staging_calls;
CREATE TABLE IF NOT EXISTS staging_calls (
     "CAD_EVNT_ID"   BIGINT,
     "CREATE_DATE"   TEXT,
     "INCIDENT_DATE" TEXT,
     "INCIDENT_TIME" TEXT,
     "NYPD_PCT_CD"   TEXT,
     "BORO_NM"       TEXT,
     "PATRL_BORO_NM" TEXT,
     "GEO_CD_X"      BIGINT,
     "GEO_CD_Y"      BIGINT,
     "RADIO_CODE"    TEXT,
     "TYP_DESC"      TEXT,
     "CIP_JOBS"      TEXT,
     "ADD_TS"        TEXT,
     "DISP_TS"       TEXT,
     "ARRIVD_TS"     TEXT,
     "CLOSNG_TS"     TEXT,
     "Latitude"      TEXT,
     "Longitude"     TEXT
);
--- STAGING - staging_arrests --------------------------------------------------
DROP TABLE IF EXISTS staging_arrests;
CREATE TABLE IF NOT EXISTS staging_arrests (
     "ARREST_KEY"  BIGINT,
     "ARREST_DATE" TEXT,
     "PD_CD"       TEXT,
     "PD_DESC"     TEXT,
     "KY_CD"       TEXT,
     "OFNS_DESC"   TEXT,
     "LAW_CODE"    TEXT,
     "LAW_CAT_CD"  TEXT,
     "ARREST_BORO" TEXT,
     "ARREST_PRECINCT"   BIGINT,
     "JURISDICTION_CODE" BIGINT,
     "AGE_GROUP"   TEXT,
     "PERP_SEX"    TEXT,
     "PERP_RACE"   TEXT,
     "X_COORD_CD"  BIGINT,
     "Y_COORD_CD"  BIGINT,
     "Latitude"    TEXT,
     "Longitude"   TEXT,
     "New Georeferenced Column" TEXT
);

--------------------------------------------------------------------------------
--- DIMENSION - dim_shooting_location ------------------------------------------
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_shooting_location;
CREATE TABLE IF NOT EXISTS dim_shooting_location (
    id SERIAL PRIMARY KEY,
    site TEXT NOT NULL,
    class TEXT NOT NULL,
    description TEXT NOT NULL
);
ALTER TABLE dim_shooting_location ADD CONSTRAINT dim_shooting_location_unique UNIQUE (site, class, description);

--- DIMENSION - dim_shooting_murder_flag ---------------------------------------
DROP TABLE IF EXISTS dim_shooting_murder_flag;
CREATE TABLE IF NOT EXISTS dim_shooting_murder_flag (
    id SERIAL PRIMARY KEY,
    key BIGINT NOT NULL,
    flag BOOLEAN NOT NULL
);
ALTER TABLE dim_shooting_murder_flag ADD CONSTRAINT dim_shooting_murder_flag_unique UNIQUE (key, flag);

--- DIMENSION - dim_offender ---------------------------------------------------
DROP TABLE IF EXISTS dim_offender;
CREATE TABLE IF NOT EXISTS dim_offender (
    id SERIAL PRIMARY KEY,
    key BIGINT NOT NULL,
    age TEXT NOT NULL,
    sex TEXT NOT NULL,
    race TEXT NOT NULL,
    source TEXT NOT NULL
);
ALTER TABLE dim_offender ADD CONSTRAINT dim_offender_unique UNIQUE (key, age, sex, race, source);

--- DIMENSION - dim_victim -----------------------------------------------------
DROP TABLE IF EXISTS dim_victim;
CREATE TABLE IF NOT EXISTS dim_victim (
    id SERIAL PRIMARY KEY,
    key BIGINT NOT NULL,
    age TEXT NOT NULL,
    sex TEXT NOT NULL,
    race TEXT NOT NULL
);
ALTER TABLE dim_victim ADD CONSTRAINT dim_victim_unique UNIQUE (key, age, sex, race);

--- DIMENSION - dim_call_times -------------------------------------------------
DROP TABLE IF EXISTS dim_call_times;
CREATE TABLE IF NOT EXISTS dim_call_times (
    id SERIAL PRIMARY KEY,
    key BIGINT NOT NULL,
    added TIMESTAMP, 
    dispatched TIMESTAMP, 
    arrived TIMESTAMP,
    closed TIMESTAMP
);
ALTER TABLE dim_call_times ADD CONSTRAINT dim_call_times_unique UNIQUE (key, added, dispatched, arrived, closed);

--- DIMENSION - dim_call_code --------------------------------------------------
DROP TABLE IF EXISTS dim_call_code;
CREATE TABLE IF NOT EXISTS dim_call_code (
    id         SERIAL PRIMARY KEY,
    code        TEXT NOT NULL,
    description TEXT NOT NULL,
    category    TEXT NOT NULL
);
ALTER TABLE dim_call_code ADD CONSTRAINT dim_call_code_unique UNIQUE (code);

--- DIMENSION - dim_precinct_patrol --------------------------------------------
DROP TABLE IF EXISTS dim_precinct_patrol;
CREATE TABLE IF NOT EXISTS dim_precinct_patrol (
    id    SERIAL PRIMARY KEY,
    patrol TEXT NOT NULL
);
ALTER TABLE dim_precinct_patrol ADD CONSTRAINT dim_precinct_patrol_unique UNIQUE (patrol);

--- DIMENSION - dim_arrest_legal -----------------------------------------------
DROP TABLE IF EXISTS dim_arrest_legal;
CREATE TABLE IF NOT EXISTS dim_arrest_legal (
    id          SERIAL PRIMARY KEY,
    description TEXT NOT NULL,
    offence     TEXT NOT NULL,
    code        TEXT NOT NULL,
    category    TEXT NOT NULL
);
ALTER TABLE dim_arrest_legal ADD CONSTRAINT dim_arrest_legal_unique UNIQUE (description,offence,code,category);

--- DIMENSION - dim_precinct ---------------------------------------------------
DROP TABLE IF EXISTS dim_precinct;
CREATE TABLE IF NOT EXISTS dim_precinct (
    id        SERIAL PRIMARY KEY,
    place     TEXT NOT NULL,
    borough   TEXT NOT NULL,
    point     TEXT NOT NULL
);
ALTER TABLE dim_precinct ADD CONSTRAINT dim_precinct_unique UNIQUE (place, borough, point);

--- DIMENSION - dim_location ---------------------------------------------------
DROP TABLE IF EXISTS dim_location;
CREATE TABLE IF NOT EXISTS dim_location(
    id SERIAL PRIMARY KEY,
    borough TEXT NOT NULL,
    x_coord INTEGER NOT NULL,
    y_coord INTEGER NOT NULL,
    point TEXT NOT NULL
);
ALTER TABLE dim_location ADD CONSTRAINT dim_location_unique UNIQUE (borough, x_coord, y_coord);

--------------------------------------------------------------------------------
--- FACTS - facts_shooting -----------------------------------------------------
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS facts_shooting;
CREATE TABLE IF NOT EXISTS facts_shooting (
    key BIGINT PRIMARY KEY,
    occured TIMESTAMP NOT NULL,
    precinct_key INTEGER NOT NULL,
    site_id INTEGER NOT NULL,
    location_key BIGINT NOT NULL,
    jurisdiction_code INTEGER NOT NULL,
    murder_flag BOOLEAN NOT NULL
);

--- FACTS - facts_call ---------------------------------------------------------
DROP TABLE IF EXISTS facts_call;
CREATE TABLE IF NOT EXISTS facts_call (
     key BIGINT PRIMARY KEY,
     created DATE NOT NULL,
     occured DATE NOT NULL,
     police_code INTEGER NOT NULL,
     patrol_key INTEGER NOT NULL,
     location_key BIGINT NOT NULL,
     call_code_key BIGINT NOT NULL,
     call_times_key BIGINT NOT NULL
);

--------------------------------------------------------------------------------
--- DIMENSION - facts_arrest ---------------------------------------------------
DROP TABLE IF EXISTS facts_arrest;
CREATE TABLE IF NOT EXISTS facts_arrest (
     key BIGINT PRIMARY KEY,
     arrested DATE NOT NULL,
     police_key BIGINT NOT NULL,
     arrest_legal_key BIGINT NOT NULL,
     key_cd INTEGER NOT NULL,
     location_key BIGINT NOT NULL,
     precinct_key BIGINT NOT NULL,
     jurisdiction_code BIGINT NOT NULL
);
