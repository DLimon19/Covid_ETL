-- Se crea la tabla stage_covid_data
CREATE TABLE IF NOT EXISTS luis_981908_coderhouse.stage_covid_data(
    fips	             VARCHAR(200)
,   admin2	             VARCHAR(200)
,   province_state	     VARCHAR(200)
,   country_region       VARCHAR(200)
,   last_update          DATE
,   lat                  VARCHAR(150)
,   long_                VARCHAR(150)
,   confirmed            VARCHAR(150)
,   deaths               VARCHAR(150)
,   recovered            VARCHAR(150)
,   active               VARCHAR(150)
,   combined_key         VARCHAR(150)
,   incident_rate        VARCHAR(150)
,   case_fatality_ratio  VARCHAR(150)
);