/* *************************************************************

* Product: Snowflake Migration Platform

* Utility: "Transformer" which generates Snowflake DDL script

* Date: Jun 2021

* Company: Dattendriya Data Science Solutions

*************************************************************** */

--Database Created
CREATE OR REPLACE DATABASE ORACLE_META_PUBLIC;

--Schema Created
USE ORACLE_META_PUBLIC.PUBLIC;

--TABLE:feedback_status_config
CREATE OR REPLACE TABLE feedback_status_config ( 
 updated_ts timestamp_ntz(6),
 updated_by varchar (1000),
 created_ts timestamp_ntz(6),
 created_by varchar (1000),
 status varchar (100),
 next_possible_status varchar (1000),
 status_id varchar (1000),
 ticket_type_id varchar (1000),
 client_id number,
 id number NOT NULL
 );

--TABLE:feedback_vendors
CREATE OR REPLACE TABLE feedback_vendors ( 
 updated_ts timestamp_ntz(6),
 vendor_name varchar (1000),
 status varchar (10),
 created_by varchar (500),
 created_ts timestamp_ntz(6),
 updated_by varchar (500),
 vendor_id number NOT NULL
 );

--TABLE:ipl_match_players
CREATE OR REPLACE TABLE ipl_match_players ( 
 last_update_ts timestamp_ltz(6),
 create_user_id varchar (40),
 player_name varchar (255),
 team varchar (255),
 ipl_match_players_id number (10) NOT NULL,
 role varchar (100),
 create_ts timestamp_ltz(6),
 last_update_user_id varchar (40)
 );

--TABLE:ipl_match_prediction
CREATE OR REPLACE TABLE ipl_match_prediction ( 
 mom_prediction varchar (255),
 ipl_match_prediction_id number (10) NOT NULL,
 last_update_user_id varchar (40),
 last_update_ts timestamp_ltz(6),
 create_user_id varchar (40),
 create_ts timestamp_ltz(6),
 ipl_match_schedule_id number (10),
 winner_prediction varchar (255),
 email_id varchar (255)
 );

--TABLE:ipl_match_schedule
CREATE OR REPLACE TABLE ipl_match_schedule ( 
 ipl_match_schedule_id number (10) NOT NULL,
 cricapi_match_id number,
 actual_winner varchar (255),
 match_location varchar (255),
 actual_mom varchar (255),
 result varchar (2000),
 toss varchar (200),
 year varchar (20),
 match_id number,
 last_update_user_id varchar (40),
 match_date date,
 create_ts timestamp_ltz(6),
 team_1 varchar (255),
 last_update_ts timestamp_ltz(6),
 create_user_id varchar (40),
 team_2 varchar (255)
 );

--TABLE:ipl_teams
CREATE OR REPLACE TABLE ipl_teams ( 
 ipl_team_id number (10) NOT NULL,
 captain_image_path varchar (2000),
 last_update_user_id varchar (40),
 last_update_ts timestamp_ltz(6),
 team_name varchar (255),
 create_ts timestamp_ltz(6),
 image_path varchar (2000),
 team_code varchar (50),
 create_user_id varchar (40)
 );

--Adding Primary Key constraints
ALTER TABLE feedback_status_config ADD CONSTRAINT fb_status_config_pk PRIMARY KEY (id);
ALTER TABLE feedback_vendors ADD CONSTRAINT feedback_vendors_pk PRIMARY KEY (vendor_id);
ALTER TABLE ipl_match_players ADD CONSTRAINT ipl_match_players PRIMARY KEY (ipl_match_players_id);
ALTER TABLE ipl_match_prediction ADD CONSTRAINT ipl_match_prediction_pk PRIMARY KEY (ipl_match_prediction_id);
ALTER TABLE ipl_match_schedule ADD CONSTRAINT ipl_match_schedule_pk PRIMARY KEY (ipl_match_schedule_id);
ALTER TABLE ipl_teams ADD CONSTRAINT ipl_logos_pk PRIMARY KEY (ipl_team_id);

--Adding Foreign Key constraints
ALTER TABLE ipl_match_prediction ADD CONSTRAINT sys_c0089431 FOREIGN KEY (ipl_match_schedule_id) references ipl_match_schedule (ipl_match_schedule_id);
