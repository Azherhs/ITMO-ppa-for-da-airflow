create table dds.states
(id serial, cop_state varchar(2), state_name varchar(20));
ALTER TABLE dds.states ADD CONSTRAINT state_name_uindex UNIQUE (state_name);
create table dds.editors
(id integer, username varchar(50), first_name varchar(50), last_name varchar(50), email varchar(50), isu_number varchar(6));
ALTER TABLE dds.editors ADD CONSTRAINT editors_uindex UNIQUE (id);
create table dds.up
(app_isu_id integer, on_check varchar(20), laboriousness integer, year integer, qualification varchar(20), update_ts timestamp);
create table dds.wp
(wp_id integer, discipline_code integer, wp_title text, wp_status integer, unit_id integer, wp_description text, update_ts timestamp);
ALTER TABLE dds.wp
ALTER COLUMN discipline_code TYPE text;
create table dds.wp_editor
(wp_id integer, editor_id integer);
create table dds.wp_up
(wp_id integer, up_id integer);
create table dds.units
(id integer, unit_title varchar(100), faculty_id integer);
ALTER TABLE dds.units ADD CONSTRAINT units_uindex UNIQUE (id);