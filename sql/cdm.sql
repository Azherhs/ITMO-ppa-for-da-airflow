-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS cdm;

-- Create table in cdm schema cdm.up_lab
CREATE TABLE cdm.up_lab AS
SELECT DISTINCT
    app_isu_id,
    on_check,
    laboriousness,
    year,
    CASE
        WHEN qualification = 'master' THEN 'магистратура'
        WHEN qualification = 'bachelor' THEN 'бакалавриат'
        ELSE qualification
    END AS qualification,
    update_ts
FROM dds.up;



-- Create the new table in cdm schema
CREATE TABLE cdm.wp_statuses_aggregation (
    wp_id INTEGER,
    update_ts TIMESTAMP,
    cop_state VARCHAR(2),
    wp_description TEXT  -- Adding wp_description field
);

-- Insert data into the new table
INSERT INTO cdm.wp_statuses_aggregation (wp_id, update_ts, cop_state, wp_description)
SELECT
    wp_id,
    update_ts,
    st.cop_state,
    wp_description  -- Including wp_description field
FROM
    dds.wp
    INNER JOIN dds.states st ON dds.wp.wp_status = st.id;


-- Creating and inserting data into cdm.su_wp_statuses table
CREATE TABLE cdm.su_wp_statuses__ (
    wp_id INTEGER,
    discipline_code TEXT, -- Change the data type to TEXT
    wp_title TEXT,
    state_name TEXT,
    unit_title TEXT,
    description_flag SMALLINT,
    number_of_editors INTEGER
);

INSERT INTO cdm.su_wp_statuses__
    (wp_id, discipline_code, wp_title, state_name, unit_title, description_flag, number_of_editors)
WITH t AS (
    SELECT
        wp.wp_id,
        wp.discipline_code::TEXT, -- Explicitly cast discipline_code to TEXT
        wp.wp_title,
        s.state_name,
        u2.unit_title,
        CASE WHEN wp.wp_description IS NULL THEN 0 ELSE 1 END AS description_flag,
        we.editor_id
    FROM
        dds.wp wp
        JOIN dds.states s ON wp.wp_status = s.id
        LEFT JOIN dds.units u2 ON u2.id = wp.unit_id
        LEFT JOIN dds.wp_editor we ON wp.wp_id = we.wp_id
)
SELECT
    wp_id,
    discipline_code,
    wp_title,
    state_name,
    unit_title,
    description_flag,
    COUNT(editor_id) AS number_of_editors
FROM
    t
GROUP BY
    wp_id,
    discipline_code,
    wp_title,
    state_name,
    unit_title,
    description_flag;





-- Creating and inserting data into cdm.up_wp_statuses__ table
CREATE TABLE cdm.up_wp_statuses__ (
    up_id INTEGER,
    state_name TEXT,
    state_count INTEGER,
    annotated INTEGER
);

INSERT INTO cdm.up_wp_statuses__
    (up_id, state_name, state_count, annotated)
WITH t AS (
    SELECT
        u.app_isu_id AS up_id,
        wu.wp_id
    FROM
        dds.up u
        JOIN dds.wp_up wu ON wu.up_id = u.app_isu_id
),
t2 AS (
    SELECT
        t.up_id,
        w.discipline_code,
        w.wp_title,
        CASE WHEN w.wp_description IS NULL THEN 0 ELSE 1 END AS description_flag,
        s.state_name
    FROM
        t
        JOIN dds.wp w ON t.wp_id = w.wp_id
        JOIN dds.states s ON w.wp_status = s.id
),
t3 AS (
    SELECT
        up_id,
        state_name,
        COUNT(DISTINCT discipline_code) AS state_count,
        SUM(description_flag) AS annotated
    FROM
        t2
    GROUP BY
        up_id,
        state_name
)
SELECT * FROM t3;