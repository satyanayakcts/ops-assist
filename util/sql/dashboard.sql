-- Initial SQL which was giving wrong total billed fte
CREATE OR REPLACE VIEW dashboard AS
    WITH 
    -- 1. Aggregate Utilization to Project-Grade Grain
    util_agg AS (
        SELECT 
            "Project Id", 
            Practice, 
            "Utilization Location" AS Location, 
            "Grade Name" AS Grade,
            COUNT("Associate ID") AS headcount,
            -- Convert strings to numbers safely
            SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS total_billed_fte,
            SUM(TRY_CAST("Total FTE" AS DOUBLE)) AS total_fte,
            ANY_VALUE("Customer Id") AS account_id,
            ANY_VALUE("Project Name") AS project_name,
            ANY_VALUE("Project Billability") AS project_billability,
            ANY_VALUE("Customer Id") AS customer_id,
            ANY_VALUE("Customer Name") AS customer_name,
            ANY_VALUE("ParentCustomerID") AS parent_customer_id,
            ANY_VALUE("Parent Customer") AS parent_customer,
            ANY_VALUE("Is Onsite") AS is_onsite,
            CURRENT_DATE AS prediction_date,

            -- Take any one value for lookups (since they should be the same for a project)
            ANY_VALUE("BU") AS bu_id,
            
        FROM utilization_prediction_report
        GROUP BY 1, 2, 3, 4
    ),

    -- 2. Aggregate Demand 
    demand_agg AS (
        SELECT 
            "Project Id", 
            Practice, 
            Location, 
            "Grade HR" AS Grade,
            COUNT(*) AS open_demands
        FROM demand_base
        GROUP BY 1, 2, 3, 4
    ),

    -- 3. Aggregate Releases
    release_agg AS (
        SELECT 
            "Project Id", 
            Practice, 
            Location, 
            Grade,
            COUNT(*) AS release_count
        FROM np_jan26
        GROUP BY 1, 2, 3, 4
    )

    -- 4. Final Join and Enrichment with 4 Lookups
    SELECT 
        -- Dimensions
        u."Project Id",
        u.project_name, 
        u.Practice, 
        u.Location, 
        u.Grade,
        u.project_billability,
        u.account_id,
        u.customer_name,
        u.parent_customer_id,
        u.parent_customer,
        u.is_onsite,
        u.prediction_date,
        
        -- Metrics
        u.headcount,
        u.total_billed_fte,
        u.total_fte,
        COALESCE(d.open_demands, 0) AS open_demands,
        COALESCE(r.release_count, 0) AS release_count,
        
        -- Lookup 1: Location to Country/Geo
        l_map.Country, 
        l_map.Geo,
        
        -- Lookup 2: BU to SBU/Market
        b_map.SBU, 
        b_map.Market,
        
        -- Lookup 3: Account to PDL
        a_map."PDL ID", 
        a_map."PDL Name",
        
        -- Lookup 4: SBU to SBU Head
        s_map."SBU Head ID", 
        s_map."SBU Head Name"

    FROM util_agg u
    LEFT JOIN demand_agg d  
        ON  u."Project Id" = d."Project Id" 
        AND u.Practice = d.Practice 
        AND u.Location = d.Location 
        AND u.Grade = d.Grade
    LEFT JOIN release_agg r 
        ON  u."Project Id" = r."Project Id" 
        AND u.Practice = r.Practice 
        AND u.Location = r.Location 
        AND u.Grade = r.Grade
    -- Mapping Joins
    LEFT JOIN map_location l_map ON u.Location = l_map."Utilization Location"
    LEFT JOIN map_bu b_map       ON u.bu_id = b_map.BU
    LEFT JOIN map_account a_map  ON u.account_id = a_map."Account ID"
    LEFT JOIN map_sbu s_map      ON b_map.SBU = s_map.SBU;

 ------------------------------------------------------------------------------------------------
 -----------------------------------------------------------------------------------------------

-- New debugged SQL for dashboard - correct Billed FTE
CREATE OR REPLACE VIEW dashboard AS
WITH 
-- 1. Aggregate Employees into Project-Grade buckets
util_summarized AS (
    SELECT 
        "Project Id", 
        Practice, 
        "Utilization Location" AS Location, 
        "Grade Name" AS Grade,
        ANY_VALUE("Project Name") AS project_name, -- Just grab the name once
        COUNT("Associate ID") AS headcount,
        SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS total_billed_fte,
        SUM(TRY_CAST("Total FTE" AS DOUBLE)) AS total_fte,
        ANY_VALUE("BU") AS bu_id,
        ANY_VALUE("Customer Id") AS account_id
    FROM utilization_prediction_report
    GROUP BY 1, 2, 3, 4
),
-- 2. Your Demand Summary (already correct)
demand_summary AS (
    SELECT "Project Id", Practice, Location, "Grade HR" AS Grade, COUNT(*) AS dem_count
    FROM demand_base
    GROUP BY 1, 2, 3, 4
),
-- 3. Your Release Summary (already correct)
release_summary AS (
    SELECT "Project Id", Practice, Location, Grade, COUNT(*) AS rel_count
    FROM np_jan26
    GROUP BY 1, 2, 3, 4
)

-- 4. Final Join (1-to-1-to-1 Join)
SELECT 
    u."Project Id",u.project_name,u.bu_id, u.account_id,u.total_billed_fte,
    COALESCE(r.rel_count, 0) AS release_count,
    COALESCE(d.dem_count, 0) AS open_demands,
    l_map.Country, l_map.Geo,
    b_map.SBU, b_map.Market,
    s_map."SBU Head ID", s_map."SBU Head Name",
    --a_map."PDL ID",a_map."PDL Name"
FROM util_summarized u
LEFT JOIN release_summary r 
    ON  u."Project Id" = r."Project Id" 
    AND u.Practice = r.Practice 
    AND u.Location = r.Location 
    AND u.Grade = r.Grade
LEFT JOIN demand_summary d 
    ON  u."Project Id" = d."Project Id" 
    AND u.Practice = d.Practice 
    AND u.Location = d.Location 
    AND u.Grade = d.Grade
LEFT JOIN map_location l_map ON u.Location = l_map."Utilization Location"
LEFT JOIN map_bu b_map       ON u.bu_id = b_map.BU
LEFT JOIN map_sbu s_map      ON b_map.SBU = s_map.SBU;
--LEFT JOIN map_account a_map  ON u.account_id = a_map."Account ID";


-- Account to PDL mapping got duplicates
SELECT "Account ID", COUNT(*) as mappings
FROM map_account
GROUP BY "Account ID"
HAVING COUNT(*) > 1
ORDER BY mappings DESC;

----------------------------Query Version 3----------------------------------
-----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dashboard AS
WITH 
-- 1. Aggregate Employees into Project-Grade buckets
util_summarized AS (
    SELECT 
        "Project Id", 
        Practice, 
        "Utilization Location" AS Location, 
        "Grade Name" AS Grade,
        COUNT("Associate ID") AS headcount,
        SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS billed_fte,
        SUM(TRY_CAST("Total FTE" AS DOUBLE)) AS total_fte,
        ANY_VALUE("BU") AS bu_id,
        ANY_VALUE("Customer Id") AS account_id,
        ANY_VALUE("Project Name") AS project_name,
        ANY_VALUE("Project Billability") AS project_billability,
        ANY_VALUE("Customer Name") AS customer_name,
        ANY_VALUE("ParentCustomerID") AS parent_customer_id,
        ANY_VALUE("Parent Customer") AS parent_customer,
        ANY_VALUE("Is Onsite") AS is_onsite,
        strftime(CURRENT_DATE, '%Y-%m-%d') AS prediction_date
        
    FROM utilization_prediction_report
    GROUP BY 1, 2, 3, 4
),
-- 2. Your Demand Summary (already correct)
demand_summary AS (
    SELECT "Project Id", Practice, Location, "Grade HR" AS Grade, COUNT(*) AS dem_count
    FROM demand_base
    GROUP BY 1, 2, 3, 4
),
-- 3. Your Release Summary (already correct)
release_summary AS (
    SELECT "Project Id", Practice, Location, Grade, COUNT(*) AS rel_count
    FROM np_jan26
    GROUP BY 1, 2, 3, 4
),
-- New Cleaned Up Account Map
map_account_unique AS (
  SELECT "Account ID", ANY_VALUE("PDL ID") AS "PDL ID", ANY_VALUE("PDL Name") AS "PDL Name"
  FROM map_account
  GROUP BY "Account ID"
),
-- Previous month actual
previous_util AS (
SELECT "Project Id", Practice, "Utilization Location" AS Location, "Grade Name" AS Grade,
SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS prev_mon_actual_billed_fte
FROM previous_month_actual
GROUP BY 1, 2, 3, 4
)

-- 4. Final Join (1-to-1-to-1 Join)
SELECT  
    u."Project Id", u.project_name, u.Practice, u.Location, u.Grade, g_map."CoD Grade", u.project_billability, 
    u.customer_name, u.parent_customer_id, u.parent_customer, u.is_onsite, u.bu_id, 
    u.account_id,u.billed_fte, u.prediction_date,
    p.prev_mon_actual_billed_fte,
    COALESCE(r.rel_count, 0) AS release_count,
    COALESCE(d.dem_count, 0) AS open_demands, 
    (u.billed_fte + open_demands - release_count ) AS eff_billed_fte, 
    (CAST(c.Cost AS DOUBLE)) AS d_cost, 
    (u.billed_fte * d_cost) AS curr_total_cost,
    (eff_billed_fte * d_cost) AS pred_total_cost,
    l_map.Country, l_map.Geo,
    b_map.SBU, b_map.Market,
    a_map."PDL ID",a_map."PDL Name",
    s_map."SBU Head ID", s_map."SBU Head Name"
FROM util_summarized u
LEFT JOIN release_summary r 
    ON  u."Project Id" = r."Project Id" 
    AND u.Practice = r.Practice 
    AND u.Location = r.Location 
    AND u.Grade = r.Grade
LEFT JOIN demand_summary d 
    ON  u."Project Id" = d."Project Id" 
    AND u.Practice = d.Practice 
    AND u.Location = d.Location 
    AND u.Grade = d.Grade
LEFT JOIN previous_util p
  ON u."Project Id" = p."Project Id"
  AND u.Practice = p.Practice 
  AND u.Location = p.Location 
  AND u.Grade = p.Grade
LEFT JOIN map_location l_map ON u.Location = l_map."Utilization Location"
LEFT JOIN cost_dec_25 c
        ON u.Practice = c.Practice
        AND l_map.Country = c.Country
        AND u.Grade = c."Grade name"
LEFT JOIN map_bu b_map       ON u.bu_id = b_map.BU
LEFT JOIN map_sbu s_map      ON b_map.SBU = s_map.SBU
LEFT JOIN map_grade g_map    ON u.Grade = g_map.Grade
LEFT JOIN map_account_unique a_map  ON u.account_id = a_map."Account ID";


-------------------- Query V4 ----------------------------------------------------------
-- Added the Projects which are closed in previous month and not present in current month
----------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dashboard AS
WITH 
-- 1. Aggregate Employees into Project-Grade buckets
util_summarized AS (
    SELECT 
        "Project Id", 
        Practice, 
        "Utilization Location" AS Location, 
        "Grade Name" AS Grade,
        COUNT("Associate ID") AS headcount,
        SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS billed_fte,
        SUM(TRY_CAST("Total FTE" AS DOUBLE)) AS total_fte,
        ANY_VALUE("BU") AS bu_id,
        ANY_VALUE("Customer Id") AS account_id,
        ANY_VALUE("Project Name") AS project_name,
        ANY_VALUE("Project Billability") AS project_billability,
        ANY_VALUE("Customer Name") AS customer_name,
        ANY_VALUE("ParentCustomerID") AS parent_customer_id,
        ANY_VALUE("Parent Customer") AS parent_customer,
        ANY_VALUE("Is Onsite") AS is_onsite,
        strftime(CURRENT_DATE, '%Y-%m-%d') AS prediction_date
    
    FROM utilization_prediction_report
    GROUP BY 1, 2, 3, 4
),
-- 2. Your Demand Summary (already correct)
demand_summary AS (
    SELECT "Project Id", Practice, Location, "Grade HR" AS Grade, COUNT(*) AS dem_count
    FROM demand_base
    GROUP BY 1, 2, 3, 4
),
-- 3. Your Release Summary (already correct)
release_summary AS (
    SELECT "Project Id", Practice, Location, Grade, COUNT(*) AS rel_count
    FROM np_jan26
    GROUP BY 1, 2, 3, 4
),
-- New Cleaned Up Account Map
map_account_unique AS (
  SELECT "Account ID", ANY_VALUE("PDL ID") AS "PDL ID", ANY_VALUE("PDL Name") AS "PDL Name"
  FROM map_account
  GROUP BY "Account ID"
),
-- Previous month actual
previous_util AS (
SELECT "Project Id", Practice, "Utilization Location" AS Location, "Grade Name" AS Grade,
SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS prev_mon_actual_billed_fte
FROM previous_month_actual
GROUP BY 1, 2, 3, 4
),
-- master keys
master_keys AS (
    SELECT "Project Id", Practice, Location, Grade FROM util_summarized
    UNION 
    SELECT "Project Id", Practice, Location, Grade FROM previous_util
)
-- 4. Final Join (1-to-1-to-1 Join)
SELECT  
    m."Project Id", 
    COALESCE(u.project_name, 'Closed Project') AS project_name, -- Labeling closed projects
    m.Practice, m.Location, m.Grade,
    COALESCE(u.billed_fte, 0) AS billed_fte, 
    COALESCE(p.prev_mon_actual_billed_fte, 0) AS prev_mon_actual_billed_fte,

    --u."Project Id", 
    --u.project_name, u.Practice, u.Location, u.Grade,
    g_map."CoD Grade", u.project_billability, 
    u.customer_name, u.parent_customer_id, u.parent_customer, u.is_onsite, u.bu_id, 
    u.account_id,
    -- u.billed_fte, 
    u.prediction_date,
    -- p.prev_mon_actual_billed_fte,
    COALESCE(r.rel_count, 0) AS release_count,
    COALESCE(d.dem_count, 0) AS open_demands, 
    (u.billed_fte + open_demands - release_count ) AS eff_billed_fte, 
    (CAST(c.Cost AS DOUBLE)) AS d_cost, 
    (u.billed_fte * d_cost) AS curr_total_cost,
    (p.prev_mon_actual_billed_fte * d_cost) AS prev_total_cost,
    (eff_billed_fte * d_cost) AS pred_total_cost,

    l_map.Country, l_map.Geo,
    b_map.SBU, b_map.Market,
    a_map."PDL ID",a_map."PDL Name",
    s_map."SBU Head ID", s_map."SBU Head Name"
    
FROM master_keys m
LEFT JOIN util_summarized u 
    ON m."Project Id" = u."Project Id" AND m.Practice = u.Practice AND m.Location = u.Location AND m.Grade = u.Grade
LEFT JOIN previous_util p
    ON m."Project Id" = p."Project Id" AND m.Practice = p.Practice AND m.Location = p.Location AND m.Grade = p.Grade
-- FROM util_summarized u
LEFT JOIN release_summary r 
    ON  u."Project Id" = r."Project Id" 
    AND u.Practice = r.Practice 
    AND u.Location = r.Location 
    AND u.Grade = r.Grade
LEFT JOIN demand_summary d 
    ON  u."Project Id" = d."Project Id" 
    AND u.Practice = d.Practice 
    AND u.Location = d.Location 
    AND u.Grade = d.Grade
-- LEFT JOIN previous_util p
--   ON u."Project Id" = p."Project Id"
--   AND u.Practice = p.Practice 
--   AND u.Location = p.Location 
--   AND u.Grade = p.Grade
LEFT JOIN map_location l_map ON u.Location = l_map."Utilization Location"
LEFT JOIN cost_dec_25 c
        ON u.Practice = c.Practice
        AND l_map.Country = c.Country
        AND u.Grade = c."Grade name"
LEFT JOIN map_bu b_map       ON u.bu_id = b_map.BU
LEFT JOIN map_sbu s_map      ON b_map.SBU = s_map.SBU
LEFT JOIN map_grade g_map    ON u.Grade = g_map.Grade
LEFT JOIN map_account_unique a_map  ON u.account_id = a_map."Account ID";