CREATE OR REPLACE VIEW dashboard2 AS
WITH 
-- 1. Current Month Aggregation
util_summarized AS (
    SELECT 
        "Project Id", Practice, "Utilization Location" AS Location, "Grade Name" AS Grade,
        COUNT("Associate ID") AS headcount,
        SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS billed_fte,
        ANY_VALUE("BU") AS bu_id,
        ANY_VALUE("Customer Id") AS account_id,
        ANY_VALUE("Project Name") AS project_name,
        ANY_VALUE("Project Billability") AS project_billability,
        ANY_VALUE("Customer Name") AS customer_name,
        ANY_VALUE("ParentCustomerID") AS parent_customer_id,
        ANY_VALUE("Parent Customer") AS parent_customer,
        ANY_VALUE("Is Onsite") AS is_onsite
    FROM utilization_prediction_report
    GROUP BY 1, 2, 3, 4
),
-- 2. Previous Month Aggregation (Now with Descriptive attributes)
previous_util AS (
    SELECT 
        "Project Id", Practice, "Utilization Location" AS Location, "Grade Name" AS Grade,
        SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS prev_mon_actual_billed_fte,
        ANY_VALUE("BU") AS bu_id,
        ANY_VALUE("Customer Id") AS account_id,
        ANY_VALUE("Project Name") AS project_name,
        ANY_VALUE("Customer Name") AS customer_name,
        ANY_VALUE("Parent Customer") AS parent_customer
    FROM previous_month_actual
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

-- 3. Master Keys
master_keys AS (
    SELECT "Project Id", Practice, Location, Grade FROM util_summarized
    UNION 
    SELECT "Project Id", Practice, Location, Grade FROM previous_util
)

-- 4. Final Join
SELECT  
    m."Project Id", 
    m.Practice, m.Location, m.Grade,
    -- Pull from Current, fallback to Previous
    COALESCE(u.project_name, p.project_name) AS project_name,
    COALESCE(u.customer_name, p.customer_name) AS customer_name,
    COALESCE(u.parent_customer, p.parent_customer) AS parent_customer,
    COALESCE(u.bu_id, p.bu_id) AS bu_id,
    COALESCE(u.account_id, p.account_id) AS account_id,
    
    COALESCE(u.billed_fte, 0) AS billed_fte, 
    COALESCE(p.prev_mon_actual_billed_fte, 0) AS prev_mon_actual_billed_fte,
    
    COALESCE(r.rel_count, 0) AS release_count,
    COALESCE(d.dem_count, 0) AS open_demands, 
    
    -- Calculations (using COALESCE to avoid NULL results)
    (COALESCE(u.billed_fte, 0) + COALESCE(d.dem_count, 0) - COALESCE(r.rel_count, 0)) AS eff_billed_fte,
    
    CAST(c.Cost AS DOUBLE) AS d_cost,
    (COALESCE(u.billed_fte, 0) * CAST(c.Cost AS DOUBLE)) AS curr_total_cost,
    ((COALESCE(u.billed_fte, 0) + COALESCE(d.dem_count, 0) - COALESCE(r.rel_count, 0)) * CAST(c.Cost AS DOUBLE)) AS proj_total_cost,
    
    l_map.Country, l_map.Geo,
    b_map.SBU, b_map.Market,
    s_map."SBU Head Name",
    a_map."PDL Name"
    
FROM master_keys m
LEFT JOIN util_summarized u ON m."Project Id" = u."Project Id" AND m.Practice = u.Practice AND m.Location = u.Location AND m.Grade = u.Grade
LEFT JOIN previous_util p ON m."Project Id" = p."Project Id" AND m.Practice = p.Practice AND m.Location = p.Location AND m.Grade = p.Grade
-- IMPORTANT: Join map tables to m (master) or COALESCE values to ensure they work for closed projects
LEFT JOIN map_location l_map ON m.Location = l_map."Utilization Location"
LEFT JOIN cost_dec_25 c ON m.Practice = c.Practice AND l_map.Country = c.Country AND m.Grade = c."Grade name"
LEFT JOIN map_bu b_map ON COALESCE(u.bu_id, p.bu_id) = b_map.BU
LEFT JOIN map_sbu s_map ON b_map.SBU = s_map.SBU
LEFT JOIN map_account_unique a_map ON COALESCE(u.account_id, p.account_id) = a_map."Account ID"
-- Demand and Release still join to m/u/p logic
LEFT JOIN demand_summary d ON m."Project Id" = d."Project Id" AND m.Practice = d.Practice AND m.Location = d.Location AND m.Grade = d.Grade
LEFT JOIN release_summary r ON m."Project Id" = r."Project Id" AND m.Practice = r.Practice AND m.Location = r.Location AND m.Grade = r.Grade;