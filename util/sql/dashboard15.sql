CREATE OR REPLACE VIEW dashboard15 AS
    WITH 
    dept_map AS(
    SELECT "HCM Department Name" AS department_name, ANY_VALUE("HCM Department ID") AS department_id
    from utilization_prediction_report
    where department_name IS NOT NULL
    GROUP BY 1
    ),
    -- 1. Current Month Aggregation
    util_summarized AS (
        SELECT 
            "Project Id", Practice, "Utilization Location" AS Location, "Grade Name" AS Grade, 
            "HCM Department Name" AS department_name,
            COUNT("Associate ID") AS headcount,
            SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS billed_fte,
            SUM(TRY_CAST("Total FTE" AS DOUBLE)) AS total_fte,
            ANY_VALUE("BU") AS bu_id,
            ANY_VALUE("Customer Id") AS account_id,
            ANY_VALUE("Project Name") AS project_name,
            ANY_VALUE("Project Type") AS project_type,
            ANY_VALUE("Project Billability") AS project_billability,
            ANY_VALUE("Customer Name") AS customer_name,
            ANY_VALUE("ParentCustomerID") AS parent_customer_id,
            ANY_VALUE("Parent Customer") AS parent_customer,
            ANY_VALUE("Is Onsite") AS is_onsite
        FROM utilization_prediction_report
        GROUP BY 1, 2, 3, 4, 5
    ),
    -- 2. Previous Month Aggregation (Now with Descriptive attributes)
    previous_util AS (
        SELECT 
            "Project Id", Practice, "Utilization Location" AS Location, "Grade Name" AS Grade,
            "HCM Department Name" AS department_name,
            SUM(TRY_CAST("Billed FTE Internal" AS DOUBLE)) AS prev_mon_billed_fte,
            SUM(TRY_CAST("Total FTE" AS DOUBLE)) AS prev_mon_total_fte,
            ANY_VALUE("BU") AS bu_id,
            ANY_VALUE("Customer Id") AS account_id,
            ANY_VALUE("Project Name") AS project_name,
            ANY_VALUE("Project Type") AS project_type,
            ANY_VALUE("Project Billability") AS project_billability,
            ANY_VALUE("Customer Name") AS customer_name,
            ANY_VALUE("ParentCustomerID") AS parent_customer_id,
            ANY_VALUE("Parent Customer") AS parent_customer
        FROM previous_month_actual
        GROUP BY 1, 2, 3, 4, 5
    ),
    -- 2. Your Demand Summary (already correct)
    demand_summary AS (
        SELECT "Project Id", Practice, Location, "Grade HR" AS Grade, 
        "Pool Name" AS department_name,
        COUNT(*) AS dem_count,
        ANY_VALUE("BU") AS bu_id,
        ANY_VALUE("Account Id") AS account_id,
        ANY_VALUE("Project Description") AS project_name,
        ANY_VALUE("Project Type") AS project_type,
        ANY_VALUE("Project Billability") AS project_billability,
        ANY_VALUE("Account Name") AS customer_name,
        ANY_VALUE("Account ID") AS customer_id,
        ANY_VALUE("Parent Customer ID") AS parent_customer_id,
        ANY_VALUE("Parent Customer") AS parent_customer
        FROM demand_base
        GROUP BY 1, 2, 3, 4, 5
    ),
    fulfilment_summary AS (
        SELECT "Project Id", Practice, Location, "Associate Hired Grade" AS Grade, 
        "Pool Name" AS department_name,
        SUM(TRY_CAST("FTE Impact" AS DOUBLE)) AS fulfil_count
        FROM fulfilment
        GROUP BY 1, 2, 3, 4, 5
    ),

    release_summary AS (
    SELECT "Project Id", Practice, Location, Grade, "Department Name" AS department_name,
        SUM(TRY_CAST("Impact FTE" AS DOUBLE)) AS rel_count
        FROM releases
        GROUP BY 1, 2, 3, 4, 5
    ),
    attrition_summary AS (
    SELECT "Project Id", Practice, Location, Grade, "Department Name" AS department_name,
        SUM(TRY_CAST("FTE Impact" AS DOUBLE)) AS attr_count
        FROM attrition
        GROUP BY 1, 2, 3, 4, 5
    ),
    -- New Cleaned Up Account Map
    map_account_unique AS (
    SELECT "Account ID", ANY_VALUE("PDL ID") AS "PDL ID", ANY_VALUE("PDL Name") AS "PDL Name"
    FROM map_account
    GROUP BY "Account ID"
    ),

    -- 3. Master Keys
    master_keys AS (
        SELECT "Project Id", Practice, Location, Grade, department_name FROM util_summarized
        UNION 
        SELECT "Project Id", Practice, Location, Grade, department_name FROM previous_util
        UNION
        SELECT "Project Id", practice, Location, Grade, department_name From demand_summary
    )

    -- 4. Final Join
    SELECT  
        m."Project Id", 
        COALESCE(u.project_name, p.project_name, d.project_name) AS project_name,
        COALESCE(u.project_type, p.project_type, d.project_type) AS project_type,
        COALESCE(u.project_billability, p.project_billability, d.project_billability) AS project_billability,
        m.Practice, m.Location, m.Grade,
        -- Pull from Current, fallback to Previous
        
        COALESCE(u.account_id, p.account_id, d.account_id) AS customer_id,
        COALESCE(u.customer_name, p.customer_name, d.customer_name) AS customer_name,
        COALESCE(u.parent_customer_id, p.parent_customer_id, d.parent_customer_id) AS parent_customer_id,
        COALESCE(u.parent_customer, p.parent_customer, d.parent_customer) AS parent_customer_name,
        COALESCE(u.bu_id, p.bu_id, d.bu_id) AS BU,
        m.department_name, d_map.department_id, t_map.Tower,
        
        --ROUND(COALESCE(u.billed_fte, 0), 5) AS billed_fte, 
        COALESCE(r.rel_count, 0) AS release_count,
        COALESCE(a.attr_count, 0) AS attr_count,
        COALESCE(d.dem_count, 0) AS open_demands, 
        COALESCE(f.fulfil_count, 0) AS demands_fulfilled,
        
        -- ALL computed columns
        ROUND(CAST(c.Cost AS DOUBLE), 5) AS std_cost,
        
        ROUND(COALESCE(p.prev_mon_billed_fte, 0), 5) AS prev_mon_billed_fte,
        ROUND(COALESCE(p.prev_mon_total_fte, 0), 5) AS prev_mon_total_fte,
        
        ROUND((COALESCE(p.prev_mon_billed_fte, 0) + COALESCE(0.5 * demands_fulfilled, 0) + COALESCE(0.2 *open_demands, 0) - COALESCE(attr_count, 0) - COALESCE(release_count, 0) ), 5) AS eff_billed_fte,
        ROUND((COALESCE(p.prev_mon_total_fte, 0) + COALESCE( demands_fulfilled, 0) + COALESCE(0.5 * open_demands, 0) - COALESCE(attr_count, 0) - COALESCE(release_count, 0) ), 5) AS eff_total_fte,
        
        ROUND((COALESCE(p.prev_mon_billed_fte, 0) * std_cost), 5) AS prev_billed_cost,
        ROUND((COALESCE(p.prev_mon_total_fte, 0) * std_cost), 5) AS prev_mon_total_cost,
        
        ROUND((eff_billed_fte * std_cost), 5) AS proj_billed_cost,
        ROUND((eff_total_fte * std_cost), 5) AS proj_total_cost,
        
        l_map.Country, l_map.Geo,
        b_map.SBU, b_map.Market,
        a_map."PDL ID",
        a_map."PDL Name",
        s_map."SBU Head ID",
        s_map."SBU Head Name"
        
    FROM master_keys m
    LEFT JOIN dept_map d_map ON m.department_name = d_map.department_name
    LEFT JOIN map_tower t_map ON d_map.department_id = t_map."Department ID"
    LEFT JOIN util_summarized u ON m."Project Id" = u."Project Id" AND m.Practice = u.Practice AND m.Location = u.Location AND m.Grade = u.Grade AND m.department_name = u.department_name
    LEFT JOIN previous_util p ON m."Project Id" = p."Project Id" AND m.Practice = p.Practice AND m.Location = p.Location AND m.Grade = p.Grade AND m.department_name = p.department_name
    LEFT JOIN demand_summary d ON m."Project Id" = d."Project Id" AND m.Practice = d.Practice AND m.Location = d.Location AND m.Grade = d.Grade AND m.department_name = d.department_name
    -- IMPORTANT: Join map tables to m (master) or COALESCE values to ensure they work for closed projects
    LEFT JOIN map_location l_map ON m.Location = l_map."Utilization Location"
    LEFT JOIN cost_file c ON m.Practice = c.Practice AND l_map.Country = c.Country AND m.Grade = c."Grade name"
    LEFT JOIN map_bu b_map ON COALESCE(u.bu_id, p.bu_id, d.bu_id) = b_map.BU
    LEFT JOIN map_sbu s_map ON b_map.SBU = s_map.SBU
    LEFT JOIN map_account_unique a_map ON COALESCE(u.account_id, p.account_id) = a_map."Account ID"
    -- Demand and Release still join to m/u/p logic
    LEFT JOIN fulfilment_summary f ON m."Project Id" = f."Project Id" AND m.Practice = f.Practice AND m.Location = f.Location AND m.Grade = f.Grade AND m.department_name = f.department_name
    LEFT JOIN attrition_summary a ON m."Project Id" = a."Project Id" AND m.Practice = a.Practice AND m.Location = a.Location AND m.Grade = a.Grade AND m.department_name = a.department_name
    LEFT JOIN release_summary r ON m."Project Id" = r."Project Id" AND m.Practice = r.Practice AND m.Location = r.Location AND m.Grade = r.Grade AND m.department_name = r.department_name;
