
-- Function to clear data before full upload (optional, if user wants to replace everything)
CREATE OR REPLACE FUNCTION clear_all_data()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM public.data_detailed;
    DELETE FROM public.data_history;
    DELETE FROM public.data_clients;
END;
$$;

-- Function: Get Main Dashboard Data
CREATE OR REPLACE FUNCTION get_main_dashboard_data(
    p_filial text default null,
    p_cidade text default null,
    p_supervisor text default null,
    p_vendedor text default null,
    p_fornecedor text default null,
    p_ano text default null,
    p_mes text default null
)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_year int;
    v_previous_year int;
    v_target_month int;

    v_kpi_clients_attended int;
    v_kpi_clients_base int;

    v_monthly_chart_current json;
    v_monthly_chart_previous json;
    v_summary_table json;

    v_result json;
BEGIN
    -- 1. Determine Years
    IF p_ano IS NULL OR p_ano = 'todos' OR p_ano = '' THEN
        -- Default to the most recent year found in sales
        SELECT COALESCE(MAX(EXTRACT(YEAR FROM dtped))::int, EXTRACT(YEAR FROM CURRENT_DATE)::int)
        INTO v_current_year
        FROM public.all_sales;
    ELSE
        v_current_year := p_ano::int;
    END IF;
    v_previous_year := v_current_year - 1;

    -- 2. Determine Month Filter
    IF p_mes IS NOT NULL AND p_mes != '' AND p_mes != 'todos' THEN
        v_target_month := p_mes::int;
    ELSE
        v_target_month := NULL;
    END IF;

    -- 3. Base Filter CTE
    -- We filter sales once to reuse
    CREATE TEMPORARY TABLE temp_filtered_sales AS
    SELECT *
    FROM public.all_sales s
    WHERE
        (p_filial IS NULL OR p_filial = '' OR s.filial = p_filial)
        AND (p_cidade IS NULL OR p_cidade = '' OR s.cidade = p_cidade)
        AND (p_supervisor IS NULL OR p_supervisor = '' OR s.superv = p_supervisor)
        AND (p_vendedor IS NULL OR p_vendedor = '' OR s.nome = p_vendedor)
        AND (p_fornecedor IS NULL OR p_fornecedor = '' OR s.codfor = p_fornecedor);

    -- 4. KPI: Clients Attended (Positive Sales > 1) in the selected period
    -- Logic: Count unique CODCLI where (SUM(VLVENDA) - SUM(VLDEVOLUCAO)) > 1
    -- Period: If Month selected, that month. If not, maybe whole year or last month?
    -- Index.html logic: "referenceMonthForKPI". If no month selected, last month with sales.
    -- Here, for simplicity, if no month selected, we calculate for the whole Current Year (or handle in Front).
    -- But dashboard usually shows "Month vs Last Year Month".
    -- Let's try to detect the "last active month" if p_mes is null.

    IF v_target_month IS NULL THEN
         SELECT MAX(EXTRACT(MONTH FROM dtped))::int INTO v_target_month
         FROM temp_filtered_sales
         WHERE EXTRACT(YEAR FROM dtped) = v_current_year;
    END IF;

    IF v_target_month IS NULL THEN v_target_month := 11; END IF; -- Default to Dec (11 index? No, SQL is 1-12. JS is 0-11). Let's assume input p_mes is 0-based from JS, so we add 1.

    -- Adjust p_mes input which is likely 0-11 from JS
    IF p_mes IS NOT NULL AND p_mes != '' THEN
        v_target_month := p_mes::int + 1;
    END IF;

    -- KPI: Active Clients in Target Month (Current Year)
    SELECT COUNT(*) INTO v_kpi_clients_attended
    FROM (
        SELECT codcli, SUM(vlvenda) - SUM(COALESCE(vldevolucao, 0)) as val
        FROM temp_filtered_sales
        WHERE EXTRACT(YEAR FROM dtped) = v_current_year
          AND EXTRACT(MONTH FROM dtped) = v_target_month
        GROUP BY codcli
    ) t
    WHERE val > 1;

    -- KPI: Base Clients (Filtered)
    -- This is tricky because it depends on Client Filters.
    -- "Eligible Clients": Active status, and matching RCA of supervisor/vendor if selected.
    -- Simplified: Count clients in `data_clients` that match City/RCA filters.
    -- If Supervisor/Vendedor selected, we need their RCAs.
    WITH relevant_rcas AS (
        SELECT DISTINCT codusur
        FROM temp_filtered_sales
        WHERE EXTRACT(YEAR FROM dtped) = v_current_year
    )
    SELECT COUNT(*) INTO v_kpi_clients_base
    FROM public.data_clients c
    WHERE
        (p_cidade IS NULL OR p_cidade = '' OR c.cidade = p_cidade)
        AND c.bloqueio != 'S'
        AND (
            (p_supervisor IS NULL AND p_vendedor IS NULL)
            OR
            (c.rca1 IN (SELECT codusur FROM relevant_rcas))
        );

    -- 5. Monthly Data for Chart & Table
    -- We need sums for every month of Current and Previous Year
    WITH monthly_agg AS (
        SELECT
            EXTRACT(YEAR FROM dtped)::int as yr,
            EXTRACT(MONTH FROM dtped)::int as mth,
            SUM(vlvenda) as faturamento,
            SUM(totpesoliq) as peso,
            SUM(vlbonific) as bonificacao,
            SUM(COALESCE(vldevolucao,0)) as devolucao,
            COUNT(DISTINCT CASE WHEN (vlvenda - COALESCE(vldevolucao,0)) > 1 THEN codcli END) as positivacao
        FROM temp_filtered_sales
        WHERE EXTRACT(YEAR FROM dtped) IN (v_current_year, v_previous_year)
        GROUP BY 1, 2
    )
    SELECT json_agg(row_to_json(t)) INTO v_monthly_chart_current
    FROM (
        SELECT mth - 1 as month_index, faturamento, peso, bonificacao, devolucao, positivacao
        FROM monthly_agg WHERE yr = v_current_year ORDER BY mth
    ) t;

    SELECT json_agg(row_to_json(t)) INTO v_monthly_chart_previous
    FROM (
        SELECT mth - 1 as month_index, faturamento, peso, bonificacao, devolucao, positivacao
        FROM monthly_agg WHERE yr = v_previous_year ORDER BY mth
    ) t;

    -- 6. Construct Result
    v_result := json_build_object(
        'current_year', v_current_year,
        'previous_year', v_previous_year,
        'target_month_index', v_target_month - 1,
        'kpi_clients_attended', COALESCE(v_kpi_clients_attended, 0),
        'kpi_clients_base', COALESCE(v_kpi_clients_base, 0),
        'monthly_data_current', COALESCE(v_monthly_chart_current, '[]'::json),
        'monthly_data_previous', COALESCE(v_monthly_chart_previous, '[]'::json)
    );

    DROP TABLE temp_filtered_sales;
    RETURN v_result;
END;
$$;

-- Function: Get City View Data
CREATE OR REPLACE FUNCTION get_city_view_data(
    p_filial text default null,
    p_cidade text default null,
    p_supervisor text default null,
    p_vendedor text default null,
    p_fornecedor text default null,
    p_ano text default null,
    p_mes text default null
)
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_year int;
    v_target_month int;
    v_result json;
    v_active_clients json;
    v_inactive_clients json;
BEGIN
    -- Defaults
    IF p_ano IS NULL OR p_ano = 'todos' OR p_ano = '' THEN
         SELECT MAX(EXTRACT(YEAR FROM dtped))::int INTO v_current_year FROM public.all_sales;
    ELSE
        v_current_year := p_ano::int;
    END IF;

    IF p_mes IS NOT NULL AND p_mes != '' AND p_mes != 'todos' THEN
        v_target_month := p_mes::int + 1; -- JS 0-based -> SQL 1-based
    ELSE
         -- Default to last month with sales in current year
         SELECT MAX(EXTRACT(MONTH FROM dtped))::int INTO v_target_month
         FROM public.all_sales
         WHERE EXTRACT(YEAR FROM dtped) = v_current_year;
    END IF;
    IF v_target_month IS NULL THEN v_target_month := 12; END IF;

    -- Active Clients in Target Month
    WITH sales_filtered AS (
        SELECT * FROM public.all_sales
        WHERE EXTRACT(YEAR FROM dtped) = v_current_year
          AND EXTRACT(MONTH FROM dtped) = v_target_month
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
    ),
    client_totals AS (
        SELECT codcli, SUM(vlvenda) as total_fat
        FROM sales_filtered
        GROUP BY codcli
        HAVING SUM(vlvenda) > 0
    )
    SELECT json_agg(
        json_build_object(
            'Código', c.codigo_cliente,
            'fantasia', c.fantasia,
            'razaoSocial', c.razaosocial,
            'totalFaturamento', ct.total_fat,
            'cidade', c.cidade,
            'bairro', c.bairro,
            'rca1', c.rca1,
            'rca2', c.rca2
        ) ORDER BY ct.total_fat DESC
    ) INTO v_active_clients
    FROM client_totals ct
    JOIN public.data_clients c ON c.codigo_cliente = ct.codcli;

    -- Inactive Clients (No sales in target month, but active in DB)
    -- Must respect filters. If "Supervisor" selected, show clients of that supervisor (via RCA)
    -- This part is complex due to RCA mapping.
    -- Simplified: Return clients matching City/Supervisor(via RCA) who are NOT in client_totals.

    -- Filter Clients logic similar to KPI Base
    WITH relevant_rcas AS (
        SELECT DISTINCT codusur
        FROM public.all_sales s
        WHERE (p_supervisor IS NULL OR p_supervisor = '' OR s.superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR s.nome = p_vendedor)
    )
    SELECT json_agg(
        json_build_object(
            'Código', c.codigo_cliente,
            'fantasia', c.fantasia,
            'razaoSocial', c.razaosocial,
            'cidade', c.cidade,
            'bairro', c.bairro,
            'ultimaCompra', c.ultimacompra,
            'rca1', c.rca1,
            'rca2', c.rca2
        ) ORDER BY c.ultimacompra DESC NULLS LAST
    ) INTO v_inactive_clients
    FROM public.data_clients c
    WHERE c.bloqueio != 'S'
      AND (p_cidade IS NULL OR p_cidade = '' OR c.cidade = p_cidade)
      AND (
          (p_supervisor IS NULL AND p_vendedor IS NULL)
          OR
          (c.rca1 IN (SELECT codusur FROM relevant_rcas))
      )
      AND c.codigo_cliente NOT IN (SELECT codcli FROM client_totals);

    v_result := json_build_object(
        'active_clients', COALESCE(v_active_clients, '[]'::json),
        'inactive_clients', COALESCE(v_inactive_clients, '[]'::json)
    );

    RETURN v_result;
END;
$$;

-- Function: Get Filters (Populate Dropdowns)
CREATE OR REPLACE FUNCTION get_dashboard_filters()
RETURNS JSON
LANGUAGE plpgsql
AS $$
DECLARE
    v_supervisors text[];
    v_vendedores text[];
    v_fornecedores json;
    v_cidades text[];
    v_filiais text[];
    v_anos int[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT superv ORDER BY superv) INTO v_supervisors FROM public.all_sales WHERE superv IS NOT NULL;
    SELECT ARRAY_AGG(DISTINCT nome ORDER BY nome) INTO v_vendedores FROM public.all_sales WHERE nome IS NOT NULL;
    SELECT json_agg(json_build_object('cod', codfor, 'name', fornecedor)) FROM (
        SELECT DISTINCT codfor, fornecedor FROM public.all_sales WHERE codfor IS NOT NULL ORDER BY fornecedor
    ) t INTO v_fornecedores;
    SELECT ARRAY_AGG(DISTINCT cidade ORDER BY cidade) INTO v_cidades FROM public.all_sales WHERE cidade IS NOT NULL;
    SELECT ARRAY_AGG(DISTINCT filial ORDER BY filial) INTO v_filiais FROM public.all_sales WHERE filial IS NOT NULL;
    SELECT ARRAY_AGG(DISTINCT EXTRACT(YEAR FROM dtped)::int ORDER BY 1 DESC) INTO v_anos FROM public.all_sales WHERE dtped IS NOT NULL;

    RETURN json_build_object(
        'supervisors', v_supervisors,
        'vendedores', v_vendedores,
        'fornecedores', v_fornecedores,
        'cidades', v_cidades,
        'filiais', v_filiais,
        'anos', v_anos
    );
END;
$$;
