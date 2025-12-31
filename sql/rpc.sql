
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
-- Function: Get Main Dashboard Data (Optimized)
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
    v_start_date_curr date;
    v_end_date_curr date;
    v_start_date_prev date;
    v_end_date_prev date;

    v_kpi_clients_attended int;
    v_kpi_clients_base int;

    v_monthly_chart_current json;
    v_monthly_chart_previous json;

    v_result json;
BEGIN
    -- 1. Determine Years and Dates
    IF p_ano IS NULL OR p_ano = 'todos' OR p_ano = '' THEN
        -- Efficient check for max year using index if available, else standard max
        SELECT COALESCE(MAX(EXTRACT(YEAR FROM dtped))::int, EXTRACT(YEAR FROM CURRENT_DATE)::int)
        INTO v_current_year
        FROM public.all_sales;
    ELSE
        v_current_year := p_ano::int;
    END IF;
    v_previous_year := v_current_year - 1;

    -- Define Date Ranges for Index Usage (SARGability)
    v_start_date_curr := make_date(v_current_year, 1, 1);
    v_end_date_curr := make_date(v_current_year + 1, 1, 1);
    v_start_date_prev := make_date(v_previous_year, 1, 1);
    v_end_date_prev := make_date(v_previous_year + 1, 1, 1);

    -- 2. Determine Month Filter
    IF p_mes IS NOT NULL AND p_mes != '' AND p_mes != 'todos' THEN
        v_target_month := p_mes::int + 1; -- JS is 0-indexed
    ELSE
         -- Find last month with sales in current year efficiently
         SELECT EXTRACT(MONTH FROM MAX(dtped))::int INTO v_target_month
         FROM public.all_sales
         WHERE dtped >= v_start_date_curr AND dtped < v_end_date_curr;

         IF v_target_month IS NULL THEN v_target_month := 12; END IF;
    END IF;

    -- 3. KPIs Calculation
    -- KPI: Active Clients (Positive Sales > 1) in Target Month
    -- Optimized: No Temp Table, direct aggregation with SARGable date filter
    SELECT COUNT(*) INTO v_kpi_clients_attended
    FROM (
        SELECT codcli
        FROM public.all_sales
        WHERE dtped >= make_date(v_current_year, v_target_month, 1)
          AND dtped <  (make_date(v_current_year, v_target_month, 1) + interval '1 month')
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        GROUP BY codcli
        HAVING (SUM(vlvenda) - SUM(COALESCE(vldevolucao, 0))) > 1
    ) t;

    -- KPI: Base Clients
    -- Optimized: Avoid extracting RCAs from full sales if possible, or use distinct scan
    WITH relevant_rcas AS (
        SELECT DISTINCT codusur
        FROM public.all_sales
        WHERE dtped >= v_start_date_curr AND dtped < v_end_date_curr
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
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

    -- 4. Monthly Data for Charts
    -- Single pass aggregation for both years using CASE
    WITH monthly_agg AS (
        SELECT
            EXTRACT(YEAR FROM dtped)::int as yr,
            EXTRACT(MONTH FROM dtped)::int as mth,
            SUM(vlvenda) as faturamento,
            SUM(totpesoliq) as peso,
            SUM(vlbonific) as bonificacao,
            SUM(COALESCE(vldevolucao,0)) as devolucao,
            COUNT(DISTINCT CASE WHEN (vlvenda - COALESCE(vldevolucao,0)) > 1 THEN codcli END) as positivacao
        FROM public.all_sales
        WHERE dtped >= v_start_date_prev AND dtped < v_end_date_curr
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        GROUP BY 1, 2
    )
    SELECT
        COALESCE(json_agg(json_build_object(
            'month_index', mth - 1,
            'faturamento', faturamento,
            'peso', peso,
            'bonificacao', bonificacao,
            'devolucao', devolucao,
            'positivacao', positivacao
        ) ORDER BY mth) FILTER (WHERE yr = v_current_year), '[]'::json),
        COALESCE(json_agg(json_build_object(
            'month_index', mth - 1,
            'faturamento', faturamento,
            'peso', peso,
            'bonificacao', bonificacao,
            'devolucao', devolucao,
            'positivacao', positivacao
        ) ORDER BY mth) FILTER (WHERE yr = v_previous_year), '[]'::json)
    INTO v_monthly_chart_current, v_monthly_chart_previous
    FROM monthly_agg;

    -- 5. Result
    v_result := json_build_object(
        'current_year', v_current_year,
        'previous_year', v_previous_year,
        'target_month_index', v_target_month - 1,
        'kpi_clients_attended', COALESCE(v_kpi_clients_attended, 0),
        'kpi_clients_base', COALESCE(v_kpi_clients_base, 0),
        'monthly_data_current', v_monthly_chart_current,
        'monthly_data_previous', v_monthly_chart_previous
    );

    RETURN v_result;
END;
$$;

-- Function: Get City View Data
-- Function: Get City View Data (Optimized)
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
    v_start_date date;
    v_end_date date;
    v_result json;
    v_active_clients json;
    v_inactive_clients json;
BEGIN
    -- Defaults
    IF p_ano IS NULL OR p_ano = 'todos' OR p_ano = '' THEN
         SELECT COALESCE(MAX(EXTRACT(YEAR FROM dtped))::int, EXTRACT(YEAR FROM CURRENT_DATE)::int)
         INTO v_current_year
         FROM public.all_sales;
    ELSE
        v_current_year := p_ano::int;
    END IF;

    IF p_mes IS NOT NULL AND p_mes != '' AND p_mes != 'todos' THEN
        v_target_month := p_mes::int + 1; -- JS 0-based -> SQL 1-based
    ELSE
         -- Default to last month with sales in current year efficiently
         SELECT EXTRACT(MONTH FROM MAX(dtped))::int INTO v_target_month
         FROM public.all_sales
         WHERE dtped >= make_date(v_current_year, 1, 1)
           AND dtped < make_date(v_current_year + 1, 1, 1);
    END IF;
    IF v_target_month IS NULL THEN v_target_month := 12; END IF;

    -- Define target date range for indices
    v_start_date := make_date(v_current_year, v_target_month, 1);
    v_end_date := v_start_date + interval '1 month';

    -- Active Clients in Target Month
    -- Optimized: No full SELECT *, only aggregation
    WITH client_totals AS (
        SELECT codcli, SUM(vlvenda) as total_fat
        FROM public.all_sales
        WHERE dtped >= v_start_date AND dtped < v_end_date
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
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

    -- Inactive Clients
    -- Optimized: Avoid distinct scan of full sales if possible
    WITH relevant_rcas AS (
        SELECT DISTINCT codusur
        FROM public.all_sales s
        WHERE dtped >= make_date(v_current_year, 1, 1) -- Assuming inactivity is relevant for current year context
          AND dtped < make_date(v_current_year + 1, 1, 1)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR s.superv = p_supervisor)
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
      -- Check inactivity efficiently: client must not be in active list
      -- We can assume 'client_totals' CTE is available if we used a temp table, but here we can re-derive or use NOT EXISTS
      AND NOT EXISTS (
          SELECT 1
          FROM public.all_sales s2
          WHERE s2.codcli = c.codigo_cliente
            AND s2.dtped >= v_start_date AND s2.dtped < v_end_date
            -- Re-apply filters to ensure we exclude clients who bought in this filtered view
            AND (p_filial IS NULL OR p_filial = '' OR s2.filial = p_filial)
            AND (p_cidade IS NULL OR p_cidade = '' OR s2.cidade = p_cidade)
            AND (p_supervisor IS NULL OR p_supervisor = '' OR s2.superv = p_supervisor)
            AND (p_vendedor IS NULL OR p_vendedor = '' OR s2.nome = p_vendedor)
            AND (p_fornecedor IS NULL OR p_fornecedor = '' OR s2.codfor = p_fornecedor)
      );

    v_result := json_build_object(
        'active_clients', COALESCE(v_active_clients, '[]'::json),
        'inactive_clients', COALESCE(v_inactive_clients, '[]'::json)
    );

    RETURN v_result;
END;
$$;

-- Function: Get Filters (Populate Dropdowns)
-- Function: Get Filters (Optimized - Recursive CTE for Loose Index Scan)
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
    -- Note: Loose Index Scan requires an index on the column.
    -- If no index exists, it falls back to full scan but is not worse.

    -- Supervisors
    WITH RECURSIVE t AS (
        SELECT min(superv) AS val FROM public.all_sales WHERE superv IS NOT NULL
        UNION ALL
        SELECT (SELECT min(superv) FROM public.all_sales WHERE superv > t.val AND superv IS NOT NULL)
        FROM t WHERE t.val IS NOT NULL
    )
    SELECT ARRAY_AGG(val ORDER BY val) INTO v_supervisors FROM t WHERE val IS NOT NULL;

    -- Vendedores
    WITH RECURSIVE t AS (
        SELECT min(nome) AS val FROM public.all_sales WHERE nome IS NOT NULL
        UNION ALL
        SELECT (SELECT min(nome) FROM public.all_sales WHERE nome > t.val AND nome IS NOT NULL)
        FROM t WHERE t.val IS NOT NULL
    )
    SELECT ARRAY_AGG(val ORDER BY val) INTO v_vendedores FROM t WHERE val IS NOT NULL;

    -- Fornecedores (Slightly more complex due to tuple distinct, defaulting to standard distinct for safety/simplicity as loose index scan for tuples is harder)
    SELECT json_agg(json_build_object('cod', codfor, 'name', fornecedor)) FROM (
        SELECT DISTINCT codfor, fornecedor FROM public.all_sales WHERE codfor IS NOT NULL ORDER BY fornecedor
    ) t INTO v_fornecedores;

    -- Cidades
    WITH RECURSIVE t AS (
        SELECT min(cidade) AS val FROM public.all_sales WHERE cidade IS NOT NULL
        UNION ALL
        SELECT (SELECT min(cidade) FROM public.all_sales WHERE cidade > t.val AND cidade IS NOT NULL)
        FROM t WHERE t.val IS NOT NULL
    )
    SELECT ARRAY_AGG(val ORDER BY val) INTO v_cidades FROM t WHERE val IS NOT NULL;

    -- Filiais
    WITH RECURSIVE t AS (
        SELECT min(filial) AS val FROM public.all_sales WHERE filial IS NOT NULL
        UNION ALL
        SELECT (SELECT min(filial) FROM public.all_sales WHERE filial > t.val AND filial IS NOT NULL)
        FROM t WHERE t.val IS NOT NULL
    )
    SELECT ARRAY_AGG(val ORDER BY val) INTO v_filiais FROM t WHERE val IS NOT NULL;

    -- Anos (Extraction makes loose index scan hard, using standard but optimized)
    -- Since there are very few years, a sequential scan of a year index or just the table is acceptable, but we can optimize if needed.
    -- Standard DISTINCT is fine for low cardinality year extraction if dtped index exists.
    SELECT ARRAY_AGG(DISTINCT EXTRACT(YEAR FROM dtped)::int ORDER BY EXTRACT(YEAR FROM dtped)::int DESC) INTO v_anos FROM public.all_sales WHERE dtped IS NOT NULL;

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
