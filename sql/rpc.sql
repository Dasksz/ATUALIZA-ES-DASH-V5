
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
    -- Increase timeout for large data aggregation
    SET LOCAL statement_timeout = '60s';

    -- 1. Determine Years and Dates
    IF p_ano IS NULL OR p_ano = 'todos' OR p_ano = '' THEN
        -- Check both tables to find the true latest year
        SELECT COALESCE(GREATEST(
            (SELECT MAX(EXTRACT(YEAR FROM dtped))::int FROM public.data_detailed),
            (SELECT MAX(EXTRACT(YEAR FROM dtped))::int FROM public.data_history)
        ), EXTRACT(YEAR FROM CURRENT_DATE)::int)
        INTO v_current_year;
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
         -- Find last month with sales in current year by checking both tables
         SELECT COALESCE(GREATEST(
            (SELECT EXTRACT(MONTH FROM MAX(dtped))::int FROM public.data_detailed WHERE dtped >= v_start_date_curr AND dtped < v_end_date_curr),
            (SELECT EXTRACT(MONTH FROM MAX(dtped))::int FROM public.data_history WHERE dtped >= v_start_date_curr AND dtped < v_end_date_curr)
         ), 12) -- Default to Dec if null
         INTO v_target_month;
    END IF;

    -- 3. KPIs Calculation
    -- KPI: Active Clients (Positive Sales > 1) in Target Month
    -- Optimized: UNION ALL of direct tables instead of View to help planner push down predicates
    SELECT COUNT(*) INTO v_kpi_clients_attended
    FROM (
        SELECT codcli, SUM(vlvenda) as sum_venda, SUM(COALESCE(vldevolucao, 0)) as sum_dev
        FROM (
            SELECT codcli, vlvenda, vldevolucao FROM public.data_detailed
            WHERE dtped >= make_date(v_current_year, v_target_month, 1)
              AND dtped <  (make_date(v_current_year, v_target_month, 1) + interval '1 month')
              AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
              AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
              AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
              AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
              AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
            UNION ALL
            SELECT codcli, vlvenda, vldevolucao FROM public.data_history
            WHERE dtped >= make_date(v_current_year, v_target_month, 1)
              AND dtped <  (make_date(v_current_year, v_target_month, 1) + interval '1 month')
              AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
              AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
              AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
              AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
              AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        ) combined
        GROUP BY codcli
        HAVING (SUM(vlvenda) - SUM(COALESCE(vldevolucao, 0))) > 1
    ) t;

    -- KPI: Base Clients
    -- Optimized: Skip 'relevant_rcas' scan if no specific Supervisor/Vendor filters are applied.
    -- This avoids scanning millions of rows to find "Active RCAs" when we just want the base client count.
    
    IF (p_supervisor IS NULL OR p_supervisor = '') AND (p_vendedor IS NULL OR p_vendedor = '') THEN
        SELECT COUNT(*) INTO v_kpi_clients_base
        FROM public.data_clients c
        WHERE c.bloqueio != 'S'
          AND (p_cidade IS NULL OR p_cidade = '' OR c.cidade = p_cidade);
    ELSE
        -- Complex filtering: Only count clients belonging to RCAs active in the period
        WITH relevant_rcas AS (
            SELECT DISTINCT codusur
            FROM (
                SELECT codusur FROM public.data_detailed
                WHERE dtped >= v_start_date_curr AND dtped < v_end_date_curr
                  AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
                  AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
                  AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
                UNION
                SELECT codusur FROM public.data_history
                WHERE dtped >= v_start_date_curr AND dtped < v_end_date_curr
                  AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
                  AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
                  AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
            ) t
        )
        SELECT COUNT(*) INTO v_kpi_clients_base
        FROM public.data_clients c
        WHERE
            (p_cidade IS NULL OR p_cidade = '' OR c.cidade = p_cidade)
            AND c.bloqueio != 'S'
            AND (c.rca1 IN (SELECT codusur FROM relevant_rcas));
    END IF;

    -- 4. Monthly Data for Charts
    -- Optimized: Pre-aggregate by month per table to reduce rows before UNION
    -- We calculate distinct active clients PER TABLE.
    -- Assumption: Detailed and History data do not overlap for the same month (disjoint sets).
    -- This avoids the extremely expensive array_agg of all clients.
    WITH pre_agg_detailed AS (
        SELECT
            EXTRACT(YEAR FROM dtped)::int as yr,
            EXTRACT(MONTH FROM dtped)::int as mth,
            SUM(vlvenda) as faturamento,
            SUM(totpesoliq) as peso,
            SUM(vlbonific) as bonificacao,
            SUM(COALESCE(vldevolucao,0)) as devolucao,
            COUNT(DISTINCT CASE WHEN (vlvenda - COALESCE(vldevolucao,0)) > 1 THEN codcli END) as positivacao
        FROM public.data_detailed
        WHERE dtped >= v_start_date_prev AND dtped < v_end_date_curr
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        GROUP BY 1, 2
    ),
    pre_agg_history AS (
        SELECT
            EXTRACT(YEAR FROM dtped)::int as yr,
            EXTRACT(MONTH FROM dtped)::int as mth,
            SUM(vlvenda) as faturamento,
            SUM(totpesoliq) as peso,
            SUM(vlbonific) as bonificacao,
            SUM(COALESCE(vldevolucao,0)) as devolucao,
            COUNT(DISTINCT CASE WHEN (vlvenda - COALESCE(vldevolucao,0)) > 1 THEN codcli END) as positivacao
        FROM public.data_history
        WHERE dtped >= v_start_date_prev AND dtped < v_end_date_curr
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        GROUP BY 1, 2
    ),
    monthly_agg AS (
        SELECT
            yr,
            mth,
            SUM(faturamento) as faturamento,
            SUM(peso) as peso,
            SUM(bonificacao) as bonificacao,
            SUM(devolucao) as devolucao,
            SUM(positivacao) as positivacao
        FROM (
            SELECT * FROM pre_agg_detailed
            UNION ALL
            SELECT * FROM pre_agg_history
        ) combined_agg
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
CREATE OR REPLACE FUNCTION get_dashboard_filters(
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
    v_supervisors text[];
    v_vendedores text[];
    v_fornecedores json;
    v_cidades text[];
    v_filiais text[];
    v_anos int[];

    -- Helper variables for date filtering
    v_filter_year int;
    v_filter_month int;
BEGIN
    -- Handle Year/Month parsing
    IF p_ano IS NOT NULL AND p_ano != '' AND p_ano != 'todos' THEN
        v_filter_year := p_ano::int;
    END IF;

    IF p_mes IS NOT NULL AND p_mes != '' AND p_mes != 'todos' THEN
        v_filter_month := p_mes::int + 1; -- JS is 0-indexed
    END IF;

    -- 1. Supervisors (Exclude p_supervisor)
    SELECT ARRAY_AGG(DISTINCT superv ORDER BY superv) INTO v_supervisors
    FROM public.all_sales
    WHERE
        (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
        AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
        -- Exclude p_supervisor check
        AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
        AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        AND (v_filter_year IS NULL OR EXTRACT(YEAR FROM dtped)::int = v_filter_year)
        AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month);

    -- 2. Vendedores (Exclude p_vendedor)
    SELECT ARRAY_AGG(DISTINCT nome ORDER BY nome) INTO v_vendedores
    FROM public.all_sales
    WHERE
        (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
        AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
        AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
        -- Exclude p_vendedor check
        AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        AND (v_filter_year IS NULL OR EXTRACT(YEAR FROM dtped)::int = v_filter_year)
        AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month);

    -- 3. Fornecedores (Exclude p_fornecedor)
    SELECT json_agg(json_build_object('cod', codfor, 'name', fornecedor) ORDER BY fornecedor) INTO v_fornecedores
    FROM (
        SELECT DISTINCT codfor, fornecedor
        FROM public.all_sales
        WHERE
            (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
            AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
            AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
            AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
            -- Exclude p_fornecedor check
            AND (v_filter_year IS NULL OR EXTRACT(YEAR FROM dtped)::int = v_filter_year)
            AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
            AND codfor IS NOT NULL
    ) t;

    -- 4. Cidades (Exclude p_cidade)
    SELECT ARRAY_AGG(DISTINCT cidade ORDER BY cidade) INTO v_cidades
    FROM public.all_sales
    WHERE
        (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
        -- Exclude p_cidade check
        AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
        AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
        AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        AND (v_filter_year IS NULL OR EXTRACT(YEAR FROM dtped)::int = v_filter_year)
        AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month);

    -- 5. Filiais (Exclude p_filial)
    SELECT ARRAY_AGG(DISTINCT filial ORDER BY filial) INTO v_filiais
    FROM public.all_sales
    WHERE
        -- Exclude p_filial check
        (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
        AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
        AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
        AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        AND (v_filter_year IS NULL OR EXTRACT(YEAR FROM dtped)::int = v_filter_year)
        AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month);

    -- 6. Anos (Exclude p_ano, but include p_mes)
    SELECT ARRAY_AGG(DISTINCT EXTRACT(YEAR FROM dtped)::int ORDER BY EXTRACT(YEAR FROM dtped)::int DESC) INTO v_anos
    FROM public.all_sales
    WHERE
        (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
        AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
        AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
        AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
        AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        -- Exclude p_ano check
        AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month);

    RETURN json_build_object(
        'supervisors', COALESCE(v_supervisors, '{}'),
        'vendedores', COALESCE(v_vendedores, '{}'),
        'fornecedores', COALESCE(v_fornecedores, '[]'::json),
        'cidades', COALESCE(v_cidades, '{}'),
        'filiais', COALESCE(v_filiais, '{}'),
        'anos', COALESCE(v_anos, '{}')
    );
END;
$$;
