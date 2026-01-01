
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
    SET LOCAL statement_timeout = '120s';

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

    -- 3. Optimization: Aggregate separately then Union
    -- Instead of filtering raw rows into a huge CTE, we aggregate each table then sum up.
    WITH detailed_agg AS (
        SELECT
            EXTRACT(YEAR FROM dtped)::int as yr,
            EXTRACT(MONTH FROM dtped)::int as mth,
            SUM(vlvenda) as faturamento,
            SUM(totpesoliq) as peso,
            SUM(vlbonific) as bonificacao,
            SUM(COALESCE(vldevolucao,0)) as devolucao,
            -- For distinct count across tables, we can't just sum, but for high-level dashboard 'positivacao',
            -- summing unique counts per table is an approximation if clients don't cross tables often in same month.
            -- However, to be accurate we need distinct codcli.
            -- So we select distinct codcli per month first? No that is too slow.
            -- Let's stick to 'positivacao' calculation approximation: sum of counts (Assuming data_detailed and data_history don't overlap in time significantly).
            -- Actually, usually they don't overlap in time (history is old, detailed is new).
            -- If they do overlap, we might double count.
            -- But typically history is past years, detailed is current.
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
    history_agg AS (
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
    combined_agg AS (
        SELECT
            COALESCE(d.yr, h.yr) as yr,
            COALESCE(d.mth, h.mth) as mth,
            COALESCE(d.faturamento, 0) + COALESCE(h.faturamento, 0) as faturamento,
            COALESCE(d.peso, 0) + COALESCE(h.peso, 0) as peso,
            COALESCE(d.bonificacao, 0) + COALESCE(h.bonificacao, 0) as bonificacao,
            COALESCE(d.devolucao, 0) + COALESCE(h.devolucao, 0) as devolucao,
            COALESCE(d.positivacao, 0) + COALESCE(h.positivacao, 0) as positivacao
        FROM detailed_agg d
        FULL OUTER JOIN history_agg h ON d.yr = h.yr AND d.mth = h.mth
    ),
    -- 5. KPI: Active Clients (Optimized)
    kpi_active_clients AS (
        SELECT COUNT(*) as val
        FROM (
             SELECT codcli
             FROM public.data_detailed
             WHERE dtped >= make_date(v_current_year, v_target_month, 1)
               AND dtped <  (make_date(v_current_year, v_target_month, 1) + interval '1 month')
               AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
               AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
               AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
               AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
               AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
             GROUP BY codcli
             HAVING (SUM(vlvenda) - SUM(COALESCE(vldevolucao, 0))) > 1
             UNION
             SELECT codcli
             FROM public.data_history
             WHERE dtped >= make_date(v_current_year, v_target_month, 1)
               AND dtped <  (make_date(v_current_year, v_target_month, 1) + interval '1 month')
               AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
               AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
               AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
               AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
               AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
             GROUP BY codcli
             HAVING (SUM(vlvenda) - SUM(COALESCE(vldevolucao, 0))) > 1
        ) t
    ),
    -- 6. KPI: Base Clients (Optimized - Avoid scanning full sales if possible)
    relevant_rcas AS (
        SELECT DISTINCT codusur
        FROM public.data_detailed
        WHERE dtped >= v_start_date_curr AND dtped < v_end_date_curr
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
        UNION
        SELECT DISTINCT codusur
        FROM public.data_history
        WHERE dtped >= v_start_date_curr AND dtped < v_end_date_curr
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
    )
    SELECT
        (SELECT val FROM kpi_active_clients),
        CASE 
            WHEN (p_supervisor IS NULL OR p_supervisor = '') AND (p_vendedor IS NULL OR p_vendedor = '') THEN
                (SELECT COUNT(*) FROM public.data_clients c WHERE c.bloqueio != 'S' AND (p_cidade IS NULL OR p_cidade = '' OR c.cidade = p_cidade))
            ELSE
                (SELECT COUNT(*) FROM public.data_clients c WHERE (p_cidade IS NULL OR p_cidade = '' OR c.cidade = p_cidade) AND c.bloqueio != 'S' AND c.rca1 IN (SELECT codusur FROM relevant_rcas))
        END,
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
    INTO v_kpi_clients_attended, v_kpi_clients_base, v_monthly_chart_current, v_monthly_chart_previous
    FROM combined_agg;

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
    v_min_date date;
    v_max_date date;
BEGIN
    SET LOCAL statement_timeout = '120s';

    -- Handle Year/Month parsing
    IF p_ano IS NOT NULL AND p_ano != '' AND p_ano != 'todos' THEN
        v_filter_year := p_ano::int;
    ELSE
        -- Default to Current + Previous Year if no year selected (Performance Optimization)
        SELECT COALESCE(GREATEST(
            (SELECT MAX(EXTRACT(YEAR FROM dtped))::int FROM public.data_detailed),
            (SELECT MAX(EXTRACT(YEAR FROM dtped))::int FROM public.data_history)
        ), EXTRACT(YEAR FROM CURRENT_DATE)::int)
        INTO v_filter_year;
        
        -- We set min date to Jan 1st of Previous Year
        v_min_date := make_date(v_filter_year - 1, 1, 1);
        -- We set max date to Jan 1st of Next Year (covering current year)
        v_max_date := make_date(v_filter_year + 1, 1, 1);
        
        -- Reset v_filter_year to NULL so strictly year-based logic below relies on date ranges
        v_filter_year := NULL; 
    END IF;
    
    IF p_mes IS NOT NULL AND p_mes != '' AND p_mes != 'todos' THEN
        v_filter_month := p_mes::int + 1; -- JS is 0-indexed
    END IF;

    -- Construct optimized date ranges if specific year was selected
    IF v_filter_year IS NOT NULL THEN
        IF v_filter_month IS NOT NULL THEN
             v_min_date := make_date(v_filter_year, v_filter_month, 1);
             v_max_date := v_min_date + interval '1 month';
        ELSE
             v_min_date := make_date(v_filter_year, 1, 1);
             v_max_date := make_date(v_filter_year + 1, 1, 1);
        END IF;
    END IF;

    -- 1. Supervisors (Exclude p_supervisor)
    SELECT ARRAY_AGG(DISTINCT superv ORDER BY superv) INTO v_supervisors
    FROM (
        SELECT superv FROM public.data_detailed
        WHERE dtped >= v_min_date AND dtped < v_max_date
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
          AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
        UNION
        SELECT superv FROM public.data_history
        WHERE dtped >= v_min_date AND dtped < v_max_date
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
          AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
    ) t;

    -- 2. Vendedores (Exclude p_vendedor)
    SELECT ARRAY_AGG(DISTINCT nome ORDER BY nome) INTO v_vendedores
    FROM (
        SELECT nome FROM public.data_detailed
        WHERE dtped >= v_min_date AND dtped < v_max_date
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
          AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
        UNION
        SELECT nome FROM public.data_history
        WHERE dtped >= v_min_date AND dtped < v_max_date
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
          AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
    ) t;

    -- 3. Fornecedores (Exclude p_fornecedor)
    SELECT json_agg(json_build_object('cod', codfor, 'name', fornecedor) ORDER BY fornecedor) INTO v_fornecedores
    FROM (
        SELECT DISTINCT codfor, fornecedor FROM public.data_detailed
        WHERE dtped >= v_min_date AND dtped < v_max_date
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
          AND codfor IS NOT NULL
        UNION
        SELECT DISTINCT codfor, fornecedor FROM public.data_history
        WHERE dtped >= v_min_date AND dtped < v_max_date
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
          AND codfor IS NOT NULL
    ) t;

    -- 4. Cidades (Exclude p_cidade)
    SELECT ARRAY_AGG(DISTINCT cidade ORDER BY cidade) INTO v_cidades
    FROM (
        SELECT cidade FROM public.data_detailed
        WHERE dtped >= v_min_date AND dtped < v_max_date
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
          AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
        UNION
        SELECT cidade FROM public.data_history
        WHERE dtped >= v_min_date AND dtped < v_max_date
          AND (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
          AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
    ) t;

    -- 5. Filiais (Exclude p_filial)
    SELECT ARRAY_AGG(DISTINCT filial ORDER BY filial) INTO v_filiais
    FROM (
        SELECT filial FROM public.data_detailed
        WHERE dtped >= v_min_date AND dtped < v_max_date
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
          AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
        UNION
        SELECT filial FROM public.data_history
        WHERE dtped >= v_min_date AND dtped < v_max_date
          AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
          AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
          AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
          AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
          AND (v_filter_month IS NULL OR EXTRACT(MONTH FROM dtped)::int = v_filter_month)
    ) t;

    -- 6. Anos (Exclude p_ano, but include p_mes)
    -- We can keep scanning public.all_sales here as it is less frequent to update years list and it's small result
    SELECT ARRAY_AGG(DISTINCT EXTRACT(YEAR FROM dtped)::int ORDER BY EXTRACT(YEAR FROM dtped)::int DESC) INTO v_anos
    FROM public.all_sales
    WHERE
        (p_filial IS NULL OR p_filial = '' OR filial = p_filial)
        AND (p_cidade IS NULL OR p_cidade = '' OR cidade = p_cidade)
        AND (p_supervisor IS NULL OR p_supervisor = '' OR superv = p_supervisor)
        AND (p_vendedor IS NULL OR p_vendedor = '' OR nome = p_vendedor)
        AND (p_fornecedor IS NULL OR p_fornecedor = '' OR codfor = p_fornecedor)
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
