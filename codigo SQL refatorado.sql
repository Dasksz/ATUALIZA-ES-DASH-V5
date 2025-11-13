-- =================================================================
-- SCRIPT SQL UNIFICADO V2.0 - PRIME
-- OBJETIVO: Manter segurança RLS, adicionar Índices de performance
--           e criar Funções RPC para cálculos no backend.
-- =================================================================

-- ETAPA 1: POLÍTICAS DE SEGURANÇA (RLS) - (O SEU SCRIPT ORIGINAL)
-- Garantir que a sua lógica de segurança está em vigor.

-- 1.1: Função Auxiliar de Segurança
CREATE OR REPLACE FUNCTION public.is_caller_approved () 
RETURNS boolean 
LANGUAGE sql 
STABLE
SECURITY DEFINER
SET search_path = public AS $$
  SELECT EXISTS (
    SELECT 1 FROM profiles 
    WHERE id = (SELECT auth.uid()) AND status = 'aprovado'
  );
$$;

-- 1.2: Políticas RLS para Tabelas de Dados
-- (Aplicando a todas as suas tabelas de dados)
DO $$
DECLARE
    tbl_name TEXT;
BEGIN
    FOR tbl_name IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name LIKE 'data_%'
    LOOP
        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY;', tbl_name);
        
        EXECUTE format('
            DROP POLICY IF EXISTS "Permitir acesso total para service_role ou aprovados" ON public.%I;
            CREATE POLICY "Permitir acesso total para service_role ou aprovados" ON public.%I
            FOR ALL 
            USING (
              ((SELECT auth.role()) = ''service_role'') OR (public.is_caller_approved())
            )
            WITH CHECK (
              ((SELECT auth.role()) = ''service_role'') OR (public.is_caller_approved())
            );
        ', tbl_name, tbl_name, tbl_name);
    END LOOP;
END $$;

-- 1.3: Políticas RLS para Perfis (Profiles)
ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Perfis: Usuários autenticados podem ver o próprio perfil" ON public.profiles;
DROP POLICY IF EXISTS "Perfis: Usuários autenticados podem criar o próprio perfil" ON public.profiles;
DROP POLICY IF EXISTS "Perfis: Usuários autenticados podem atualizar o próprio perfil" ON public.profiles;

CREATE POLICY "Perfis: Usuários autenticados podem ver o próprio perfil" ON public.profiles
FOR SELECT USING ( (SELECT auth.uid()) = id );
CREATE POLICY "Perfis: Usuários autenticados podem criar o próprio perfil" ON public.profiles
FOR INSERT WITH CHECK ( (SELECT auth.uid()) = id );
CREATE POLICY "Perfis: Usuários autenticados podem atualizar o próprio perfil" ON public.profiles
FOR UPDATE USING ( (SELECT auth.uid()) = id ) WITH CHECK ( (SELECT auth.uid()) = id );

-- =================================================================
-- ETAPA 2: ÍNDICES (INDEXES) - (NOVO!)
-- OBJETIVO: Acelerar drasticamente as consultas de filtro (WHERE).
-- =================================================================

-- 2.1: Índices para data_detailed (Tabela principal de vendas)
CREATE INDEX IF NOT EXISTS idx_detailed_dtped ON public.data_detailed (dtped);
CREATE INDEX IF NOT EXISTS idx_detailed_superv ON public.data_detailed (superv);
CREATE INDEX IF NOT EXISTS idx_detailed_codusur ON public.data_detailed (codusur);
CREATE INDEX IF NOT EXISTS idx_detailed_nome ON public.data_detailed (nome);
CREATE INDEX IF NOT EXISTS idx_detailed_codcli ON public.data_detailed (codcli);
CREATE INDEX IF NOT EXISTS idx_detailed_cidade ON public.data_detailed (cidade);
CREATE INDEX IF NOT EXISTS idx_detailed_observacaofor ON public.data_detailed (observacaofor);
CREATE INDEX IF NOT EXISTS idx_detailed_codfor ON public.data_detailed (codfor);
CREATE INDEX IF NOT EXISTS idx_detailed_produto ON public.data_detailed (produto);
CREATE INDEX IF NOT EXISTS idx_detailed_posicao ON public.data_detailed (posicao);
CREATE INDEX IF NOT EXISTS idx_detailed_tipovenda ON public.data_detailed (tipovenda);
CREATE INDEX IF NOT EXISTS idx_detailed_filial ON public.data_detailed (filial);

-- 2.2: Índices para data_history (Tabela de histórico)
CREATE INDEX IF NOT EXISTS idx_history_dtped ON public.data_history (dtped);
CREATE INDEX IF NOT EXISTS idx_history_superv ON public.data_history (superv);
CREATE INDEX IF NOT EXISTS idx_history_codusur ON public.data_history (codusur);
CREATE INDEX IF NOT EXISTS idx_history_nome ON public.data_history (nome);
CREATE INDEX IF NOT EXISTS idx_history_codcli ON public.data_history (codcli);
CREATE INDEX IF NOT EXISTS idx_history_cidade ON public.data_history (cidade);
CREATE INDEX IF NOT EXISTS idx_history_observacaofor ON public.data_history (observacaofor);
CREATE INDEX IF NOT EXISTS idx_history_codfor ON public.data_history (codfor);
CREATE INDEX IF NOT EXISTS idx_history_produto ON public.data_history (produto);
CREATE INDEX IF NOT EXISTS idx_history_filial ON public.data_history (filial);

-- 2.3: Índices para data_clients (Tabela de clientes)
CREATE INDEX IF NOT EXISTS idx_clients_ramo ON public.data_clients (ramo); -- Para filtro "Rede"
CREATE INDEX IF NOT EXISTS idx_clients_rca1 ON public.data_clients (rca1);
CREATE INDEX IF NOT EXISTS idx_clients_cidade ON public.data_clients (cidade);
CREATE INDEX IF NOT EXISTS idx_clients_codigo_cliente ON public.data_clients (codigo_cliente);

-- 2.4: Índices para data_orders (Tabela de pedidos agregados)
CREATE INDEX IF NOT EXISTS idx_orders_dtped ON public.data_orders (dtped);
CREATE INDEX IF NOT EXISTS idx_orders_superv ON public.data_orders (superv);
CREATE INDEX IF NOT EXISTS idx_orders_nome ON public.data_orders (nome);
CREATE INDEX IF NOT EXISTS idx_orders_codcli ON public.data_orders (codcli);
CREATE INDEX IF NOT EXISTS idx_orders_posicao ON public.data_orders (posicao);
CREATE INDEX IF NOT EXISTS idx_orders_codfors_list ON public.data_orders USING GIN (codfors_list); -- Para filtros de fornecedor
CREATE INDEX IF NOT EXISTS idx_orders_fornecedores_list ON public.data_orders USING GIN (fornecedores_list); -- Para filtros de pasta

-- 2.5: Índices para data_product_details
CREATE INDEX IF NOT EXISTS idx_product_details_code ON public.data_product_details (code);
CREATE INDEX IF NOT EXISTS idx_product_details_codfor ON public.data_product_details (codfor);

-- 2.6: Índice para data_stock
CREATE INDEX IF NOT EXISTS idx_stock_product_code ON public.data_stock (product_code);
CREATE INDEX IF NOT EXISTS idx_stock_filial ON public.data_stock (filial);

-- =================================================================
-- ETAPA 3: FUNÇÕES DE CÁLCULO (RPC) - (NOVO!)
-- OBJETIVO: Mover todo o processamento pesado do JavaScript
--           para o Banco de Dados.
-- =================================================================

-- 3.0: Função Auxiliar de Filtro de Cliente (BASE)
-- Esta função interna será usada por todas as outras funções RPC
-- para filtrar clientes com base nos filtros de UI.
CREATE OR REPLACE FUNCTION get_filtered_client_base(
    p_supervisor TEXT DEFAULT NULL,
    p_vendedor_nomes TEXT[] DEFAULT NULL,
    p_rede_group TEXT DEFAULT NULL,
    p_redes TEXT[] DEFAULT NULL,
    p_cidade TEXT DEFAULT NULL,
    p_codcli TEXT DEFAULT NULL,
    p_filial TEXT DEFAULT NULL -- '05', '08', 'ambas'
)
RETURNS TABLE(codigo_cliente TEXT)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    -- CTE para obter a última filial de cada cliente (lógica do seu JS)
    WITH ClientLastBranch AS (
        SELECT DISTINCT ON (codcli) codcli, filial
        FROM (
            SELECT codcli, filial, dtped FROM public.data_detailed
            UNION ALL
            SELECT codcli, filial, dtped FROM public.data_history
        ) AS all_sales
        ORDER BY codcli, dtped DESC
    )
    RETURN QUERY
    SELECT c.codigo_cliente
    FROM public.data_clients AS c
    LEFT JOIN ClientLastBranch AS clb ON c.codigo_cliente = clb.codcli
    WHERE
        -- Filtro de Rede
        (p_rede_group IS NULL OR
         (p_rede_group = 'sem_rede' AND (c.ramo IS NULL OR c.ramo = 'N/A')) OR
         (p_rede_group = 'com_rede' AND (c.ramo IS NOT NULL AND c.ramo != 'N/A') AND 
            (p_redes IS NULL OR c.ramo = ANY(p_redes)))
        )
    -- Filtro de Vendedor/Supervisor
    AND (p_supervisor IS NULL OR c.rca1 IN (
            SELECT DISTINCT codusur FROM data_detailed WHERE superv = p_supervisor
            UNION
            SELECT DISTINCT codusur FROM data_history WHERE superv = p_supervisor
        ))
    AND (p_vendedor_nomes IS NULL OR c.rca1 IN (
            SELECT DISTINCT codusur FROM data_detailed WHERE nome = ANY(p_vendedor_nomes)
            UNION
            SELECT DISTINCT codusur FROM data_history WHERE nome = ANY(p_vendedor_nomes)
        ))
    -- Filtro de Localização
    AND (p_cidade IS NULL OR c.cidade = p_cidade)
    AND (p_codcli IS NULL OR c.codigo_cliente = p_codcli)
    -- Filtro de Filial (baseado na última compra)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR clb.filial = p_filial);
END;
$$;


-- 3.1: Funções para os Ecrãs 'dashboard-view' e 'orders-view'
-- (Estas foram detalhadas na resposta anterior, incluídas aqui para unificação)

-- KPIs Principais (Usado por AMBOS os ecrãs)
CREATE OR REPLACE FUNCTION get_main_kpis(
    p_pasta TEXT DEFAULT NULL,
    p_supervisor TEXT DEFAULT NULL,
    p_vendedor_nomes TEXT[] DEFAULT NULL,
    p_codcli TEXT DEFAULT NULL,
    p_posicao TEXT DEFAULT NULL,
    p_codfor TEXT DEFAULT NULL,
    p_tipos_venda TEXT[] DEFAULT NULL,
    p_rede_group TEXT DEFAULT NULL,
    p_redes TEXT[] DEFAULT NULL,
    p_cidade TEXT DEFAULT NULL,
    p_filial TEXT DEFAULT 'ambas'
)
RETURNS TABLE(
    total_faturamento NUMERIC,
    total_peso NUMERIC,
    total_skus BIGINT,
    total_pdvs_positivados BIGINT,
    base_clientes_filtro BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_base_clientes_count BIGINT;
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    -- Obter a contagem da base de clientes com base nos filtros
    SELECT COUNT(*)
    INTO v_base_clientes_count
    FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial);

    RETURN QUERY
    WITH VendasFiltradas AS (
        SELECT v.vlvenda, v.vlbonific, v.totpesoliq, v.codcli, v.produto
        FROM public.data_detailed AS v
        WHERE
            (p_pasta IS NULL OR v.observacaofor = p_pasta)
        AND (p_supervisor IS NULL OR v.superv = p_supervisor)
        AND (p_vendedor_nomes IS NULL OR v.nome = ANY(p_vendedor_nomes))
        AND (p_codcli IS NULL OR v.codcli = p_codcli)
        AND (p_posicao IS NULL OR v.posicao = p_posicao)
        AND (p_codfor IS NULL OR v.codfor = p_codfor)
        AND (p_tipos_venda IS NULL OR v.tipovenda = ANY(p_tipos_venda))
        AND (p_cidade IS NULL OR v.cidade = p_cidade)
        AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)
        -- Filtro de Rede (join com a base de clientes filtrada)
        AND (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (
            SELECT codigo_cliente FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
        )
    )
    SELECT
        COALESCE(SUM(v.vlvenda), 0) AS total_faturamento,
        COALESCE(SUM(v.totpesoliq), 0) AS total_peso,
        COUNT(v.produto) AS total_skus,
        COUNT(DISTINCT v.codcli) AS total_pdvs_positivados,
        v_base_clientes_count AS base_clientes_filtro
    FROM VendasFiltradas AS v
    WHERE v.vlvenda > 0 OR v.vlbonific > 0;
END;
$$;

-- Gráficos de Barras (dashboard-view)
CREATE OR REPLACE FUNCTION get_sales_by_group(
    p_group_by TEXT, -- 'supervisor', 'vendedor', 'categoria'
    p_pasta TEXT DEFAULT NULL,
    p_supervisor TEXT DEFAULT NULL,
    p_vendedor_nomes TEXT[] DEFAULT NULL,
    p_codcli TEXT DEFAULT NULL,
    p_posicao TEXT DEFAULT NULL,
    p_codfor TEXT DEFAULT NULL,
    p_tipos_venda TEXT[] DEFAULT NULL,
    p_rede_group TEXT DEFAULT NULL,
    p_redes TEXT[] DEFAULT NULL,
    p_cidade TEXT DEFAULT NULL,
    p_filial TEXT DEFAULT 'ambas'
)
RETURNS TABLE(group_name TEXT, total_faturamento NUMERIC)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    ),
    VendasFiltradas AS (
        SELECT v.vlvenda, v.superv, v.nome, v.observacaofor
        FROM public.data_detailed AS v
        WHERE
            (p_pasta IS NULL OR v.observacaofor = p_pasta)
        AND (p_supervisor IS NULL OR v.superv = p_supervisor)
        AND (p_vendedor_nomes IS NULL OR v.nome = ANY(p_vendedor_nomes))
        AND (p_codcli IS NULL OR v.codcli = p_codcli)
        AND (p_posicao IS NULL OR v.posicao = p_posicao)
        AND (p_codfor IS NULL OR v.codfor = p_codfor)
        AND (p_tipos_venda IS NULL OR v.tipovenda = ANY(p_tipos_venda))
        AND (p_cidade IS NULL OR v.cidade = p_cidade)
        AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)
        AND (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (SELECT codigo_cliente FROM ClientBase)
    )
    SELECT
        CASE
            WHEN p_group_by = 'supervisor' THEN v.superv
            WHEN p_group_by = 'vendedor' THEN v.nome
            WHEN p_group_by = 'categoria' THEN v.observacaofor
            ELSE 'N/A'
        END AS group_name,
        SUM(v.vlvenda) AS total_faturamento
    FROM VendasFiltradas AS v
    GROUP BY group_name
    ORDER BY total_faturamento DESC
    LIMIT 20; -- Limita para o gráfico não ficar sobrecarregado
END;
$$;

-- Top Produtos (dashboard-view)
CREATE OR REPLACE FUNCTION get_top_products(
    p_metric TEXT, -- 'faturamento' ou 'peso'
    p_pasta TEXT DEFAULT NULL,
    p_supervisor TEXT DEFAULT NULL,
    p_vendedor_nomes TEXT[] DEFAULT NULL,
    p_codcli TEXT DEFAULT NULL,
    p_posicao TEXT DEFAULT NULL,
    p_codfor TEXT DEFAULT NULL,
    p_tipos_venda TEXT[] DEFAULT NULL,
    p_rede_group TEXT DEFAULT NULL,
    p_redes TEXT[] DEFAULT NULL,
    p_cidade TEXT DEFAULT NULL,
    p_filial TEXT DEFAULT 'ambas'
)
RETURNS TABLE(codigo_produto TEXT, descricao_produto TEXT, valor_metrica NUMERIC)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    SELECT
        v.produto AS codigo_produto,
        v.descricao AS descricao_produto,
        COALESCE(SUM(
            CASE
                WHEN p_metric = 'faturamento' THEN v.vlvenda
                WHEN p_metric = 'peso' THEN v.totpesoliq
                ELSE 0
            END
        ), 0) AS valor_metrica
    FROM
        public.data_detailed AS v
    WHERE
        (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisor IS NULL OR v.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR v.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR v.codcli = p_codcli)
    AND (p_posicao IS NULL OR v.posicao = p_posicao)
    AND (p_codfor IS NULL OR v.codfor = p_codfor)
    AND (p_tipos_venda IS NULL OR v.tipovenda = ANY(p_tipos_venda))
    AND (p_cidade IS NULL OR v.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)
    AND (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR v.codcli IN (SELECT codigo_cliente FROM ClientBase)
    GROUP BY
        v.produto, v.descricao
    ORDER BY
        valor_metrica DESC
    LIMIT 10;
END;
$$;

-- Tabela de Pedidos Paginada (orders-view)
CREATE OR REPLACE FUNCTION get_paginated_orders(
    p_page_number INT,
    p_page_size INT,
    p_pasta TEXT DEFAULT NULL,
    p_supervisor TEXT DEFAULT NULL,
    p_vendedor_nomes TEXT[] DEFAULT NULL,
    p_codcli TEXT DEFAULT NULL,
    p_posicao TEXT DEFAULT NULL,
    p_codfor TEXT DEFAULT NULL,
    p_tipos_venda TEXT[] DEFAULT NULL,
    p_rede_group TEXT DEFAULT NULL,
    p_redes TEXT[] DEFAULT NULL,
    p_cidade TEXT DEFAULT NULL,
    p_filial TEXT DEFAULT 'ambas'
)
RETURNS SETOF data_orders
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    RETURN QUERY
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    SELECT *
    FROM public.data_orders AS o
    WHERE
        (p_pasta IS NULL OR (o.fornecedores_list::text[] @> ARRAY[p_pasta]))
    AND (p_supervisor IS NULL OR o.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR o.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR o.codcli = p_codcli)
    AND (p_posicao IS NULL OR o.posicao = p_posicao)
    AND (p_codfor IS NULL OR (o.codfors_list::text[] @> ARRAY[p_codfor]))
    AND (p_tipos_venda IS NULL OR o.tipovenda = ANY(p_tipos_venda))
    AND (p_cidade IS NULL OR o.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR o.filial = p_filial)
    AND (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR o.codcli IN (SELECT codigo_cliente FROM ClientBase)
    ORDER BY
        o.dtped DESC, o.pedido DESC
    LIMIT p_page_size
    OFFSET (p_page_number - 1) * p_page_size;
END;
$$;

-- Contagem de Pedidos (orders-view)
CREATE OR REPLACE FUNCTION get_orders_count(
    p_pasta TEXT DEFAULT NULL,
    p_supervisor TEXT DEFAULT NULL,
    p_vendedor_nomes TEXT[] DEFAULT NULL,
    p_codcli TEXT DEFAULT NULL,
    p_posicao TEXT DEFAULT NULL,
    p_codfor TEXT DEFAULT NULL,
    p_tipos_venda TEXT[] DEFAULT NULL,
    p_rede_group TEXT DEFAULT NULL,
    p_redes TEXT[] DEFAULT NULL,
    p_cidade TEXT DEFAULT NULL,
    p_filial TEXT DEFAULT 'ambas'
)
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    total_count BIGINT;
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    SELECT COUNT(*)
    INTO total_count
    FROM public.data_orders AS o
    WHERE
        (p_pasta IS NULL OR (o.fornecedores_list::text[] @> ARRAY[p_pasta]))
    AND (p_supervisor IS NULL OR o.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR o.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR o.codcli = p_codcli)
    AND (p_posicao IS NULL OR o.posicao = p_posicao)
    AND (p_codfor IS NULL OR (o.codfors_list::text[] @> ARRAY[p_codfor]))
    AND (p_tipos_venda IS NULL OR o.tipovenda = ANY(p_tipos_venda))
    AND (p_cidade IS NULL OR o.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR o.filial = p_filial)
    AND (p_rede_group IS NULL AND p_supervisor IS NULL AND p_vendedor_nomes IS NULL AND p_cidade IS NULL AND p_codcli IS NULL) OR o.codcli IN (SELECT codigo_cliente FROM ClientBase);
    
    RETURN total_count;
END;
$$;

-- 3.2: Funções para o Ecrã 'city-view'

-- Gráfico de Cidades/Bairros e Tabelas de Clientes
CREATE OR REPLACE FUNCTION get_city_analysis(
    p_supervisor TEXT DEFAULT NULL,
    p_vendedor_nomes TEXT[] DEFAULT NULL,
    p_rede_group TEXT DEFAULT NULL,
    p_redes TEXT[] DEFAULT NULL,
    p_cidade TEXT DEFAULT NULL,
    p_codcli TEXT DEFAULT NULL
)
RETURNS TABLE(
    tipo_analise TEXT, -- 'chart', 'client_list'
    group_name TEXT, -- Nome da cidade ou bairro
    total_faturamento NUMERIC,
    -- Campos da tabela de clientes
    codigo_cliente TEXT,
    fantasia TEXT,
    cidade TEXT,
    bairro TEXT,
    ultimacompra DATE,
    rca1 TEXT,
    status_cliente TEXT -- 'ativo', 'inativo', 'novo', 'retorno'
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_current_month DATE := date_trunc('month', NOW());
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    -- Base de clientes filtrada
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, 'ambas')
    ),
    -- Vendas do mês atual para os clientes da base
    SalesThisMonth AS (
        SELECT
            codcli,
            cidade,
            bairro,
            SUM(vlvenda) AS total_faturado
        FROM public.data_detailed
        WHERE dtped >= v_current_month AND vlvenda > 0
          AND codcli IN (SELECT cb.codigo_cliente FROM ClientBase cb)
        GROUP BY codcli, cidade, bairro
    )
    -- 1. Dados para os Gráficos
    SELECT
        'chart' AS tipo_analise,
        CASE
            WHEN p_cidade IS NOT NULL THEN s.bairro
            ELSE s.cidade
        END AS group_name,
        SUM(s.total_faturado) AS total_faturamento,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL
    FROM SalesThisMonth AS s
    GROUP BY group_name
    ORDER BY total_faturamento DESC
    LIMIT 10

    UNION ALL

    -- 2. Dados para as Tabelas de Clientes
    SELECT
        'client_list' AS tipo_analise,
        NULL,
        COALESCE(s.total_faturado, 0) AS total_faturamento,
        c.codigo_cliente,
        COALESCE(c.fantasia, c.razaosocial),
        c.cidade,
        c.bairro,
        c.ultimacompra,
        c.rca1,
        CASE
            WHEN s.codcli IS NOT NULL THEN 'ativo'
            WHEN c.datacadastro >= v_current_month THEN 'novo'
            -- Adicionar mais lógica de status do JS se necessário
            ELSE 'inativo'
        END AS status_cliente
    FROM ClientBase AS c
    LEFT JOIN SalesThisMonth AS s ON c.codigo_cliente = s.codcli
    WHERE 
      -- Exclui clientes bloqueados ou específicos
      c.bloqueio != 'S' AND c.rca1 != '53';

END;
$$;


-- 3.3: Funções para o Ecrã 'weekly-view'
-- (Estas são complexas e replicam a lógica do JS)

CREATE OR REPLACE FUNCTION get_weekly_sales_and_rankings(
    p_pasta TEXT DEFAULT NULL,
    p_supervisores TEXT[] DEFAULT NULL
)
RETURNS TABLE(
    tipo_dado TEXT, -- 'venda_semanal', 'rank_positivacao', 'rank_topsellers', 'rank_mix'
    group_name TEXT, -- Dia da semana, nome do vendedor
    week_num INT,
    total_valor NUMERIC
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_current_month DATE := date_trunc('month', NOW());
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    -- Vendas filtradas do mês atual
    WITH VendasFiltradas AS (
        SELECT
            vlvenda, superv, nome, codcli, produto, codfor,
            -- (1=Mon, 7=Sun)
            EXTRACT(ISODOW FROM dtped) AS dia_semana, 
            -- Função para obter o número da semana no mês
            (date_part('day', dtped - date_trunc('month', dtped)) / 7 + 1)::int AS semana_mes
        FROM public.data_detailed
        WHERE dtped >= v_current_month
          AND (p_pasta IS NULL OR observacaofor = p_pasta)
          AND (p_supervisores IS NULL OR superv = ANY(p_supervisores))
          AND superv != 'BALCAO'
    )
    -- 1. Dados para o Gráfico de Vendas Semanais
    SELECT
        'venda_semanal' AS tipo_dado,
        dia_semana::text AS group_name,
        semana_mes AS week_num,
        SUM(vlvenda) AS total_valor
    FROM VendasFiltradas
    WHERE dia_semana BETWEEN 1 AND 5 -- Segunda a Sexta
    GROUP BY semana_mes, dia_semana

    UNION ALL

    -- 2. Ranking de Positivação
    SELECT
        'rank_positivacao' AS tipo_dado,
        nome AS group_name,
        NULL,
        COUNT(DISTINCT codcli) AS total_valor
    FROM VendasFiltradas
    GROUP BY nome
    ORDER BY total_valor DESC
    LIMIT 10

    UNION ALL

    -- 3. Ranking Top Sellers
    SELECT
        'rank_topsellers' AS tipo_dado,
        nome AS group_name,
        NULL,
        SUM(vlvenda) AS total_valor
    FROM VendasFiltradas
    GROUP BY nome
    ORDER BY total_valor DESC
    LIMIT 10

    UNION ALL

    -- 4. Ranking de Mix (lógica complexa do JS)
    SELECT
        'rank_mix' AS tipo_dado,
        nome,
        NULL,
        AVG(mix_count) AS total_valor
    FROM (
        SELECT
            nome,
            codcli,
            COUNT(DISTINCT produto) AS mix_count
        FROM (
            -- Aplica a regra de filtro de mix do seu JS
            SELECT * FROM VendasFiltradas
            WHERE superv = 'OSVALDO NUNES O' OR codfor IN ('707', '708')
        ) AS VendasMix
        GROUP BY nome, codcli
    ) AS MixPorCliente
    GROUP BY nome
    ORDER BY total_valor DESC
    LIMIT 10;
END;
$$;


-- 3.4: Funções para o Ecrã 'comparison-view'
-- (Estas são as mais complexas, pois comparam atual vs. histórico)

-- Função principal para os KPIs e Gráficos
CREATE OR REPLACE FUNCTION get_comparison_data(
    p_pasta TEXT DEFAULT NULL,
    p_supervisor TEXT DEFAULT NULL,
    p_vendedor_nomes TEXT[] DEFAULT NULL,
    p_codcli TEXT DEFAULT NULL,
    p_fornecedores TEXT[] DEFAULT NULL,
    p_produtos TEXT[] DEFAULT NULL,
    p_rede_group TEXT DEFAULT NULL,
    p_redes TEXT[] DEFAULT NULL,
    p_cidade TEXT DEFAULT NULL,
    p_filial TEXT DEFAULT 'ambas'
)
RETURNS TABLE(
    origem TEXT, -- 'current', 'history'
    superv TEXT,
    dtped DATE,
    vlvenda NUMERIC,
    totpesoliq NUMERIC,
    codcli TEXT,
    produto TEXT,
    descricao TEXT,
    codfor TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    -- CTE de Clientes (para filtro de rede)
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, p_codcli, p_filial)
    )
    -- 1. Vendas Atuais (Mês Atual)
    SELECT
        'current' AS origem,
        v.superv,
        v.dtped::date,
        v.vlvenda,
        v.totpesoliq,
        v.codcli,
        v.produto,
        v.descricao,
        v.codfor
    FROM public.data_detailed AS v
    WHERE
        (p_pasta IS NULL OR v.observacaofor = p_pasta)
    AND (p_supervisor IS NULL OR v.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR v.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR v.codcli = p_codcli)
    AND (p_fornecedores IS NULL OR v.codfor = ANY(p_fornecedores))
    AND (p_produtos IS NULL OR v.produto = ANY(p_produtos))
    AND (p_cidade IS NULL OR v.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)
    AND v.codcli IN (SELECT codigo_cliente FROM ClientBase)

    UNION ALL

    -- 2. Vendas Históricas (Todos os meses anteriores)
    SELECT
        'history' AS origem,
        h.superv,
        h.dtped::date,
        h.vlvenda,
        h.totpesoliq,
        h.codcli,
        h.produto,
        h.descricao,
        h.codfor
    FROM public.data_history AS h
    WHERE
        (p_pasta IS NULL OR h.observacaofor = p_pasta)
    AND (p_supervisor IS NULL OR h.superv = p_supervisor)
    AND (p_vendedor_nomes IS NULL OR h.nome = ANY(p_vendedor_nomes))
    AND (p_codcli IS NULL OR h.codcli = p_codcli)
    AND (p_fornecedores IS NULL OR h.codfor = ANY(p_fornecedores))
    AND (p_produtos IS NULL OR h.produto = ANY(p_produtos))
    AND (p_cidade IS NULL OR h.cidade = p_cidade)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR h.filial = p_filial)
    AND h.codcli IN (SELECT codigo_cliente FROM ClientBase);
END;
$$;


-- 3.5: Função para o Ecrã 'stock-view'
-- (Semelhante ao 'comparison-view', retorna blocos de dados para o JS processar)
CREATE OR REPLACE FUNCTION get_stock_analysis_data(
    p_pasta TEXT DEFAULT NULL,
    p_supervisor TEXT DEFAULT NULL,
    p_vendedor_nomes TEXT[] DEFAULT NULL,
    p_fornecedores TEXT[] DEFAULT NULL,
    p_produtos TEXT[] DEFAULT NULL,
    p_rede_group TEXT DEFAULT NULL,
    p_redes TEXT[] DEFAULT NULL,
    p_cidade TEXT DEFAULT NULL,
    p_filial TEXT DEFAULT 'ambas'
)
RETURNS TABLE(
    -- Bloco 1: Vendas (atuais e históricas)
    origem TEXT, -- 'sale', 'stock', 'product'
    produto TEXT,
    dtped DATE,
    qtvenda_embalagem_master NUMERIC,
    -- Bloco 2: Estoque
    stock_qty NUMERIC,
    -- Bloco 3: Detalhes do Produto
    descricao TEXT,
    fornecedor TEXT,
    codfor TEXT,
    dtcadastro DATE
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    -- CTE de Clientes (para filtro de rede)
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, NULL, p_filial)
    ),
    -- Base de produtos relevantes
    ProductBase AS (
        SELECT DISTINCT produto FROM public.data_detailed WHERE (p_produtos IS NULL OR produto = ANY(p_produtos))
        UNION
        SELECT DISTINCT produto FROM public.data_history WHERE (p_produtos IS NULL OR produto = ANY(p_produtos))
        UNION
        SELECT DISTINCT product_code FROM public.data_stock WHERE (p_produtos IS NULL OR product_code = ANY(p_produtos))
    )
    -- 1. Bloco de Vendas (Atuais e Históricas)
    SELECT
        'sale' AS origem,
        v.produto,
        v.dtped::date,
        v.qtvenda_embalagem_master,
        NULL, NULL, NULL, NULL, NULL
    FROM (
        SELECT produto, dtped, qtvenda_embalagem_master FROM public.data_detailed
        UNION ALL
        SELECT produto, dtped, qtvenda_embalagem_master FROM public.data_history
    ) AS v
    WHERE
        v.produto IN (SELECT produto FROM ProductBase)
        AND (p_pasta IS NULL OR v.produto IN (SELECT produto FROM data_detailed WHERE observacaofor = p_pasta)) -- Simplificação
        AND v.codcli IN (SELECT codigo_cliente FROM ClientBase)
        AND (p_fornecedores IS NULL OR v.codfor = ANY(p_fornecedores))
        AND (p_filial = 'ambas' OR p_filial IS NULL OR v.filial = p_filial)

    UNION ALL

    -- 2. Bloco de Estoque
    SELECT
        'stock' AS origem,
        s.product_code,
        NULL, NULL,
        s.stock_qty,
        NULL, NULL, NULL, NULL
    FROM public.data_stock AS s
    WHERE
        s.product_code IN (SELECT produto FROM ProductBase)
    AND (p_filial = 'ambas' OR p_filial IS NULL OR s.filial = p_filial)
    
    UNION ALL

    -- 3. Bloco de Detalhes do Produto
    SELECT
        'product' AS origem,
        p.code,
        NULL, NULL, NULL,
        p.descricao,
        p.fornecedor,
        p.codfor,
        p.dtcadastro::date
    FROM public.data_product_details AS p
    WHERE
        p.code IN (SELECT produto FROM ProductBase)
    AND (p_fornecedores IS NULL OR p.codfor = ANY(p_fornecedores));
END;
$$;

-- 3.6: Função para os Ecrãs 'innovations-view', 'innovations-month-view', 'coverage-view'
-- (Função genérica de cobertura, pois a lógica é a mesma)
CREATE OR REPLACE FUNCTION get_coverage_analysis(
    p_product_codes TEXT[], -- Lista de produtos para analisar
    p_include_bonus BOOLEAN,
    -- Filtros
    p_supervisor TEXT DEFAULT NULL,
    p_vendedor_nomes TEXT[] DEFAULT NULL,
    p_fornecedores TEXT[] DEFAULT NULL,
    p_rede_group TEXT DEFAULT NULL,
    p_redes TEXT[] DEFAULT NULL,
    p_cidade TEXT DEFAULT NULL,
    p_filial TEXT DEFAULT 'ambas'
)
RETURNS TABLE(
    produto TEXT,
    stock_qty NUMERIC,
    current_pdvs BIGINT,
    previous_pdvs BIGINT,
    total_active_clients BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_base_clientes_count BIGINT;
    v_current_month DATE := date_trunc('month', NOW());
    v_previous_month DATE := date_trunc('month', NOW() - '1 month'::interval);
BEGIN
    IF NOT public.is_caller_approved() THEN
        RAISE EXCEPTION 'Acesso não autorizado';
    END IF;

    -- Base de clientes filtrada
    WITH ClientBase AS (
        SELECT * FROM get_filtered_client_base(p_supervisor, p_vendedor_nomes, p_rede_group, p_redes, p_cidade, NULL, p_filial)
    ),
    -- Obter contagem da base (para cálculo de %)
    ClientCount AS (
        SELECT COUNT(*) AS total FROM ClientBase
    ),
    -- Vendas do Mês Atual (filtradas)
    CurrentSales AS (
        SELECT DISTINCT codcli, produto
        FROM public.data_detailed
        WHERE dtped >= v_current_month
          AND produto = ANY(p_product_codes)
          AND (vlvenda > 0 OR (p_include_bonus AND vlbonific > 0))
          AND codcli IN (SELECT codigo_cliente FROM ClientBase)
          AND (p_fornecedores IS NULL OR codfor = ANY(p_fornecedores))
    ),
    -- Vendas do Mês Anterior (filtradas)
    PreviousSales AS (
        SELECT DISTINCT codcli, produto
        FROM public.data_history
        WHERE dtped >= v_previous_month AND dtped < v_current_month
          AND produto = ANY(p_product_codes)
          AND (vlvenda > 0 OR (p_include_bonus AND vlbonific > 0))
          AND codcli IN (SELECT codigo_cliente FROM ClientBase)
          AND (p_fornecedores IS NULL OR codfor = ANY(p_fornecedores))
    ),
    -- Estoque (filtrado)
    Stock AS (
        SELECT product_code, SUM(stock_qty) as total_stock
        FROM public.data_stock
        WHERE product_code = ANY(p_product_codes)
          AND (p_filial = 'ambas' OR p_filial IS NULL OR filial = p_filial)
        GROUP BY product_code
    )
    -- Junta tudo
    SELECT
        p.produto,
        COALESCE(s.total_stock, 0) AS stock_qty,
        COALESCE(curr.pdvs, 0) AS current_pdvs,
        COALESCE(prev.pdvs, 0) AS previous_pdvs,
        (SELECT total FROM ClientCount) AS total_active_clients
    FROM (
        SELECT unnest(p_product_codes) AS produto
    ) AS p
    LEFT JOIN Stock AS s ON p.produto = s.product_code
    LEFT JOIN (
        SELECT produto, COUNT(DISTINCT codcli) AS pdvs
        FROM CurrentSales GROUP BY produto
    ) AS curr ON p.produto = curr.produto
    LEFT JOIN (
        SELECT produto, COUNT(DISTINCT codcli) AS pdvs
        FROM PreviousSales GROUP BY produto
    ) AS prev ON p.produto = prev.produto;
    
END;
$$;


-- 3.7: Funções para popular os FILTROS (Dropdowns)
-- (Estas já foram fornecidas na resposta anterior, mas estão aqui para unificação)

CREATE OR REPLACE FUNCTION get_distinct_supervisors()
RETURNS TABLE(superv TEXT) LANGUAGE SQL SECURITY DEFINER SET search_path = public AS $$
    SELECT DISTINCT superv FROM public.data_detailed WHERE superv IS NOT NULL
    UNION SELECT DISTINCT superv FROM public.data_history WHERE superv IS NOT NULL ORDER BY 1;
$$;

CREATE OR REPLACE FUNCTION get_distinct_vendedores(p_supervisor TEXT DEFAULT NULL)
RETURNS TABLE(nome TEXT) LANGUAGE SQL SECURITY DEFINER SET search_path = public AS $$
    SELECT DISTINCT nome FROM public.data_detailed 
    WHERE nome IS NOT NULL AND (p_supervisor IS NULL OR superv = p_supervisor)
    UNION SELECT DISTINCT nome FROM public.data_history 
    WHERE nome IS NOT NULL AND (p_supervisor IS NULL OR superv = p_supervisor) ORDER BY 1;
$$;

CREATE OR REPLACE FUNCTION get_distinct_fornecedores()
RETURNS TABLE(codfor TEXT, fornecedor TEXT) LANGUAGE SQL SECURITY DEFINER SET search_path = public AS $$
    SELECT DISTINCT codfor, fornecedor FROM public.data_detailed 
    WHERE codfor IS NOT NULL AND fornecedor IS NOT NULL
    UNION SELECT DISTINCT codfor, fornecedor FROM public.data_history 
    WHERE codfor IS NOT NULL AND fornecedor IS NOT NULL ORDER BY 2;
$$;

CREATE OR REPLACE FUNCTION get_distinct_tipos_venda()
RETURNS TABLE(tipovenda TEXT) LANGUAGE SQL SECURITY DEFINER SET search_path = public AS $$
    SELECT DISTINCT tipovenda FROM public.data_detailed 
    WHERE tipovenda IS NOT NULL
    UNION SELECT DISTINCT tipovenda FROM public.data_history 
    WHERE tipovenda IS NOT NULL ORDER BY 1;
$$;

CREATE OR REPLACE FUNCTION get_distinct_redes()
RETURNS TABLE(ramo TEXT) LANGUAGE SQL SECURITY DEFINER SET search_path = public AS $$
    SELECT DISTINCT ramo FROM public.data_clients 
    WHERE ramo IS NOT NULL AND ramo != 'N/A'
    ORDER BY 1;
$$;

-- ETAPA FINAL: Forçar o Supabase a recarregar o esquema
NOTIFY pgrst, 'reload schema';
