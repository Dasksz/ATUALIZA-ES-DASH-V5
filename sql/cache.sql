
-- Tabela de Cache para Filtros do Dashboard
-- Armazena combinações distintas de filtros para consulta ultra-rápida
DROP TABLE IF EXISTS public.cache_filters CASCADE;

CREATE TABLE IF NOT EXISTS public.cache_filters (
    id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
    filial text,
    cidade text,
    superv text,
    nome text,
    codfor text,
    fornecedor text,
    tipovenda text,
    ano int,
    mes int,
    created_at timestamp with time zone default now()
);

-- Índices para suportar consultas de filtro rápidas
CREATE INDEX IF NOT EXISTS idx_cache_filters_composite 
ON public.cache_filters (ano, mes, filial, cidade, superv, nome, codfor, tipovenda);

CREATE INDEX IF NOT EXISTS idx_cache_filters_fornecedor
ON public.cache_filters (fornecedor);

-- Função para atualizar o Cache (deve ser chamada após carga de dados)
CREATE OR REPLACE FUNCTION refresh_dashboard_cache()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Limpa o cache atual
    TRUNCATE TABLE public.cache_filters;
    
    -- Preenche com dados únicos de ambas as tabelas (Histórico e Detalhado)
    INSERT INTO public.cache_filters (filial, cidade, superv, nome, codfor, fornecedor, tipovenda, ano, mes)
    SELECT DISTINCT 
        filial, 
        cidade, 
        superv, 
        nome, 
        codfor, 
        fornecedor,
        tipovenda,
        EXTRACT(YEAR FROM dtped)::int, 
        EXTRACT(MONTH FROM dtped)::int
    FROM (
        SELECT filial, cidade, superv, nome, codfor, fornecedor, tipovenda, dtped FROM public.data_detailed
        UNION ALL
        SELECT filial, cidade, superv, nome, codfor, fornecedor, tipovenda, dtped FROM public.data_history
    ) t
    WHERE dtped IS NOT NULL;
END;
$$;

-- Populate Cache immediately after recreation
SELECT refresh_dashboard_cache();
