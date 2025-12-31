-- Indexes for Performance Optimization
-- These indexes support the SARGable queries and Loose Index Scans implemented in the RPCs.

-- 1. Composite Indexes for Data Tables
-- These cover the common filter combinations + time range queries.
-- Note: 'data_detailed' and 'data_history' are the underlying tables for 'all_sales' view.

-- Detailed Data
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_detailed_dtped_composite
ON public.data_detailed (dtped, filial, cidade, superv, nome, codfor);

-- History Data
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_history_dtped_composite
ON public.data_history (dtped, filial, cidade, superv, nome, codfor);


-- 2. Indexes for Loose Index Scans (Filters)
-- These ensure getting distinct values is fast.
-- Most already exist as single column indexes, ensuring they are present here.

-- Supervisors
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_detailed_superv_btree ON public.data_detailed (superv);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_history_superv_btree ON public.data_history (superv);

-- Vendedores (nome)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_detailed_nome_btree ON public.data_detailed (nome);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_history_nome_btree ON public.data_history (nome);

-- Cidades
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_detailed_cidade_btree ON public.data_detailed (cidade);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_history_cidade_btree ON public.data_history (cidade);

-- Filiais
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_detailed_filial_btree ON public.data_detailed (filial);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_history_filial_btree ON public.data_history (filial);


-- 3. Client Data Indexes
-- Support for get_city_view_data joins and filtering

-- RCA2 was missing in original schema
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clients_rca2 ON public.data_clients (rca2);

-- Composite index for Client City lookup (often used together)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clients_cidade_composite ON public.data_clients (cidade, rca1, rca2);
