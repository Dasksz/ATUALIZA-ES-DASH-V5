-- Enable UUID extension
create extension if not exists "uuid-ossp";

-- 1. Tabela de Vendas Detalhadas (Mês Atual / Recente)
create table if not exists public.data_detailed (
  id uuid default uuid_generate_v4 () primary key,
  pedido text,
  nome text, -- Vendedor processado
  superv text, -- Supervisor processado
  produto text,
  descricao text,
  fornecedor text,
  observacaofor text,
  codfor text,
  codusur text, -- RCA processado
  codcli text,
  cliente_nome text,
  cidade text,
  bairro text,
  qtvenda numeric,
  codsupervisor text,
  vlvenda numeric,
  vlbonific numeric,
  vldevolucao numeric,
  totpesoliq numeric,
  dtped timestamp with time zone,
  dtsaida timestamp with time zone,
  posicao text,
  estoqueunit numeric,
  qtvenda_embalagem_master numeric,
  filial text,
  created_at timestamp with time zone default now()
);

-- 2. Tabela de Histórico de Vendas (Ano Anterior + Histórico Ano Atual)
create table if not exists public.data_history (
  id uuid default uuid_generate_v4 () primary key,
  pedido text,
  nome text,
  superv text,
  produto text,
  descricao text,
  fornecedor text,
  observacaofor text,
  codfor text,
  codusur text,
  codcli text,
  cliente_nome text,
  cidade text,
  bairro text,
  qtvenda numeric,
  codsupervisor text,
  vlvenda numeric,
  vlbonific numeric,
  vldevolucao numeric,
  totpesoliq numeric,
  dtped timestamp with time zone,
  dtsaida timestamp with time zone,
  posicao text,
  estoqueunit numeric,
  qtvenda_embalagem_master numeric,
  filial text,
  created_at timestamp with time zone default now()
);

-- 3. Tabela de Clientes
create table if not exists public.data_clients (
  id uuid default uuid_generate_v4 () primary key,
  codigo_cliente text unique,
  rca1 text,
  rca2 text,
  cidade text,
  nomecliente text,
  bairro text,
  razaosocial text,
  fantasia text,
  ramo text,
  ultimacompra timestamp with time zone,
  bloqueio text,
  created_at timestamp with time zone default now()
);

-- View Unificada para facilitar queries
create or replace view public.all_sales as
select * from public.data_detailed
union all
select * from public.data_history;

-- Indexes for performance
create index if not exists idx_detailed_dtped on public.data_detailed(dtped);
create index if not exists idx_detailed_superv on public.data_detailed(superv);
create index if not exists idx_detailed_nome on public.data_detailed(nome);
create index if not exists idx_detailed_cidade on public.data_detailed(cidade);
create index if not exists idx_detailed_filial on public.data_detailed(filial);
create index if not exists idx_detailed_codfor on public.data_detailed(codfor);
create index if not exists idx_detailed_codcli on public.data_detailed(codcli);

create index if not exists idx_history_dtped on public.data_history(dtped);
create index if not exists idx_history_superv on public.data_history(superv);
create index if not exists idx_history_nome on public.data_history(nome);
create index if not exists idx_history_cidade on public.data_history(cidade);
create index if not exists idx_history_filial on public.data_history(filial);
create index if not exists idx_history_codfor on public.data_history(codfor);
create index if not exists idx_history_codcli on public.data_history(codcli);

create index if not exists idx_clients_codcli on public.data_clients(codigo_cliente);
create index if not exists idx_clients_cidade on public.data_clients(cidade);
create index if not exists idx_clients_rca1 on public.data_clients(rca1);

-- RLS (Basic - Open for now based on user request context, but implies auth in PRIME)
alter table public.data_detailed enable row level security;
alter table public.data_history enable row level security;
alter table public.data_clients enable row level security;

-- Policy to allow all access for anon (since we are using a public key and user didn't specify auth flow)
-- However, ideally we should restrict. For this task, I'll allow anon select/insert to make it work.
create policy "Enable access for all users" on public.data_detailed for all using (true) with check (true);
create policy "Enable access for all users" on public.data_history for all using (true) with check (true);
create policy "Enable access for all users" on public.data_clients for all using (true) with check (true);
