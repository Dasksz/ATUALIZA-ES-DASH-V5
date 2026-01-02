-- Enable UUID extension
create extension if not exists "uuid-ossp";

-- 1. Tabela de Vendas Detalhadas (Mês Atual / Recente)
create table if not exists public.data_detailed (
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
  tipovenda text,
  filial text,
  created_at timestamp with time zone default now()
);

-- 2. Tabela de Histórico de Vendas
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
  tipovenda text,
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

-- 4. Tabela de Perfis (Profiles)
create table if not exists public.profiles (
  id uuid references auth.users on delete cascade not null primary key,
  email text,
  status text default 'pendente', -- pendente, aprovado, bloqueado
  role text default 'user',
  created_at timestamp with time zone default now(),
  updated_at timestamp with time zone default now()
);

-- View Unificada
create or replace view public.all_sales as
select * from public.data_detailed
union all
select * from public.data_history;

-- Indexes
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

-- RLS
alter table public.data_detailed enable row level security;
alter table public.data_history enable row level security;
alter table public.data_clients enable row level security;
alter table public.profiles enable row level security;

-- Policies
drop policy if exists "Enable access for all users" on public.data_detailed;
create policy "Enable access for all users" on public.data_detailed for all using (true) with check (true);

drop policy if exists "Enable access for all users" on public.data_history;
create policy "Enable access for all users" on public.data_history for all using (true) with check (true);

drop policy if exists "Enable access for all users" on public.data_clients;
create policy "Enable access for all users" on public.data_clients for all using (true) with check (true);

-- Profile Policies
drop policy if exists "Public profiles are viewable by everyone" on public.profiles;
create policy "Public profiles are viewable by everyone" on public.profiles for select using (true);

drop policy if exists "Users can insert their own profile" on public.profiles;
create policy "Users can insert their own profile" on public.profiles for insert with check (auth.uid() = id);

drop policy if exists "Users can update own profile" on public.profiles;
create policy "Users can update own profile" on public.profiles for update using (auth.uid() = id);

-- Trigger for Profile Creation
create or replace function public.handle_new_user()
returns trigger language plpgsql security definer
set search_path = public as $$
begin
  insert into public.profiles (id, email, status)
  values (new.id, new.email, 'pendente');
  return new;
end;
$$;

-- Trigger logic needs to be executed in Supabase SQL editor as triggers on auth.users require admin privileges not available in migration scripts usually
-- drop trigger if exists on_auth_user_created on auth.users;
-- create trigger on_auth_user_created after insert on auth.users for each row execute procedure public.handle_new_user();
