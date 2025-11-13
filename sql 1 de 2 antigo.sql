-- =================================================================
-- SCRIPT V13.1-A (O SCRIPT PRINCIPAL)
-- OBJETIVO: Corrigir tudo, EXCETO o índice.
-- =================================================================

-- ETAPA 1: FUNÇÃO AUXILIAR OTIMIZADA
create or replace function public.is_caller_approved () 
RETURNS boolean 
LANGUAGE sql 
STABLE
SECURITY DEFINER
set
  search_path = public as $$
  SELECT EXISTS (
    SELECT 1 FROM profiles 
    WHERE id = (select auth.uid()) AND status = 'aprovado'
  );
$$;

-- ETAPA 2: APLICAR POLÍTICAS OTIMIZADAS NAS TABELAS 'DATA_*'
-- Tabela: data_detailed
alter table public.data_detailed ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_detailed;
create policy "Permitir acesso total para service_role ou aprovados" on public.data_detailed 
for all 
using (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
)
with check (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
);

-- Tabela: data_history
alter table public.data_history ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_history;
create policy "Permitir acesso total para service_role ou aprovados" on public.data_history 
for all 
using (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
)
with check (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
);

-- Tabela: data_clients
alter table public.data_clients ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_clients;
create policy "Permitir acesso total para service_role ou aprovados" on public.data_clients 
for all 
using (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
)
with check (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
);

-- Tabela: data_orders
alter table public.data_orders ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_orders;
create policy "Permitir acesso total para service_role ou aprovados" on public.data_orders 
for all 
using (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
)
with check (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
);

-- Tabela: data_product_details
alter table public.data_product_details ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_product_details;
create policy "Permitir acesso total para service_role ou aprovados" on public.data_product_details 
for all 
using (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
)
with check (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
);

-- Tabela: data_active_products
alter table public.data_active_products ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_active_products;
create policy "Permitir acesso total para service_role ou aprovados" on public.data_active_products 
for all 
using (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
)
with check (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
);

-- Tabela: data_stock
alter table public.data_stock ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_stock;
create policy "Permitir acesso total para service_role ou aprovados" on public.data_stock 
for all 
using (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
)
with check (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
);

-- Tabela: data_innovations
alter table public.data_innovations ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_innovations;
create policy "Permitir acesso total para service_role ou aprovados" on public.data_innovations 
for all 
using (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
)
with check (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
);

-- Tabela: data_metadata
alter table public.data_metadata ENABLE row LEVEL SECURITY;
drop policy IF exists "Permitir acesso total para service_role ou aprovados" on public.data_metadata;
create policy "Permitir acesso total para service_role ou aprovados" on public.data_metadata 
for all 
using (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
)
with check (
  ((select auth.role()) = 'service_role')
  or (public.is_caller_approved ())
);

-- ETAPA 3: ATIVAR E OTIMIZAR 'PROFILES'
-- 3.1: Ativar o RLS
alter table public.profiles
enable row level security;

-- 3.2: Apagar políticas antigas ou problemáticas
drop policy IF exists "Usuários podem ver o próprio perfil" on public.profiles;
drop policy IF exists "Users can update their own profile" on public.profiles;
drop policy IF exists "Users can insert their own profile" on public.profiles;
drop policy IF exists "Public profiles are viewable by everyone" on public.profiles;
drop policy IF exists "Perfis: Usuários autenticados podem ver o próprio perfil" on public.profiles;
drop policy IF exists "Perfis: Usuários autenticados podem criar o próprio perfil" on public.profiles;
drop policy IF exists "Perfis: Usuários autenticados podem atualizar o próprio perfil" on public.profiles;

-- 3.3: Criar políticas NOVAS, seguras e otimizadas
create policy "Perfis: Usuários autenticados podem ver o próprio perfil" on public.profiles
for SELECT
using ( (select auth.uid()) = id );

create policy "Perfis: Usuários autenticados podem criar o próprio perfil" on public.profiles
for INSERT
with check ( (select auth.uid()) = id );

create policy "Perfis: Usuários autenticados podem atualizar o próprio perfil" on public.profiles
for UPDATE
using ( (select auth.uid()) = id )
with check ( (select auth.uid()) = id );

-- ETAPA FINAL: Forçar o Supabase a recarregar o esquema
notify pgrst,
  'reload schema';
