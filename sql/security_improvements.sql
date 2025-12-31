-- ==============================================================================
-- SECURITY IMPROVEMENTS: RLS & PERMISSIONS
-- Description: Security configuration to prevent data leaks and unauthorized writes.
-- Matches 'adm' role requirement.
-- ==============================================================================

-- 1. Helper Functions (Security Definer)

-- Check if user is ADMIN (role = 'adm')
CREATE OR REPLACE FUNCTION public.is_admin()
RETURNS boolean AS $$
BEGIN
  -- Service Role is always admin
  IF (select auth.role()) = 'service_role' THEN RETURN true; END IF;

  -- Check profiles table for 'adm' role
  RETURN EXISTS (
    SELECT 1 FROM public.profiles
    WHERE id = (select auth.uid())
    AND role = 'adm'
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Check if user is APPROVED (status = 'aprovado')
CREATE OR REPLACE FUNCTION public.is_approved()
RETURNS boolean AS $$
BEGIN
  IF (select auth.role()) = 'service_role' THEN RETURN true; END IF;

  RETURN EXISTS (
    SELECT 1 FROM public.profiles
    WHERE id = (select auth.uid())
    AND status = 'aprovado'
  );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ==============================================================================
-- 2. Profiles Table Security
-- ==============================================================================

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

-- Read: User sees own profile; Admin sees all.
DROP POLICY IF EXISTS "Profiles Visibility" ON public.profiles;
CREATE POLICY "Profiles Visibility" ON public.profiles FOR SELECT
USING (
  (select auth.uid()) = id
  OR public.is_admin()
);

-- Write: Only Admin can manage profiles (update roles/status).
DROP POLICY IF EXISTS "Admin Manage Profiles" ON public.profiles;
CREATE POLICY "Admin Manage Profiles" ON public.profiles FOR ALL
USING (public.is_admin())
WITH CHECK (public.is_admin());

-- Insert: Users can insert their own profile (usually via trigger, but allowed here for safety)
DROP POLICY IF EXISTS "Users can insert their own profile" ON public.profiles;
CREATE POLICY "Users can insert their own profile" ON public.profiles FOR INSERT
WITH CHECK ((select auth.uid()) = id);

-- Update: Users can update their own profile (limited usually, but base policy allows it, can be refined)
-- Update: Users CANNOT update their own profile to prevent privilege escalation (changing role to 'adm').
-- Only Admins can update profiles.
DROP POLICY IF EXISTS "Users can update own profile" ON public.profiles;


-- ==============================================================================
-- 3. Data Tables Security (detailed, history, clients)
-- ==============================================================================

DO $$
DECLARE
    t text;
BEGIN
    FOR t IN
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name IN ('data_detailed', 'data_history', 'data_clients')
    LOOP
        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY;', t);

        -- Revoke insecure permissions
        EXECUTE format('REVOKE ALL ON public.%I FROM anon;', t);
        EXECUTE format('REVOKE ALL ON public.%I FROM authenticated;', t);

        -- Grant minimal permissions to authenticated (RLS will filter)
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON public.%I TO authenticated;', t);

        -- Read Policy: Approved Users Only
        EXECUTE format('DROP POLICY IF EXISTS "Read Access Approved" ON public.%I;', t);
        EXECUTE format('CREATE POLICY "Read Access Approved" ON public.%I FOR SELECT USING (public.is_approved());', t);

        -- Write Policy: Admins Only
        EXECUTE format('DROP POLICY IF EXISTS "Write Access Admin" ON public.%I;', t);
        EXECUTE format('CREATE POLICY "Write Access Admin" ON public.%I FOR INSERT WITH CHECK (public.is_admin());', t);

        EXECUTE format('DROP POLICY IF EXISTS "Update Access Admin" ON public.%I;', t);
        EXECUTE format('CREATE POLICY "Update Access Admin" ON public.%I FOR UPDATE USING (public.is_admin()) WITH CHECK (public.is_admin());', t);

        EXECUTE format('DROP POLICY IF EXISTS "Delete Access Admin" ON public.%I;', t);
        EXECUTE format('CREATE POLICY "Delete Access Admin" ON public.%I FOR DELETE USING (public.is_admin());', t);
    END LOOP;
END $$;


-- ==============================================================================
-- 4. RPC for Safe Truncate (Admins Only)
-- ==============================================================================

CREATE OR REPLACE FUNCTION public.truncate_table(table_name text)
RETURNS void AS $$
BEGIN
  -- Verify Admin status securely
  IF NOT public.is_admin() THEN
    RAISE EXCEPTION 'Acesso negado. Apenas administradores podem limpar tabelas.';
  END IF;

  -- Whitelist tables
  IF table_name NOT IN (
    'data_detailed', 'data_history', 'data_clients'
  ) THEN
    RAISE EXCEPTION 'Tabela não permitida ou inexistente.';
  END IF;

  EXECUTE format('TRUNCATE TABLE public.%I;', table_name);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant execution to authenticated users (function checks role internally)
REVOKE EXECUTE ON FUNCTION public.truncate_table(text) FROM public;
REVOKE EXECUTE ON FUNCTION public.truncate_table(text) FROM anon;
GRANT EXECUTE ON FUNCTION public.truncate_table(text) TO authenticated;
