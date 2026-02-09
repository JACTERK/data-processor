-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "pgvector";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- TABLES
-- ============================================================

CREATE TABLE public.tenants (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  name text NOT NULL,
  CONSTRAINT tenants_pkey PRIMARY KEY (id)
);

CREATE TABLE public.notion_dbs (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  notion_api_key text NOT NULL,
  tenant_id uuid NOT NULL,
  notion_database_id text NOT NULL,
  webhook_secret text,
  name text,
  CONSTRAINT notion_dbs_pkey PRIMARY KEY (id),
  CONSTRAINT notion_dbs_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) ON DELETE CASCADE,
  CONSTRAINT notion_dbs_notion_database_id_unique UNIQUE (notion_database_id)
);

CREATE TABLE public.documents (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  content text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  embedding vector,
  tenant_id uuid NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  doc_type text NOT NULL,
  parent_id uuid,
  CONSTRAINT documents_pkey PRIMARY KEY (id),
  CONSTRAINT documents_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) ON DELETE CASCADE,
  CONSTRAINT documents_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.documents(id) ON DELETE CASCADE,
  CONSTRAINT documents_doc_type_check CHECK (doc_type IN ('parent', 'child')),
  CONSTRAINT documents_child_must_have_parent CHECK (
    (doc_type = 'child' AND parent_id IS NOT NULL) OR
    (doc_type = 'parent' AND parent_id IS NULL)
  )
);

CREATE TABLE public.ingest_queue (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  notion_db uuid NOT NULL,
  tenant_id uuid NOT NULL,
  status text NOT NULL DEFAULT 'queued'::text,
  error_message text,
  updated_at timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT ingest_queue_pkey PRIMARY KEY (id),
  CONSTRAINT ingest_queue_notion_db_id_fkey FOREIGN KEY (notion_db) REFERENCES public.notion_dbs(id) ON DELETE CASCADE,
  CONSTRAINT ingest_queue_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id) ON DELETE CASCADE,
  CONSTRAINT ingest_queue_status_check CHECK (status IN ('queued', 'processing', 'completed', 'failed'))
);

-- ============================================================
-- INDEXES
-- ============================================================

-- Documents: parent-child structure
CREATE INDEX idx_documents_doc_type ON public.documents (doc_type) WHERE doc_type = 'child';
CREATE INDEX idx_documents_parent_id ON public.documents (parent_id) WHERE parent_id IS NOT NULL;

-- Documents: tenant scoping (used by all queries)
CREATE INDEX idx_documents_tenant_id ON public.documents (tenant_id);

-- Documents: metadata lookups used by GetExistingPageMap, ReplacePageDocuments, DeleteDocument
CREATE INDEX idx_documents_notion_page_id ON public.documents ((metadata->>'notion_page_id'));
CREATE INDEX idx_documents_notion_db ON public.documents ((metadata->>'notion_db'));

-- Documents: composite for the GetExistingPageMap query
CREATE INDEX idx_documents_tenant_doctype_notiondb ON public.documents (tenant_id, doc_type, (metadata->>'notion_db'))
  WHERE doc_type = 'parent';

-- Notion DBs: lookup by external Notion database ID (used by webhook handler)
CREATE INDEX idx_notion_dbs_notion_database_id ON public.notion_dbs (notion_database_id);

-- Notion DBs: tenant scoping
CREATE INDEX idx_notion_dbs_tenant_id ON public.notion_dbs (tenant_id);

-- Ingest Queue: polling query (find oldest queued job)
CREATE INDEX idx_ingest_queue_status_created ON public.ingest_queue (created_at ASC)
  WHERE status = 'queued';

-- Ingest Queue: tenant scoping
CREATE INDEX idx_ingest_queue_tenant_id ON public.ingest_queue (tenant_id);

-- ============================================================
-- ROW LEVEL SECURITY
-- ============================================================

-- Enable RLS on all tables
ALTER TABLE public.tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.notion_dbs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ingest_queue ENABLE ROW LEVEL SECURITY;

-- Service role bypasses RLS (used by this data processor service)
-- Supabase service_role key automatically bypasses RLS, so these policies
-- only apply to client-side access (anon, authenticated roles).

-- Tenants: users can only see their own tenant
CREATE POLICY tenants_select ON public.tenants
  FOR SELECT
  USING (id = (current_setting('app.current_tenant_id', true))::uuid);

-- Notion DBs: scoped to tenant
CREATE POLICY notion_dbs_select ON public.notion_dbs
  FOR SELECT
  USING (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

CREATE POLICY notion_dbs_insert ON public.notion_dbs
  FOR INSERT
  WITH CHECK (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

CREATE POLICY notion_dbs_update ON public.notion_dbs
  FOR UPDATE
  USING (tenant_id = (current_setting('app.current_tenant_id', true))::uuid)
  WITH CHECK (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

CREATE POLICY notion_dbs_delete ON public.notion_dbs
  FOR DELETE
  USING (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

-- Documents: scoped to tenant
CREATE POLICY documents_select ON public.documents
  FOR SELECT
  USING (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

CREATE POLICY documents_insert ON public.documents
  FOR INSERT
  WITH CHECK (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

CREATE POLICY documents_update ON public.documents
  FOR UPDATE
  USING (tenant_id = (current_setting('app.current_tenant_id', true))::uuid)
  WITH CHECK (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

CREATE POLICY documents_delete ON public.documents
  FOR DELETE
  USING (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

-- Ingest Queue: scoped to tenant
CREATE POLICY ingest_queue_select ON public.ingest_queue
  FOR SELECT
  USING (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

CREATE POLICY ingest_queue_insert ON public.ingest_queue
  FOR INSERT
  WITH CHECK (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

CREATE POLICY ingest_queue_update ON public.ingest_queue
  FOR UPDATE
  USING (tenant_id = (current_setting('app.current_tenant_id', true))::uuid)
  WITH CHECK (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);

CREATE POLICY ingest_queue_delete ON public.ingest_queue
  FOR DELETE
  USING (tenant_id = (current_setting('app.current_tenant_id', true))::uuid);
