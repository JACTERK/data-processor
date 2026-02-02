-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.documents (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  content text NOT NULL,
  metadata jsonb,
  embedding USER-DEFINED,
  tenant_id uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT documents_pkey PRIMARY KEY (id),
  CONSTRAINT documents_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id)
);
CREATE TABLE public.ingest_queue (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  notion_db uuid,
  status text NOT NULL DEFAULT 'queued'::text,
  error_message text,
  updated_at timestamp with time zone DEFAULT now(),
  CONSTRAINT ingest_queue_pkey PRIMARY KEY (id),
  CONSTRAINT ingest_queue_notion_db_id_fkey FOREIGN KEY (notion_db) REFERENCES public.notion_dbs(id)
);
CREATE TABLE public.notion_dbs (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  notion_api_key text,
  tenant_id uuid,
  notion_database_id text,
  webhook_secret text,
  name text,
  CONSTRAINT notion_dbs_pkey PRIMARY KEY (id),
  CONSTRAINT notion_dbs_tenant_id_fkey FOREIGN KEY (tenant_id) REFERENCES public.tenants(id)
);
CREATE TABLE public.tenants (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  name text,
  CONSTRAINT tenants_pkey PRIMARY KEY (id)
);
