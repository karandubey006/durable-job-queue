-- 005: submission idempotency keys.

ALTER TABLE jobs ADD COLUMN idempotency_key text;

-- At most one job per (tenant, key): the constraint, not a SELECT, is what
-- settles two concurrent submits with the same key (D24). Jobs without a key
-- are unaffected, because NULLs never conflict in a UNIQUE constraint.
-- Scoped by tenant, so two tenants can't collide on (or probe) each other's keys.
--
-- On a large live table you'd build the index first with
-- CREATE UNIQUE INDEX CONCURRENTLY, then attach it with
-- ADD CONSTRAINT ... UNIQUE USING INDEX, to avoid blocking writes.
ALTER TABLE jobs ADD CONSTRAINT jobs_tenant_idempotency_key
    UNIQUE (tenant_id, idempotency_key);
