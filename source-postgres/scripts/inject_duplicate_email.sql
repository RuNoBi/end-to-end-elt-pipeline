BEGIN;

INSERT INTO public.customers (id, name, email, created_at)
OVERRIDING SYSTEM VALUE
VALUES (900000002, 'Bad Drill Duplicate', 'alice.anderson@example.com', NOW())
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    created_at = EXCLUDED.created_at;

COMMIT;
