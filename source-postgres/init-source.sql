-- Source database bootstrap for the DE PoC.
-- Executed once by the postgres:16-alpine entrypoint on first container start.

CREATE SCHEMA IF NOT EXISTS public;

SET search_path TO public;

CREATE TABLE IF NOT EXISTS public.customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(120)        NOT NULL,
    email       VARCHAR(255) UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ         NOT NULL DEFAULT NOW()
);

INSERT INTO public.customers (name, email, created_at) VALUES
    ('Alice Anderson',    'alice.anderson@example.com',    NOW() - INTERVAL '30 days'),
    ('Bob Brown',         'bob.brown@example.com',         NOW() - INTERVAL '29 days'),
    ('Carol Clark',       'carol.clark@example.com',       NOW() - INTERVAL '28 days'),
    ('David Davis',       'david.davis@example.com',       NOW() - INTERVAL '27 days'),
    ('Eva Evans',         'eva.evans@example.com',         NOW() - INTERVAL '26 days'),
    ('Frank Foster',      'frank.foster@example.com',      NOW() - INTERVAL '25 days'),
    ('Grace Green',       'grace.green@example.com',       NOW() - INTERVAL '24 days'),
    ('Henry Hill',        'henry.hill@example.com',        NOW() - INTERVAL '23 days'),
    ('Ivy Irwin',         'ivy.irwin@example.com',         NOW() - INTERVAL '22 days'),
    ('Jack Johnson',      'jack.johnson@example.com',      NOW() - INTERVAL '21 days'),
    ('Karen King',        'karen.king@example.com',        NOW() - INTERVAL '20 days'),
    ('Liam Lee',          'liam.lee@example.com',          NOW() - INTERVAL '19 days'),
    ('Mia Martinez',      'mia.martinez@example.com',      NOW() - INTERVAL '18 days'),
    ('Noah Nelson',       'noah.nelson@example.com',       NOW() - INTERVAL '17 days'),
    ('Olivia Owens',      'olivia.owens@example.com',      NOW() - INTERVAL '16 days'),
    ('Peter Perez',       'peter.perez@example.com',       NOW() - INTERVAL '15 days'),
    ('Quinn Quinn',       'quinn.quinn@example.com',       NOW() - INTERVAL '14 days'),
    ('Rachel Reed',       'rachel.reed@example.com',       NOW() - INTERVAL '13 days'),
    ('Samuel Scott',      'samuel.scott@example.com',      NOW() - INTERVAL '12 days'),
    ('Tina Turner',       'tina.turner@example.com',       NOW() - INTERVAL '11 days');
