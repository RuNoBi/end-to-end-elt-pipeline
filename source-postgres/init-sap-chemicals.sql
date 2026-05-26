-- SAP SD/MM mock on the SAME operational DB as public.customers/orders.
-- Analogue: one MSSQL Server with multiple schemas (e.g. dbo + sap).

CREATE SCHEMA IF NOT EXISTS sap;

-- Material master (SAP MM / MARA-style)
CREATE TABLE IF NOT EXISTS sap.sap_material (
    material_number       VARCHAR(18) PRIMARY KEY,
    material_description  TEXT        NOT NULL,
    material_group        VARCHAR(20) NOT NULL,
    product_hierarchy     VARCHAR(40),
    base_uom              VARCHAR(3)  NOT NULL DEFAULT 'MT',
    plant_code            VARCHAR(4)  NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sap.sap_business_partner (
    partner_number        VARCHAR(10) PRIMARY KEY,
    partner_name          VARCHAR(160) NOT NULL,
    country_code          CHAR(2)      NOT NULL,
    region                VARCHAR(40),
    industry_sector       VARCHAR(40),
    customer_group        VARCHAR(20),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sap.sap_sales_order (
    sales_order_number    VARCHAR(12) PRIMARY KEY,
    partner_number        VARCHAR(10) NOT NULL REFERENCES sap.sap_business_partner (partner_number),
    order_date            DATE        NOT NULL,
    requested_delivery_date DATE,
    sales_organization    VARCHAR(4)  NOT NULL DEFAULT 'TH01',
    distribution_channel  VARCHAR(2)  NOT NULL DEFAULT '10',
    division              VARCHAR(2)  NOT NULL DEFAULT '01',
    currency_code         CHAR(3)     NOT NULL DEFAULT 'THB',
    incoterms             VARCHAR(3),
    order_status          VARCHAR(20) NOT NULL DEFAULT 'COMPLETED',
    changed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sap.sap_sales_order_item (
    sales_order_number    VARCHAR(12) NOT NULL REFERENCES sap.sap_sales_order (sales_order_number),
    item_number           VARCHAR(6)  NOT NULL,
    material_number       VARCHAR(18) NOT NULL REFERENCES sap.sap_material (material_number),
    plant_code            VARCHAR(4)  NOT NULL,
    order_quantity        NUMERIC(13, 3) NOT NULL CHECK (order_quantity > 0),
    net_value             NUMERIC(15, 2) NOT NULL CHECK (net_value >= 0),
    currency_code         CHAR(3)     NOT NULL,
    item_changed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sales_order_number, item_number)
);

CREATE INDEX IF NOT EXISTS idx_sap_sales_order_partner ON sap.sap_sales_order (partner_number);
CREATE INDEX IF NOT EXISTS idx_sap_sales_order_order_date ON sap.sap_sales_order (order_date);
CREATE INDEX IF NOT EXISTS idx_sap_sales_order_changed_at ON sap.sap_sales_order (changed_at);
CREATE INDEX IF NOT EXISTS idx_sap_sales_order_item_material ON sap.sap_sales_order_item (material_number);
CREATE INDEX IF NOT EXISTS idx_sap_sales_order_item_changed ON sap.sap_sales_order_item (item_changed_at);

INSERT INTO sap.sap_material (
    material_number, material_description, material_group, product_hierarchy,
    base_uom, plant_code, updated_at
) VALUES
    ('CAPRO-TECH-25KG',   'Caprolactam — polymer grade (25 kg bags)',           'INTERMEDIATE', 'UBE/Nylon chain',        'MT', 'TH01', NOW() - INTERVAL '90 days'),
    ('LDPE-FILM-7420',    'LDPE film grade — blown film extrusion',            'POLYOLEFIN',   'UBE/Polyethylene',       'MT', 'TH01', NOW() - INTERVAL '88 days'),
    ('HDPE-INJ-5502',     'HDPE injection molding — rigid packaging',          'POLYOLEFIN',   'UBE/Polyethylene',       'MT', 'TH01', NOW() - INTERVAL '87 days'),
    ('AMMSUL-GRAN-50KG',  'Ammonium sulfate fertilizer granules',              'FERTILIZER',   'UBE/Fertilizer',         'MT', 'TH01', NOW() - INTERVAL '86 days'),
    ('PU-PREPOLY-200L',   'Polyurethane system — prepolymer blend',            'SPECIALTY',    'UBE/Urethane chemicals', 'MT', 'TH01', NOW() - INTERVAL '85 days'),
    ('MC-ON-99PCT',       'Melamine crystal — 99% min purity',                 'INTERMEDIATE', 'UBE/Melamine',           'MT', 'JP01', NOW() - INTERVAL '84 days'),
    ('HMDA-TECH',         'Hexamethylenediamine — nylon 66 feedstock',         'INTERMEDIATE', 'UBE/Nylon chain',        'MT', 'JP01', NOW() - INTERVAL '83 days'),
    ('BUTADIENE-REF',     'Butadiene — synthetic rubber feedstock',            'PETROCHEM',    'UBE/Elastomers',         'MT', 'JP01', NOW() - INTERVAL '82 days'),
    ('CO-EG-ANTIFREEZE',  'Ethylene glycol — industrial coolant grade',        'SPECIALTY',    'UBE/Glycols',            'MT', 'TH01', NOW() - INTERVAL '81 days'),
    ('NYLON6-CHIPS',      'Nylon 6 chips — textile filament grade',            'POLYMER',      'UBE/Nylon chain',        'MT', 'TH01', NOW() - INTERVAL '80 days'),
    ('SODA-ASH-DENSE',    'Soda ash dense — glass & chemical processing',      'INORGANIC',    'UBE/Inorganics',         'MT', 'TH01', NOW() - INTERVAL '79 days'),
    ('DMC-SOLVENT',       'Dimethyl carbonate — battery electrolyte solvent',  'SPECIALTY',    'UBE/Green chemicals',    'MT', 'TH01', NOW() - INTERVAL '78 days')
ON CONFLICT (material_number) DO NOTHING;

INSERT INTO sap.sap_business_partner (
    partner_number, partner_name, country_code, region, industry_sector,
    customer_group, updated_at
) VALUES
    ('1000001001', 'SCG Chemicals Public Company Limited',     'TH', 'Eastern',   'Petrochemicals',     'KEY_ACCOUNT', NOW() - INTERVAL '120 days'),
    ('1000001002', 'PTT Global Chemical Public Company',       'TH', 'Rayong',    'Petrochemicals',     'KEY_ACCOUNT', NOW() - INTERVAL '118 days'),
    ('1000001003', 'BASF South East Asia Co., Ltd.',           'TH', 'Bangkok',   'Specialty chemicals','STRATEGIC',   NOW() - INTERVAL '115 days'),
    ('1000001004', 'Indorama Ventures Public Company',         'TH', 'Central',   'Packaging / PET',    'KEY_ACCOUNT', NOW() - INTERVAL '112 days'),
    ('1000001005', 'Toray Industries (Thailand) Ltd.',         'TH', 'Ayutthaya', 'Fibers & textiles',  'STRATEGIC',   NOW() - INTERVAL '110 days'),
    ('1000001006', 'Mitsui Chemicals Asia Pacific Ltd.',       'SG', 'Singapore', 'Trading house',      'EXPORT',      NOW() - INTERVAL '108 days'),
    ('1000001007', 'Lotte Chemical Titan Berhad',              'MY', 'Johor',     'Polyolefins',        'EXPORT',      NOW() - INTERVAL '105 days'),
    ('1000001008', 'Formosa Plastics Corporation',             'TW', 'Mailiao',   'PVC / olefins',      'EXPORT',      NOW() - INTERVAL '102 days'),
    ('1000001009', 'Vinythai Public Company Limited',           'TH', 'Rayong',    'PVC',                'DOMESTIC',    NOW() - INTERVAL '100 days'),
    ('1000001010', 'UBE Chemicals (Asia) Trading Desk',        'TH', 'Bangkok',   'Internal / trading', 'INTERCO',     NOW() - INTERVAL '98 days'),
    ('1000001011', 'AgriGrow Fertilizer Distributors Co.',     'TH', 'Northeast', 'Agriculture',        'DOMESTIC',    NOW() - INTERVAL '95 days'),
    ('1000001012', 'GreenBattery Materials Pte. Ltd.',         'SG', 'Singapore', 'EV battery materials', 'STRATEGIC', NOW() - INTERVAL '92 days')
ON CONFLICT (partner_number) DO NOTHING;

DO $$
DECLARE
    bp RECORD;
    so_num TEXT;
    item_num INT;
    n_orders INT;
    mats TEXT[];
    i INT;
    j INT;
    qty NUMERIC;
    price_per_mt NUMERIC;
BEGIN
    IF EXISTS (SELECT 1 FROM sap.sap_sales_order LIMIT 1) THEN
        RETURN;
    END IF;

    SELECT array_agg(material_number ORDER BY material_number) INTO mats FROM sap.sap_material;

    n_orders := 0;
    FOR bp IN SELECT partner_number FROM sap.sap_business_partner ORDER BY partner_number LOOP
        FOR i IN 1..18 LOOP
            n_orders := n_orders + 1;
            so_num := '45' || lpad((1000000 + n_orders)::text, 8, '0');

            INSERT INTO sap.sap_sales_order (
                sales_order_number, partner_number, order_date, requested_delivery_date,
                sales_organization, distribution_channel, division, currency_code,
                incoterms, order_status, changed_at
            ) VALUES (
                so_num,
                bp.partner_number,
                (CURRENT_DATE - ((n_orders % 180) + 1)),
                (CURRENT_DATE - ((n_orders % 180)) + 14),
                CASE WHEN bp.partner_number IN ('1000001006','1000001007','1000001008') THEN 'SG01' ELSE 'TH01' END,
                '10',
                '01',
                CASE WHEN random() < 0.25 THEN 'USD' ELSE 'THB' END,
                (ARRAY['FOB','CIF','DDP','EXW'])[1 + floor(random() * 4)::int],
                CASE WHEN random() < 0.04 THEN 'CANCELLED' ELSE 'COMPLETED' END,
                NOW() - ((n_orders % 90) || ' days')::interval
            );

            item_num := 0;
            FOR j IN 1..(1 + floor(random() * 3)::int) LOOP
                item_num := item_num + 10;
                qty := round((5 + random() * 95)::numeric, 3);
                price_per_mt := round((800 + random() * 4200)::numeric, 2);

                INSERT INTO sap.sap_sales_order_item (
                    sales_order_number, item_number, material_number, plant_code,
                    order_quantity, net_value, currency_code, item_changed_at
                )
                SELECT
                    so_num,
                    lpad(item_num::text, 6, '0'),
                    mats[1 + floor(random() * array_length(mats, 1))::int],
                    CASE WHEN random() < 0.85 THEN 'TH01' ELSE 'JP01' END,
                    qty,
                    round(qty * price_per_mt, 2),
                    o.currency_code,
                    o.changed_at + (j || ' minutes')::interval
                FROM sap.sap_sales_order o
                WHERE o.sales_order_number = so_num;
            END LOOP;
        END LOOP;
    END LOOP;
END $$;
