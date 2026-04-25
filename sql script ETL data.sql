CREATE SCHEMA IF NOT EXISTS "3Ireland";

CREATE TABLE IF NOT EXISTS "3Ireland".raw_data (
    ret_ref_nr TEXT, 
	divisionid TEXT, 
	tran_date TEXT, 
	fraudstatus TEXT,
    masked_card TEXT, 
	total TEXT, 
	tran_currency TEXT, 
	card_issuing_country TEXT,
    cust_ip TEXT, 
	ip_country TEXT, 
	channel TEXT, 
	src_msisdn TEXT,
    dst_msisdn TEXT, 
	home_msisdn TEXT, 
	act_age TEXT, 
	cust_email TEXT,
    cust_first_name TEXT, 
	cust_last_name TEXT, 
	bill_address1 TEXT,
    bill_country TEXT, 
	postal_code TEXT, 
	reversed TEXT
);

-- ==========================================
-- 1. PREGĂTIRE STAGING (Datele brute din fișierul de azi)
-- ==========================================
-- Folosim TRUNCATE ca să nu stricăm dependențele lui raw_data
TRUNCATE TABLE "3Ireland".raw_data;

COPY "3Ireland".raw_data 
FROM 'C:\Date excel si CSV\date 3roi csv.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');



-- ==========================================
-- 2. TABELUL PROCESAT (Arhiva de 7 zile)
-- ==========================================
CREATE TABLE IF NOT EXISTS "3Ireland".processed_data (
    ret_ref_nr TEXT,
    divisionid TEXT,
    tran_date TIMESTAMPTZ,
    fraudstatus TEXT,
    masked_card TEXT,
    total NUMERIC,
    tran_currency TEXT,
    card_issuing_country TEXT,
    cust_ip TEXT,
    ip_country TEXT,
    channel TEXT,
    src_msisdn TEXT,
    dst_msisdn TEXT,
    home_msisdn TEXT,
    act_age INTEGER,
    cust_email TEXT,
    cust_first_name TEXT,
    cust_last_name TEXT,
    bill_address1 TEXT,
    bill_country TEXT,
    postal_code TEXT,
    reversed TEXT,
    card_bin TEXT,
    import_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Inserăm datele noi transformate din raw_data în processed_data
INSERT INTO "3Ireland".processed_data (
    ret_ref_nr, divisionid, tran_date, fraudstatus, masked_card, 
    total, tran_currency, card_issuing_country, cust_ip, ip_country, 
    channel, src_msisdn, dst_msisdn, home_msisdn, act_age, 
    cust_email, cust_first_name, cust_last_name, bill_address1, 
    bill_country, postal_code, reversed, card_bin
)
SELECT 
    r.ret_ref_nr,
    r.divisionid,
    to_timestamp(r.tran_date, 'DD.MM.YYYY HH24:MI:SS') AT TIME ZONE 'UTC',
    CASE WHEN NULLIF(TRIM(r.fraudstatus), '') IS NULL THEN 'SUCCESSFUL' ELSE r.fraudstatus END,
    r.masked_card,
    REGEXP_REPLACE(r.total, '[^0-9.]', '', 'g')::NUMERIC,
    r.tran_currency,
    -- card_issuing_country → NULL dacă e gol / spații
    NULLIF(TRIM(r.card_issuing_country), '') AS card_issuing_country,
    r.cust_ip,
    -- ip_country → NULL dacă e gol / spații
    NULLIF(TRIM(r.ip_country), '') AS ip_country,
    r.channel,
    -- telefoane normalizate
    NULLIF(REGEXP_REPLACE(COALESCE(r.src_msisdn, ''),  '[^0-9]', '', 'g'), ''),
    NULLIF(REGEXP_REPLACE(COALESCE(r.dst_msisdn, ''),  '[^0-9]', '', 'g'), ''),
    NULLIF(REGEXP_REPLACE(COALESCE(r.home_msisdn, ''), '[^0-9]', '', 'g'), ''),
    CASE WHEN r.act_age ~ '^[0-9]+$' THEN r.act_age::INTEGER ELSE NULL END,
    NULLIF(TRIM(LOWER(r.cust_email)), ''),
    r.cust_first_name,
    r.cust_last_name,
    r.bill_address1,
    r.bill_country,
    -- postal_code → NULL dacă e gol / spații
    NULLIF(TRIM(r.postal_code), '') AS postal_code,
    r.reversed,
    CASE WHEN r.masked_card ~ '^[0-9]{6}' THEN LEFT(r.masked_card, 6)  ELSE NULL  END
FROM "3Ireland".raw_data r;


-- Curățăm arhiva: păstrăm doar ultimele 7 zile calendaristice
DELETE FROM "3Ireland".processed_data 
WHERE tran_date < NOW() - INTERVAL '7 days';


-- ==========================================
-- 3. ACTUALIZARE BLACKLIST (Rescriere completă)
-- ==========================================
CREATE TABLE IF NOT EXISTS "3Ireland".global_blacklist (
    entity_type TEXT, 
    entity_value TEXT, 
    reason TEXT
);

-- Ștergem tot și reîncărcăm din fișierul tău actualizat manual
TRUNCATE TABLE "3Ireland".global_blacklist;

COPY "3Ireland".global_blacklist(entity_type, entity_value, reason)
FROM 'C:\Date excel si CSV\black list 3roi.csv' 
WITH (FORMAT CSV, HEADER true, DELIMITER ',');

-- Normalizam tabelul blacklist
UPDATE "3Ireland".global_blacklist
SET
    entity_type  = LOWER(TRIM(entity_type)),
    entity_value = CASE
        WHEN LOWER(TRIM(entity_type)) = 'email'       THEN NULLIF(LOWER(TRIM(entity_value)), '')
        WHEN LOWER(TRIM(entity_type)) = 'ip'          THEN NULLIF(TRIM(entity_value), '')
        WHEN LOWER(TRIM(entity_type)) = 'card_prefix' THEN NULLIF(TRIM(entity_value), '')
        WHEN LOWER(TRIM(entity_type)) = 'bin'         THEN NULLIF(TRIM(entity_value), '')
        ELSE NULLIF(LOWER(TRIM(entity_value)), '')
    END,
    reason       = NULLIF(TRIM(reason), '');

-- Elimină rânduri invalide (goale) după normalizare
DELETE FROM "3Ireland".global_blacklist
WHERE entity_type IS NULL OR entity_type = ''
   OR entity_value IS NULL OR entity_value = '';

   -- Re-creăm indexul dacă nu există pentru performanță
CREATE INDEX IF NOT EXISTS idx_blacklist_lookup ON "3Ireland".global_blacklist (entity_type, entity_value);
CREATE UNIQUE INDEX IF NOT EXISTS uq_blacklist_type_value ON "3Ireland".global_blacklist (entity_type, entity_value);


-- ==========================================
-- 4. VIEW & TABEL CYTOSCAPE (Pe baza arhivei de 7 zile)
-- ==========================================

-- View-ul se va uita acum în processed_data (unde avem datele curate de 7 zile)
DROP VIEW IF EXISTS "3Ireland".final_graph_data;

CREATE VIEW "3Ireland".final_graph_data AS
WITH base AS (
SELECT
    p.*,
    COALESCE(NULLIF(p.dst_msisdn,''), NULLIF(p.src_msisdn,''), NULLIF(p.home_msisdn,'')) AS clean_phone,

    CASE WHEN p.cust_email IS NULL OR TRIM(p.cust_email)='' THEN NULL
         WHEN EXISTS (SELECT 1 FROM "3Ireland".global_blacklist b 
                      WHERE b.entity_type='email' AND b.entity_value=p.cust_email)
         THEN NULL ELSE p.cust_email END AS clean_email,

    CASE WHEN p.masked_card IS NULL OR TRIM(p.masked_card)='' THEN NULL
         WHEN EXISTS (SELECT 1 FROM "3Ireland".global_blacklist b 
                      WHERE b.entity_type='card_prefix' AND p.masked_card LIKE b.entity_value)
         THEN NULL ELSE p.masked_card END AS clean_card,

    CASE WHEN NULLIF(TRIM(p.cust_ip),'') IS NULL THEN NULL
         WHEN (NULLIF(TRIM(p.cust_ip),'') ~ '^\d{1,3}(\.\d{1,3}){3}$') IS NOT TRUE THEN NULL
         WHEN EXISTS (SELECT 1 FROM "3Ireland".global_blacklist b 
                      WHERE b.entity_type='ip' AND b.entity_value=TRIM(p.cust_ip))
         THEN NULL ELSE TRIM(p.cust_ip) END AS clean_ip,

    date_trunc('minute', p.tran_date)   - (EXTRACT(MINUTE FROM p.tran_date)::int % 10) * interval '1 minute' AS time_hub

FROM "3Ireland".processed_data p
),

with_bin AS (
SELECT
    b.*,
    CASE WHEN b.clean_card IS NULL THEN NULL
         WHEN b.clean_card ~ '^[0-9]{6}' THEN LEFT(b.clean_card,6)
         ELSE NULL END AS clean_bin
FROM base b
),

email_metrics AS (
SELECT
    cust_email,
    MAX(CASE WHEN fraudstatus IN ('STOP','BLOCK') THEN total ELSE 0 END) AS total_value_blocked,
    MAX(CASE WHEN fraudstatus IN ('SUCCESSFUL','APPROVE','IGNORE') AND reversed='1' THEN total ELSE 0 END) AS total_value_reversed,
    SUM(CASE WHEN fraudstatus IN ('SUCCESSFUL','APPROVE','IGNORE') AND reversed='0' THEN total ELSE 0 END) AS total_value_success
FROM "3Ireland".processed_data
WHERE cust_email IS NOT NULL
GROUP BY cust_email
)

SELECT w.*, em.total_value_blocked, em.total_value_reversed, em.total_value_success
FROM with_bin w
LEFT JOIN email_metrics em ON w.cust_email = em.cust_email;



-- ==========================================
-- 4. Tabele finale 5x pentru export Cytoscape
-- ==========================================

-- UNION simplu doar card-mail-phone

DROP TABLE IF EXISTS "3Ireland".cytoscape_network_data_simplu;

CREATE TABLE "3Ireland".cytoscape_network_data_simplu AS
WITH edges AS (
    SELECT clean_email AS source, clean_card AS target, 'Card-Email' AS interaction,
           total_value_blocked, total_value_reversed, total_value_success,
           total, time_hub, act_age, card_issuing_country, ip_country, postal_code
    FROM "3Ireland".final_graph_data
    WHERE clean_email IS NOT NULL
      AND clean_card IS NOT NULL
      AND act_age IS NOT NULL
      AND act_age <= 7

    UNION ALL
    SELECT clean_phone AS source, clean_card AS target, 'Card-Phone' AS interaction,
           total_value_blocked, total_value_reversed, total_value_success,
           total, time_hub, act_age, card_issuing_country, ip_country, postal_code
    FROM "3Ireland".final_graph_data
    WHERE clean_phone IS NOT NULL
      AND clean_card IS NOT NULL
      AND act_age IS NOT NULL
      AND act_age <= 7

    UNION ALL
    SELECT clean_email AS source, clean_phone AS target, 'Email-Phone' AS interaction,
           total_value_blocked, total_value_reversed, total_value_success,
           total, time_hub, act_age, card_issuing_country, ip_country, postal_code
    FROM "3Ireland".final_graph_data
    WHERE clean_email IS NOT NULL
      AND clean_phone IS NOT NULL
      AND act_age IS NOT NULL
      AND act_age <= 7
),
agg AS (
    SELECT
        source, target, interaction,
        COUNT(*) AS txn_count,
        SUM(total) AS total_sum
    FROM edges
    GROUP BY source, target, interaction
),
first_txn AS (
    SELECT DISTINCT ON (source, target, interaction)
        source, target, interaction,
        total_value_blocked, total_value_reversed, total_value_success,
        total, time_hub, act_age, card_issuing_country, ip_country, postal_code
    FROM edges
    ORDER BY source, target, interaction, time_hub DESC NULLS LAST
)
SELECT
    f.*,
    a.txn_count,
    a.total_sum
FROM first_txn f
JOIN agg a USING (source, target, interaction);






-- UNION cu IP Card-Email, Card-Phone, Email-Phone, IP-Card, IP-Email


DROP TABLE IF EXISTS "3Ireland".cytoscape_network_data_ip;

CREATE TABLE "3Ireland".cytoscape_network_data_ip AS
WITH edges AS (
    SELECT clean_email AS source, clean_card AS target, 'Card-Email' AS interaction,
           total_value_blocked, total_value_reversed, total_value_success,
           total, time_hub, act_age, card_issuing_country, ip_country, postal_code
    FROM "3Ireland".final_graph_data
    WHERE clean_email IS NOT NULL
      AND clean_card IS NOT NULL
      AND act_age IS NOT NULL
      AND act_age <= 7

    UNION ALL
    SELECT clean_phone AS source, clean_card AS target, 'Card-Phone' AS interaction,
           total_value_blocked, total_value_reversed, total_value_success,
           total, time_hub, act_age, card_issuing_country, ip_country, postal_code
    FROM "3Ireland".final_graph_data
    WHERE clean_phone IS NOT NULL
      AND clean_card IS NOT NULL
      AND act_age IS NOT NULL
      AND act_age <= 7

    UNION ALL
    SELECT clean_email AS source, clean_phone AS target, 'Email-Phone' AS interaction,
           total_value_blocked, total_value_reversed, total_value_success,
           total, time_hub, act_age, card_issuing_country, ip_country, postal_code
    FROM "3Ireland".final_graph_data
    WHERE clean_email IS NOT NULL
      AND clean_phone IS NOT NULL
      AND act_age IS NOT NULL
      AND act_age <= 7

    UNION ALL
    SELECT clean_ip AS source, clean_card AS target, 'IP-Card' AS interaction,
           total_value_blocked, total_value_reversed, total_value_success,
           total, time_hub, act_age, card_issuing_country, ip_country, postal_code
    FROM "3Ireland".final_graph_data
    WHERE clean_ip IS NOT NULL
      AND clean_card IS NOT NULL
      AND act_age IS NOT NULL
      AND act_age <= 7

    UNION ALL
    SELECT clean_ip AS source, clean_email AS target, 'IP-Email' AS interaction,
           total_value_blocked, total_value_reversed, total_value_success,
           total, time_hub, act_age, card_issuing_country, ip_country, postal_code
    FROM "3Ireland".final_graph_data
    WHERE clean_ip IS NOT NULL
      AND clean_email IS NOT NULL
      AND act_age IS NOT NULL
      AND act_age <= 7
),
agg AS (
    SELECT
        source, target, interaction,
        COUNT(*) AS txn_count,
        SUM(total) AS total_sum
    FROM edges
    GROUP BY source, target, interaction
),
first_txn AS (
    SELECT DISTINCT ON (source, target, interaction)
        source, target, interaction,
        total_value_blocked, total_value_reversed, total_value_success,
        total, time_hub, act_age, card_issuing_country, ip_country, postal_code
    FROM edges
    -- “prima tranzacție” = cea mai recenta
    ORDER BY source, target, interaction, time_hub DESC NULLS LAST
)
SELECT
    f.*,
    a.txn_count,
    a.total_sum
FROM first_txn f
JOIN agg a USING (source, target, interaction);




-- UNION cu Postal code   Card-Email, Card-Phone, Email-Phone, postal code-Card, postal code-Email

DROP TABLE IF EXISTS "3Ireland".cytoscape_network_data_postal_code;

CREATE TABLE "3Ireland".cytoscape_network_data_postal_code AS
WITH edges AS (
  SELECT
    postal_code AS source,
    clean_phone AS target,
    'PostalCode-Phone' AS interaction,
    total_value_blocked, total_value_reversed, total_value_success,
    total, time_hub, act_age, card_issuing_country, ip_country, postal_code
  FROM "3Ireland".final_graph_data
  WHERE act_age <= 7
    AND postal_code IS NOT NULL AND postal_code <> ''
    AND clean_phone IS NOT NULL
),
agg AS (
  SELECT source, target, interaction,
         COUNT(*) AS txn_count,
         SUM(total) AS total_sum
  FROM edges
  GROUP BY source, target, interaction
),
last_txn AS (
  SELECT DISTINCT ON (source, target, interaction)
         source, target, interaction,
         total_value_blocked, total_value_reversed, total_value_success,
         total, time_hub, act_age, card_issuing_country, ip_country, postal_code
  FROM edges
  ORDER BY source, target, interaction, time_hub DESC NULLS LAST
)
SELECT l.*, a.txn_count, a.total_sum
FROM last_txn l
JOIN agg a USING (source, target, interaction);






-- UNION cu BIN Card-Email, Card-Phone, Email-Phone, BIN-Phone, BIN-Email

DROP TABLE IF EXISTS "3Ireland".cytoscape_network_data_bin;

CREATE TABLE "3Ireland".cytoscape_network_data_bin AS
WITH edges AS (
  SELECT
    LEFT(clean_card, 6) AS source,
    clean_phone        AS target,
    'BIN-Phone'        AS interaction,
    total_value_blocked, total_value_reversed, total_value_success,
    total, time_hub, act_age, card_issuing_country, ip_country, postal_code
  FROM "3Ireland".final_graph_data
  WHERE act_age <= 7
    AND clean_card IS NOT NULL
    AND LENGTH(clean_card) >= 6
    AND clean_phone IS NOT NULL
),
agg AS (
  SELECT source, target, interaction,
         COUNT(*) AS txn_count,
         SUM(total) AS total_sum
  FROM edges
  GROUP BY source, target, interaction
),
last_txn AS (
  SELECT DISTINCT ON (source, target, interaction)
         source, target, interaction,
         total_value_blocked, total_value_reversed, total_value_success,
         total, time_hub, act_age, card_issuing_country, ip_country, postal_code
  FROM edges
  ORDER BY source, target, interaction, time_hub DESC NULLS LAST
)
SELECT l.*, a.txn_count, a.total_sum
FROM last_txn l
JOIN agg a USING (source, target, interaction);





-- UNION cu time Card-Email, Card-Phone, Email-Phone, time-Card, time-Email

DROP TABLE IF EXISTS "3Ireland".cytoscape_network_data_time;

CREATE TABLE "3Ireland".cytoscape_network_data_time AS
WITH edges AS (
  SELECT
    to_char(
      date_trunc('minute', time_hub)
        - (EXTRACT(MINUTE FROM time_hub)::int % 10) * interval '1 minute',
      'YYYY-MM-DD HH24:MI'
    ) AS source,
    clean_phone AS target,
    'Time-Phone' AS interaction,
    total_value_blocked, total_value_reversed, total_value_success,
    total, time_hub, act_age, card_issuing_country, ip_country, postal_code
  FROM "3Ireland".final_graph_data
  WHERE act_age <= 7
    AND time_hub IS NOT NULL
    AND clean_phone IS NOT NULL
),
agg AS (
  SELECT source, target, interaction,
         COUNT(*) AS txn_count,
         SUM(total) AS total_sum
  FROM edges
  GROUP BY source, target, interaction
),
last_txn AS (
  SELECT DISTINCT ON (source, target, interaction)
         source, target, interaction,
         total_value_blocked, total_value_reversed, total_value_success,
         total, time_hub, act_age, card_issuing_country, ip_country, postal_code
  FROM edges
  ORDER BY source, target, interaction, time_hub DESC NULLS LAST
)
SELECT l.*, a.txn_count, a.total_sum
FROM last_txn l
JOIN agg a USING (source, target, interaction);










-- exportam cele 5 fisiere, in fisier care include in nume ziua de azi
DO $$ 
DECLARE 
    cale_fisier TEXT;
BEGIN 
    -- 1) SIMPLU
    cale_fisier := 'C:\Date excel si CSV\export_cytoscape_simplu_' || to_char(current_date, 'YYYY_MM_DD') || '.csv';
    EXECUTE format(
        'COPY "3Ireland".cytoscape_network_data_simplu TO %L WITH (FORMAT CSV, HEADER true, DELIMITER %L)', 
        cale_fisier, 
        ','
    );

    -- 2) IP
    cale_fisier := 'C:\Date excel si CSV\export_cytoscape_ip_' || to_char(current_date, 'YYYY_MM_DD') || '.csv';
    EXECUTE format(
        'COPY "3Ireland".cytoscape_network_data_ip TO %L WITH (FORMAT CSV, HEADER true, DELIMITER %L)', 
        cale_fisier, 
        ','
    );

    -- 3) POSTAL CODE
    cale_fisier := 'C:\Date excel si CSV\export_cytoscape_postal_code_' || to_char(current_date, 'YYYY_MM_DD') || '.csv';
    EXECUTE format(
        'COPY "3Ireland".cytoscape_network_data_postal_code TO %L WITH (FORMAT CSV, HEADER true, DELIMITER %L)', 
        cale_fisier, 
        ','
    );

    -- 4) BIN
    cale_fisier := 'C:\Date excel si CSV\export_cytoscape_bin_' || to_char(current_date, 'YYYY_MM_DD') || '.csv';
    EXECUTE format(
        'COPY "3Ireland".cytoscape_network_data_bin TO %L WITH (FORMAT CSV, HEADER true, DELIMITER %L)', 
        cale_fisier, 
        ','
    );

    -- 5) TIME
    cale_fisier := 'C:\Date excel si CSV\export_cytoscape_time_' || to_char(current_date, 'YYYY_MM_DD') || '.csv';
    EXECUTE format(
        'COPY "3Ireland".cytoscape_network_data_time TO %L WITH (FORMAT CSV, HEADER true, DELIMITER %L)', 
        cale_fisier, 
        ','
    );
END $$;





