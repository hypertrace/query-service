CREATE OR REPLACE FUNCTION dateTimeConvert (bigint, bigint) RETURNS bigint AS $$ select ((($1 + $2 - 1)/$2)*$2) $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION conditional (text, text, text)
 RETURNS text
  AS $$
   SELECT
    CASE WHEN $1 IS NOT NULL
     THEN CASE WHEN CAST ($1 AS BOOLEAN) THEN $2 ELSE $3 END
     ELSE NULL
    END
  $$
  LANGUAGE SQL;

CREATE OR REPLACE FUNCTION stringEquals (text, text)
 RETURNS text
  AS $$
   SELECT
    CASE WHEN $1 IS NULL AND $2 IS NULL
     THEN 'true'
     ELSE
      CASE WHEN $1 IS NULL OR $2 IS NULL
       THEN 'false'
       ELSE
        CASE WHEN $1 = $2
         THEN 'true'
         ELSE 'false'
        END
      END
    END
  $$
  LANGUAGE SQL;