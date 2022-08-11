CREATE OR REPLACE FUNCTION dateTimeConvert (bigint, bigint) RETURNS bigint AS $$ select ((($1 + $2 - 1)/$2)*$2) $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION conditional (condition text, first text, second text)
 RETURNS text
  AS $$
   SELECT
    CASE WHEN condition IS NOT NULL
     THEN CASE WHEN CAST (condition AS BOOLEAN) THEN first ELSE second END
     ELSE NULL
    END
  $$
  LANGUAGE SQL;

CREATE OR REPLACE FUNCTION stringEquals (str1 text, str2 text)
 RETURNS text
  AS $$
   SELECT
    CASE WHEN str1 IS NULL AND str2 IS NULL
     THEN 'true'
     ELSE
      CASE WHEN str1 IS NULL OR str2 IS NULL
       THEN 'false'
       ELSE
        CASE WHEN str1 = str2
         THEN 'true'
         ELSE 'false'
        END
      END
    END
  $$
  LANGUAGE SQL;