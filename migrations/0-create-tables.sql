CREATE OR REPLACE TABLE sales AS
  SELECT * FROM read_csv_auto('data/sales.csv');

CREATE OR REPLACE TABLE INT_FRPAIR_RAW AS
  SELECT * FROM read_csv_auto('data/frpair.csv');

CREATE OR REPLACE TABLE INT_FRPSEC_RAW AS
  SELECT * FROM read_csv_auto('data/frpsec.csv');

CREATE OR REPLACE TABLE INT_FRPHOLD_RAW AS
  SELECT * FROM read_csv('data/frphold.csv', HEADER=TRUE, AUTO_DETECT=TRUE);

CREATE OR REPLACE TABLE INT_FRPTRAN_RAW AS
  SELECT * FROM read_csv_auto('data/frptran.csv');

CREATE OR REPLACE TABLE INT_FRPTCD_RAW AS
  SELECT * FROM read_csv_auto('data/frptcd.csv');

CREATE OR REPLACE TABLE INT_FRPSI1_RAW AS
  SELECT * FROM read_csv_auto('data/frpsi1.csv');

CREATE OR REPLACE TABLE INT_FRPINDX_RAW AS
  SELECT * FROM read_csv_auto('data/frpindx.csv');

CREATE OR REPLACE TABLE INT_FRPPRICE_RAW AS
  SELECT * FROM read_csv_auto('data/frpprice.csv');

CREATE OR REPLACE TABLE INT_FRPCTG_RAW AS
  SELECT * FROM read_csv_auto('data/frpctg.csv');

CREATE OR REPLACE TABLE INT_FRPAGG_RAW AS
  SELECT * FROM read_csv_auto('data/frpagg.csv');
