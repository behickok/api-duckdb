CREATE OR REPLACE TABLE kb_docs AS
  SELECT * FROM read_csv_auto('data/kb_docs.csv');
