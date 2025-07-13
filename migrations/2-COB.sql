 CREATE OR REPLACE VIEW COB AS
/*------------------------------------------------------------------
  0.  Clean cash snapshots
  ----------------------------------------------------------------*/
WITH h_cash_raw AS (
    SELECT
        h.aacct AS account,
        date_trunc('month',
                   strptime(replace(h.adate::varchar,'/',''),'%Y%m')::date)
                   AS month_date,
        strptime(replace(h.adate::varchar,'/',''),'%Y%m') AS snap_dt,
        COALESCE(
            TRY_CAST(
                REPLACE(
                    NULLIF(CAST(h.hprincipal AS VARCHAR),'.'),',',''
                ) AS DOUBLE), 0
        ) AS principal
    FROM frphold h
    JOIN frpsi1 s ON h.hdirect1 = s.sori
    WHERE s.csh = 'C'
),
/*------------------------------------------------------------------
  1.  First-of-month (beginning) cash
  ----------------------------------------------------------------*/
t_begin_cash AS (
    SELECT
        account,
        month_date,
        arg_min(principal, snap_dt) AS beginning_cash
    FROM h_cash_raw
    GROUP BY account, month_date
),
/*------------------------------------------------------------------
  2.  Periods: start = month_date, end-cash = next month’s beginning
  ----------------------------------------------------------------*/
t_periods AS (
    SELECT
        account,
        month_date                              AS period_start,
        LEAD(beginning_cash)
          OVER (PARTITION BY account
                ORDER BY month_date)            AS ending_cash
    FROM t_begin_cash
),
/*------------------------------------------------------------------
  3.  Net cash-flows within each period
  ----------------------------------------------------------------*/
t_cash_flows AS (
    SELECT
        t.aacct AS account,
        date_trunc('month',
                   strptime(replace(t.tdate::varchar,'/',''),
                             '%Y%m%d')::date)   AS month_date,
        SUM(
            CASE
                WHEN t.tcode IN ('SELL','CO','DIV','INT','BUYR') THEN
                     COALESCE(
                         TRY_CAST(
                             REPLACE(
                                 NULLIF(CAST(t.tprincipal AS VARCHAR),'.'),
                                 ',', ''
                             ) AS DOUBLE), 0)
                WHEN t.tcode IN ('BUY','DI','FEE','SELLR') THEN
                    -COALESCE(
                         TRY_CAST(
                             REPLACE(
                                 NULLIF(CAST(t.tprincipal AS VARCHAR),'.'),
                                 ',', ''
                             ) AS DOUBLE), 0)
                ELSE 0
            END
        ) AS net_cash_flow
    FROM frptran t
    GROUP BY account, month_date
),
/*------------------------------------------------------------------
  4.  Reconcile
  ----------------------------------------------------------------*/
t_recon AS (
    SELECT
        p.account,
        p.period_start,
        bc.beginning_cash,
        COALESCE(cf.net_cash_flow,0)        AS net_cash_flow,
        p.ending_cash                       AS actual_ending_cash
    FROM t_periods      p
    JOIN t_begin_cash   bc
      ON bc.account = p.account
     AND bc.month_date = p.period_start
    LEFT JOIN t_cash_flows cf
      ON cf.account = p.account
     AND cf.month_date = date_trunc('month', period_start + INTERVAL '1 month')
)
/*------------------------------------------------------------------
  5.  Final result – only out-of-balance months
  ----------------------------------------------------------------*/
SELECT
    account,
    period_start,
    date_trunc('month', period_start + INTERVAL '1 month') AS period_end,
    beginning_cash,
    net_cash_flow,
    COALESCE(actual_ending_cash,0)           AS actual_ending_cash,
    beginning_cash + net_cash_flow           AS calculated_ending_cash,
    ROUND(COALESCE(actual_ending_cash,0)
      - (beginning_cash + net_cash_flow),2)     AS out_of_balance_amount
FROM t_recon
WHERE ABS( COALESCE(actual_ending_cash,0)
           - (beginning_cash + net_cash_flow) ) > 1

