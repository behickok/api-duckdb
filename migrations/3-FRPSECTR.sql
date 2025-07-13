/* ================================================================
   Modified Dietz monthly components  (DuckDB 0.10+)
   ================================================================ */
 CREATE OR REPLACE VIEW FRPSECTR AS

/*------------------------------------------------------------------
  1.  Cash-flows with timing weights
  ----------------------------------------------------------------*/
WITH t_flows AS (
    SELECT
        /* ───────────  dates  ─────────── */
        strptime(replace(t.tdate::varchar,'/',''), '%Y%m%d')::date      AS flow_dt,
        date_trunc('month',
                   strptime(replace(t.tdate::varchar,'/',''), '%Y%m%d')::date)
                                                                      AS month_date,
        t.aacct,
        t.hid,

        /* Wi = (CD − Di) / CD */
        (
          day(last_day(strptime(replace(t.tdate::varchar,'/',''),'%Y%m%d')::date))
          - day(        strptime(replace(t.tdate::varchar,'/',''),'%Y%m%d')::date)
        )::double
        /
        day(last_day(strptime(replace(t.tdate::varchar,'/',''),'%Y%m%d')::date))
                                                                      AS timing_weight,

        /* ───────────  clean numeric  ─────────── */
        COALESCE(
            try_cast(replace(NULLIF(t.tnet::varchar, '.'), ',', '') AS double),
            0
        )                                                            AS tnet_num,

        CASE WHEN t.tcode IN ('BUY','CO')        THEN tnet_num ELSE 0 END AS positive_flow,
        CASE WHEN t.tcode IN ('SELL','FEE','DI') THEN tnet_num ELSE 0 END AS negative_flow,
        CASE WHEN t.tcode IN ('DIV','INT')       THEN tnet_num ELSE 0 END AS income
    FROM frptran t
),

/*------------------------------------------------------------------
  2.  Monthly aggregates of cash-flows
  ----------------------------------------------------------------*/
t_flows_agg AS (
    SELECT
        month_date,
        aacct,
        hid,
        SUM(income)                                 AS income,
        SUM(positive_flow)                          AS positive_flows,
        SUM(negative_flow)                          AS negative_flows,
        SUM(positive_flow * timing_weight)          AS positive_factor,
        SUM(negative_flow * timing_weight)          AS negative_factor
    FROM t_flows
    GROUP BY month_date, aacct, hid
),

/*------------------------------------------------------------------
  3.  Beginning / ending market values & accruals
  ----------------------------------------------------------------*/
t_holdings AS (
    SELECT
        date_trunc('month',
                   strptime(replace(h.adate::varchar,'/',''), '%Y%m')::date)
                                                                      AS month_date,
        h.aacct,
        h.hid,
        h.hdirect1,

        arg_min(
            COALESCE(
                try_cast(replace(NULLIF(h.hprincipal, '.'), ',', '') AS double),
                0
            ),
            strptime(replace(h.adate::varchar,'/',''), '%Y%m')
        ) AS bmv,

        arg_max(
            COALESCE(
                try_cast(replace(NULLIF(h.hprincipal, '.'), ',', '') AS double),
                0
            ),
            strptime(replace(h.adate::varchar,'/',''), '%Y%m')
        ) AS emv,

        arg_min(
            COALESCE(
                try_cast(replace(NULLIF(h.haccrual, '.'), ',', '') AS double),
                0
            ),
            strptime(replace(h.adate::varchar,'/',''), '%Y%m')
        ) AS b_accrual,

        arg_max(
            COALESCE(
                try_cast(replace(NULLIF(h.haccrual, '.'), ',', '') AS double),
                0
            ),
            strptime(replace(h.adate::varchar,'/',''), '%Y%m')
        ) AS e_accrual
    FROM frphold h
    GROUP BY month_date, h.aacct, h.hid, h.hdirect1
),

/*------------------------------------------------------------------
  4.  Sector → parent-segment lookup
  ----------------------------------------------------------------*/
t_segment_hierarchy AS (
    SELECT DISTINCT
        p.sori        AS parent_id,
        p.soriname    AS parent_name,
        c.csector     AS child_id
    FROM frpctg  c
    JOIN frpsi1 p ON c.category = p.sori
    WHERE p.siflag = 'S'   -- keep only sectors
)

/*------------------------------------------------------------------
  5.  Assemble the final view
  ----------------------------------------------------------------*/
SELECT
    h.month_date                         AS "date",
    h.aacct                              AS "account",
    COALESCE(s.parent_name,'Unclassified')                        AS "segment",

    /* aggregated components */
    SUM(h.bmv)                AS beginning_market_value,
    SUM(h.emv)                AS ending_market_value,
    SUM(f.positive_flows)     AS positive_flows,
    SUM(f.negative_flows)     AS negative_flows,
    SUM(f.positive_factor)    AS positive_factor,
    SUM(f.negative_factor)    AS negative_factor,
    SUM(f.income)             AS income,
    SUM(h.b_accrual)          AS beginning_accruals,
    SUM(h.e_accrual)          AS ending_accruals

FROM t_holdings            h
JOIN t_segment_hierarchy   s ON lpad(h.hdirect1::varchar, 2, '0') = s.child_id
LEFT JOIN t_flows_agg      f ON h.aacct     = f.aacct
                             AND h.month_date = f.month_date
                             AND h.hid        = f.hid
GROUP BY h.month_date,
         h.aacct,
         COALESCE(s.parent_name,'Unclassified')
ORDER BY
    "date", "account", "segment";
