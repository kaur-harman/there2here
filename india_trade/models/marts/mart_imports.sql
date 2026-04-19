with usd as (
    select * from {{ ref('stg_imports_usd') }}
    where is_total_row = false
),

qty as (
    select * from {{ ref('stg_imports_quantity') }}
    where is_total_row = false
),

joined as (
    select
        usd.country_name,
        date(usd.year, usd.month, 1)        as report_date,
        usd.hs_code,
        usd.commodity,
        qty.unit,

        round(usd.month_curr_year_usd, 2)   as month_curr_year_usd,
        round(usd.month_prev_year_usd, 2)   as month_prev_year_usd,
        round(usd.month_growth_pct, 2)      as month_growth_pct,

        round(qty.month_curr_year_qty, 2)   as month_curr_year_qty,
        round(qty.month_prev_year_qty, 2)   as month_prev_year_qty,
        round(qty.month_growth_pct_qty, 2)  as month_growth_pct_qty,

        round(usd.ytd_curr_year_usd, 2)     as ytd_curr_year_usd,
        round(usd.ytd_prev_year_usd, 2)     as ytd_prev_year_usd,
        round(usd.ytd_growth_pct, 2)        as ytd_growth_pct,

        round(qty.ytd_curr_year_qty, 2)     as ytd_curr_year_qty,
        round(qty.ytd_prev_year_qty, 2)     as ytd_prev_year_qty,
        round(qty.ytd_growth_pct_qty, 2)    as ytd_growth_pct_qty

    from usd
    left join qty
        on  usd.country_name = qty.country_name
        and usd.year         = qty.year
        and usd.month        = qty.month
        and usd.hs_code      = qty.hs_code
        and usd.commodity    = qty.commodity
),

-- commodity totals per month for proper ranking
commodity_totals as (
    select
        report_date,
        hs_code,
        sum(month_curr_year_usd) as commodity_total_usd
    from joined
    group by report_date, hs_code
),

final as (
    select
        j.*,

        -- total imports per month
        round(sum(j.month_curr_year_usd) over (
            partition by j.report_date
        ), 2)                                                       as total_monthly_imports_usd,

        -- country share of total imports that month (fix: use total already computed)
        round(100 * j.month_curr_year_usd / nullif(
            sum(j.month_curr_year_usd) over (partition by j.report_date)
        , 0), 4)                                                    as import_share_pct,

        -- rank country within each commodity per month
        rank() over (
            partition by j.report_date, j.hs_code
            order by j.month_curr_year_usd desc
        )                                                           as country_rank_by_commodity,

        -- rank commodities by total value per month (fix: use commodity totals)
        dense_rank() over (
            partition by j.report_date
            order by ct.commodity_total_usd desc
        )                                                           as commodity_rank,

        -- price proxy (renamed)
        round(
            j.month_curr_year_usd * 1000000 / nullif(j.month_curr_year_qty, 0)
        , 6)                                                        as price_proxy_usd_per_unit,

        -- monthly growth category (tightened thresholds)
        case
            when j.month_growth_pct >= 25   then 'high_growth'
            when j.month_growth_pct >= 5    then 'moderate_growth'
            when j.month_growth_pct > -5    then 'stable'
            else                                 'decline'
        end                                                         as growth_category,

        -- YTD growth category
        case
            when j.ytd_growth_pct >= 25     then 'high_growth'
            when j.ytd_growth_pct >= 5      then 'moderate_growth'
            when j.ytd_growth_pct > -5      then 'stable'
            else                                 'decline'
        end                                                         as ytd_growth_category

    from joined j
    left join commodity_totals ct
        on  j.report_date = ct.report_date
        and j.hs_code     = ct.hs_code

    where j.month_curr_year_usd is not null
       or j.month_curr_year_qty is not null
    and j.hs_code is not null
)

select * from final