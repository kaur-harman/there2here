with source as (
    select * from {{ source('india_trade_warehouse', 'imports_usd') }}
),

cleaned as (
    select
        country_name,
        year,
        month,
        report_date,
        hs_code,
        commodity,
        -- filter out total rows
        case when sno is null then true else false end as is_total_row,
        cast(month_prev_year as float64)  as month_prev_year_usd,
        cast(month_curr_year as float64)  as month_curr_year_usd,
        cast(month_growth_pct as float64) as month_growth_pct,
        cast(ytd_prev_year as float64)    as ytd_prev_year_usd,
        cast(ytd_curr_year as float64)    as ytd_curr_year_usd,
        cast(ytd_growth_pct as float64)   as ytd_growth_pct
    from source
    where country_name is not null
        and commodity is not null
)

select * from cleaned