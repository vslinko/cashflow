drop function if exists convert_price(text, text, date, numeric);
drop function if exists convert_price(text, text, timestamp with time zone, numeric);
drop view if exists currency_exchange_rates;
drop view if exists portfolio_currencies;
drop view if exists portfolio_current_performance;
drop materialized view if exists portfolio_performance;
drop view if exists portfolio_bonds_ndk;
drop table if exists portfolio_bonds_payments;
drop table if exists portfolio_prices;
drop table if exists portfolio_operations;

create table portfolio_operations (
    id int not null primary key,
    transaction_code text,
    asset text,
    assetname text,
    date date,
    price numeric,
    ticker text,
    quantity numeric,
    fee numeric,
    nkd numeric,
    nominal numeric,
    note text,
    currency text,
    type text,
    operation text
);

create table portfolio_prices (
    ticker text,
    o numeric,
    c numeric,
    h numeric,
    l numeric,
    v int,
    time timestamp with time zone,
    interval text
);

create table portfolio_bonds_payments (
    bond text,
    payment_date date,
    coupon numeric,
    payment numeric,
    percent numeric,
    nominal numeric
);

create view portfolio_bonds_ndk as
select
    next.bond,
    dates.date,
    case when next.payment_date - prev.payment_date > 0 then
        next.nominal * (next.percent / 100) * (cast(dates.date as date) - prev.payment_date) / (next.payment_date - prev.payment_date)
    else 0
    end as nkd
from
    (
        select
            ticker as bond
        from
            portfolio_operations
        where
            ticker in (select distinct bond from portfolio_bonds_payments)
        group by
            ticker
    ) as bonds
    full outer join generate_series(
        (select min(payment_date) from portfolio_bonds_payments),
        cast(current_date as date),
        interval '1' day
    ) as dates(date) on true
    inner join lateral (
        select
            *
        from
            portfolio_bonds_payments
        where
            bond = bonds.bond
            and payment_date >= cast(dates.date as date)
            and nominal is not null
            and percent is not null
        order by
            payment_date
        limit 1
    ) as next on true
    inner join lateral (
        select
            *
        from
            portfolio_bonds_payments
        where
            bond = bonds.bond
            and payment_date <= dates.date
        order by
            payment_date desc
        limit 1
    ) as prev on true;

create materialized view portfolio_performance as
select
    dates.date,
    tickers.ticker,
    tickers.original_currency,
    tickers.currency,
    tickers.ticker || ' ' || tickers.currency as ticker_full,
    agg.quantity,
    agg.spent,
    agg.fees,
    agg.got,
    agg.divs,
    current_price.current_price,
    calc1.current_value,
    calc1.outcome,
    calc1.income,
    calc2.total,
    agg.quantity > 0 or operations_count > 0 as visible
from
    (
        select
            cast(date as date) date
        from
            generate_series(
                (select min(date) from portfolio_operations),
                current_date,
                interval '1' day
            ) date
    ) as dates(date)
    full outer join (
        (
            select
                ticker,
                min(currency) as original_currency,
                min(currency) as currency
            from
                portfolio_operations
            where
                ticker != 'MONEY'
            group by
                ticker
        ) union (
            select
                ticker,
                min(currency) as original_currency,
                'RUB' as currency
            from
                portfolio_operations
            where
                ticker != 'MONEY'
                and currency != 'RUB'
            group by
                ticker
        ) union (
            select
                ticker,
                min(currency) as original_currency,
                'USD' as currency
            from
                portfolio_operations
            where
                ticker != 'MONEY'
                and currency != 'USD'
            group by
                ticker
        )
    ) as tickers on true
    left join lateral (
        select
            sum(case
                when operation = 'BUY' and type in ('B', 'S') then quantity
                when operation = 'SELL' and type = 'S' then -quantity
            end) as quantity,
            sum(case
                when operation = 'BUY' and type = 'S' then
                    convert_price(tickers.original_currency, tickers.currency, date, price) * quantity
                when operation = 'BUY' and type = 'B' then
                    convert_price(tickers.original_currency, tickers.currency, date, price) / 100 * nominal * quantity
            end) as spent,
            sum(convert_price(tickers.original_currency, tickers.currency, date, fee)) as fees,
            sum(case
                when operation = 'SELL' and type = 'S' then
                    convert_price(tickers.original_currency, tickers.currency, date, price) * quantity
            end) as got,
            sum(case
                when operation = 'BUY' and type = 'D' then
                    convert_price(tickers.original_currency, tickers.currency, date, price)
            end) as divs,
            sum(case when date = dates.date then 1 else 0 end) as operations_count
        from
             portfolio_operations
        where
             (ticker = tickers.ticker or asset = tickers.ticker)
             and date <= dates.date
    ) as agg on true
    left join lateral (
        select
            convert_price(tickers.original_currency, tickers.currency, dates.date, nkd) as nkd
        from
            portfolio_bonds_ndk
        where
            bond = tickers.ticker
            and date = dates.date
    ) as nkd on true
    left join lateral (
        select
            convert_price(tickers.original_currency, tickers.currency, dates.date, c) as current_price
        from
            portfolio_prices
        where
            ticker = tickers.ticker
            and date_trunc('day', time) <= dates.date
        order by
            time desc
        limit 1
    ) as current_price on true
    left join lateral (
        select
            agg.quantity * (current_price.current_price + coalesce(nkd.nkd, 0)) as current_value,
            agg.spent + agg.fees as outcome,
            coalesce(agg.got, 0) + coalesce(agg.divs, 0) as income
    ) as calc1 on true
    left join lateral (
        select
            calc1.current_value + calc1.income - calc1.outcome as total
    ) as calc2 on true
order by
    date desc,
    ticker;

create view portfolio_current_performance as
select
    ticker,
    original_currency,
    currency,
    quantity,
    current_value,
    total,
    total_p,
    share,
    current_value - value_1_day as diff_1_day,
    case when value_1_day > 0 then current_value / value_1_day - 1 end as diff_1_day_p,
    current_value - value_1_week as diff_1_week,
    case when value_1_week > 0 then current_value / value_1_week - 1 end as diff_1_week_p,
    current_value - value_1_month as diff_1_month,
    case when value_1_month > 0 then current_value / value_1_month - 1 end as diff_1_month_p
from
    (
        select
            ticker,
            original_currency,
            currency,
            quantity,
            current_value,
            total,
            case when outcome > 0 then total / outcome else 0 end as total_p,
            case when currency in ('RUB', 'USD') then
                current_value / (select sum(current_value) from portfolio_performance where date = (select max(date) from portfolio_performance) and currency = o.currency)
            end as share,
            (select current_value from portfolio_performance where ticker_full = o.ticker_full and date = o.date - interval '1' day) value_1_day,
            (select current_value from portfolio_performance where ticker_full = o.ticker_full and date = o.date - interval '7' day) value_1_week,
            (select current_value from portfolio_performance where ticker_full = o.ticker_full and date = o.date - interval '1' month) value_1_month
        from
            portfolio_performance o
        where
            date = (select max(date) from portfolio_performance)
            and visible
    ) x
order by
    share desc;

create view portfolio_currencies as
select
    currency,
    sum(case
        when operation = 'BUY' and type in ('M', 'D') then price * quantity - fee
        when operation = 'SELL' and type in ('S') then price * quantity - fee

        when operation = 'BUY' and type in ('S') then price * quantity * -1 - fee
        when operation = 'BUY' and type in ('B') then price * nominal * quantity / 100 * -1 - fee
        when operation = 'SELL' and type in ('F', 'TAX', 'M') then price * quantity * -1 - fee
    end) as value
from
    portfolio_operations
group by
    currency;

create view currency_exchange_rates as
select
    time,
    'USD/RUB' as rate,
    c as value
from
    portfolio_prices
where
    ticker = 'USD000UTSTOM';

create function convert_price(f text, t text, w timestamp with time zone, v numeric) returns numeric as $$
declare
    direct_rate text;
    indirect_rate text;
    r numeric;
begin
    if f = t then
        return v;
    end if;

    direct_rate := f || '/' || t;
    indirect_rate := t || '/' || f;

    select
        case when rate = direct_rate then value else 1 / value end into r
    from
        currency_exchange_rates
    where
        rate in (direct_rate, indirect_rate)
        and time <= w
    order by
        time desc
    limit 1;

    return v * r;
end;
$$ language plpgsql;

create function convert_price(f text, t text, w date, v numeric) returns numeric as $$
begin
    return convert_price(f, t, cast(w as timestamptz) + interval '23:59:59', v);
end;
$$ language plpgsql;

create view portfolio_performance_week_agg as
with data as (
select
    date,
    cast(date_trunc('week', date) as date) as wdate,
    ticker,
    original_currency,
    currency,
    ticker_full,
    last_value(quantity) over (partition by ticker_full, date_trunc('week', date) order by date nulls first) as quantity,
    last_value(spent) over (partition by ticker_full, date_trunc('week', date) order by date nulls first) as spent,
    last_value(fees) over (partition by ticker_full, date_trunc('week', date) order by date nulls first) as fees,
    last_value(got) over (partition by ticker_full, date_trunc('week', date) order by date nulls first) as got,
    last_value(divs) over (partition by ticker_full, date_trunc('week', date) order by date nulls first) as divs,
    last_value(current_price) over (partition by ticker_full, date_trunc('week', date) order by date nulls first) as current_price,
    last_value(current_value) over (partition by ticker_full, date_trunc('week', date) order by date nulls first) as current_value,
    last_value(outcome) over (partition by ticker_full, date_trunc('week', date) order by date nulls first) as outcome,
    last_value(income) over (partition by ticker_full, date_trunc('week', date) order by date nulls first) as income,
    last_value(total) over (partition by ticker_full, date_trunc('week', date) order by date nulls first) as total,
    bool_or(visible) over (partition by ticker_full, date_trunc('week', date)) as visible,
    date = last_value(date) over (partition by ticker_full, date_trunc('week', date)) as is_last
from
    portfolio_performance
order by
    date desc,
    ticker_full
)
select
    date,
    wdate,
    ticker,
    original_currency,
    currency,
    ticker_full,
    quantity,
    spent,
    fees,
    got,
    divs,
    current_price,
    current_value,
    outcome,
    income,
    total
from
    data
where
    is_last and visible;