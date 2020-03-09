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

create materialized view portfolio_graph as
select
    *,
    current_value_orig + income_orig - outcome_orig as total_orig,
    case when outcome_orig > 0 then (current_value_orig + income_orig - outcome_orig) / outcome_orig else 0 end as total_p_orig,
    current_value + income - outcome as total,
    case when outcome > 0 then (current_value + income - outcome) / outcome else 0 end as total_p
from
    (
        select
            *,
            quantity * current_price_orig as current_value_orig,
            spent_orig + fees_orig as outcome_orig,
            coalesce(got_orig, 0) + coalesce(divs_orig, 0) as income_orig,
            quantity * current_price as current_value,
            spent + fees as outcome,
            coalesce(got, 0) + coalesce(divs, 0) as income
        from
            (
                select
                    date,
                    ticker,
                    quantity,
                    spent as spent_orig,
                    fees as fees_orig,
                    got as got_orig,
                    divs as divs_orig,
                    current_price as current_price_orig,

                    case when spent is not null and currency = 'USD' then
                        spent * (select c from portfolio_prices where ticker = 'USD000UTSTOM' and date_trunc('day', time) <= x.date order by time desc limit 1)
                    else spent end as spent,

                    case when fees is not null and currency = 'USD' then
                        fees * (select c from portfolio_prices where ticker = 'USD000UTSTOM' and date_trunc('day', time) <= x.date order by time desc limit 1)
                    else fees end as fees,

                    case when got is not null and currency = 'USD' then
                        got * (select c from portfolio_prices where ticker = 'USD000UTSTOM' and date_trunc('day', time) <= x.date order by time desc limit 1)
                    else got end as got,

                    case when divs is not null and currency = 'USD' then
                        divs * (select c from portfolio_prices where ticker = 'USD000UTSTOM' and date_trunc('day', time) <= x.date order by time desc limit 1)
                    else divs end as divs,

                    case when current_price is not null and currency = 'USD' then
                        current_price * (select c from portfolio_prices where ticker = 'USD000UTSTOM' and date_trunc('day', time) <= x.date order by time desc limit 1)
                    else current_price end as current_price
                from
                    (
                        select
                            dates.date,
                            tickers.ticker,
                            (
                                select sum(case
                                    when operation = 'BUY' and type in ('B', 'S') then quantity
                                    when operation = 'SELL' and type = 'S' then -quantity
                                end) from portfolio_operations where ticker = tickers.ticker and date <= dates.date
                            ) as quantity,
                            (
                                select sum(case
                                    when operation = 'BUY' and type = 'S' then price * quantity
                                    when operation = 'BUY' and type = 'B' then price / 100 * nominal * quantity
                                end) from portfolio_operations where ticker = tickers.ticker and date <= dates.date
                            ) as spent,
                            (
                                select sum(fee) from portfolio_operations where ticker = tickers.ticker and date <= dates.date
                            ) as fees,
                            (
                                select sum(case
                                    when operation = 'SELL' and type = 'S' then price * quantity
                                end) from portfolio_operations where ticker = tickers.ticker and date <= dates.date
                            ) as got,
                            (
                                select sum(case
                                    when operation = 'BUY' and type = 'D' then price
                                end) from portfolio_operations where asset = tickers.ticker and date <= dates.date
                            ) as divs,
                            (
                                select c from portfolio_prices where ticker = tickers.ticker and date_trunc('day', time) <= dates.date order by time desc limit 1
                            ) as current_price,
                            (select currency from portfolio_operations where ticker = tickers.ticker limit 1) as currency
                        from
                            generate_series(
                                (select min(date) from portfolio_operations),
                                current_date,
                                interval '1' day
                            ) as dates(date),
                            (select distinct ticker from portfolio_operations where ticker != 'MONEY') as tickers
                    ) x
                where
                    quantity is not null
                order by date desc
            ) y
    ) z;