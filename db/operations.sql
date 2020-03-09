create table operations (
  operation_time timestamp,
  payment_date date,
  card text,
  status text,
  operation_amount decimal,
  operation_currency text,
  payment_amount decimal,
  payment_currency text,
  cashback decimal,
  category text,
  mcc int,
  description text,
  bonuses decimal
);

create table operations_mapping (
    category text,
    description text,
    approved boolean,
    custom_category text,
    custom_category_group text,
    primary key (category, description)
);

create or replace view outcome as
select
    o.*,
    om.custom_category,
    om.custom_category_group
from
    operations o
    left join operations_mapping om on o.category = om.category and o.description = om.description
where
    o.payment_amount < 0
    and o.status != 'FAILED'
    and (o.card != '*6427' or o.card is null)
    and not (o.category = 'Переводы/иб' and o.description = 'Вячеслав Слинько')
    and not (o.category = 'Наличные' and o.operation_time between date '2018-04-18' and date '2018-04-20')
    and om.custom_category_group not in ('Перевод между счетами');

create or replace view income as
select
    o.*,
    om.custom_category,
    om.custom_category_group
from
    operations o
    left join operations_mapping om on o.category = om.category and o.description = om.description
where
    o.payment_amount >= 0
    and o.status != 'FAILED'
    and (o.card != '*6427' or o.card is null)
    and om.custom_category_group not in ('Перевод между счетами');
