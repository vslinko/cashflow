drop table if exists utkonos_mapping;
drop table if exists utkonos_orders;

create table utkonos_orders (
  order_id text,
  date date,
  name text,
  id bigint,
  article bigint,
  unit text,
  amount numeric,
  price numeric,
  total numeric
);

create table utkonos_mapping (
  name text not null primary key,
  custom_category text
);
