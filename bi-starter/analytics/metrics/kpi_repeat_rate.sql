with orders_per_cust as (
  select customer_id, count(*) as orders_cnt
  from orders
  group by 1
)
select
  (sum(case when orders_cnt >= 2 then 1 else 0 end)::double)
  / nullif(sum(case when orders_cnt >= 1 then 1 else 0 end),0) as repeat_rate
from orders_per_cust;
