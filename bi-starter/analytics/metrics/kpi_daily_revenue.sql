select
  order_date,
  sum(line_revenue) as revenue
from fact_order_items
group by 1
order by 1;
