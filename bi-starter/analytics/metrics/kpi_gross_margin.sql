select
  sum(line_gross_margin) as gross_margin,
  sum(line_revenue) as revenue,
  case when sum(line_revenue) = 0 then null
       else sum(line_gross_margin) / sum(line_revenue)
  end as margin_pct
from fact_order_items;
