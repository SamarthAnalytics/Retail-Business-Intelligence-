select
  oi.order_item_id,
  oi.order_id,
  o.order_date,
  o.customer_id,
  oi.product_id,
  oi.quantity,
  p.unit_price,
  p.unit_cost,
  (oi.quantity * p.unit_price) as line_revenue,
  (oi.quantity * (p.unit_price - p.unit_cost)) as line_gross_margin
from order_items oi
join orders o using(order_id)
join products p using(product_id);
