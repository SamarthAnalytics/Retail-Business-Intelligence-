select
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.city,
  c.state,
  c.signup_date,
  datediff('day', c.signup_date, current_date) as customer_age_days
from customers c;
