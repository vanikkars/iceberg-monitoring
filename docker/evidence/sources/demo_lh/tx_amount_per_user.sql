select
    user_id,
    count(*)              as tx_count,
    round(sum(amount), 2) as total_amount
from iceberg.demo.transactions
group by user_id
order by total_amount desc
limit 20