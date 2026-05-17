select
    user_id,
    count(*)              as tx_count,
    round(sum(amount), 2) as total_amount
from iceberg.demo.transactions
group by user_id
order by tx_count desc
limit 10