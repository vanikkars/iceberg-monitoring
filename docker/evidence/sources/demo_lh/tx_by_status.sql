select
    status,
    count(*)              as tx_count,
    round(sum(amount), 2) as total_amount
from iceberg.demo.transactions
group by status
order by tx_count desc