select
    date_format(event_time, '%Y-%m-%d') as day,
    count(*)                            as tx_count,
    round(sum(amount), 2)               as total_amount
from iceberg.demo.transactions
group by 1
order by 1