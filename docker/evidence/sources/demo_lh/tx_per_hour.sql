select
    date_format(date_trunc('hour', event_time), '%Y-%m-%d %H:00') as hour,
    count(*)                                                       as tx_count
from iceberg.demo.transactions
group by 1
order by 1