select
    u.country,
    round(avg(tx.tx_count), 2)       as avg_tx_count,
    round(avg(tx.avg_amount), 2)     as avg_tx_volume,
    round(sum(tx.total_amount), 2)   as total_volume
from (
    select
        user_id,
        count(*)              as tx_count,
        avg(amount)           as avg_amount,
        sum(amount)           as total_amount
    from iceberg.demo.transactions
    group by user_id
) tx
join iceberg.demo.users u on u.user_id = tx.user_id
group by u.country
order by total_volume desc