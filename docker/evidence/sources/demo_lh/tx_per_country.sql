select
    u.country,
    round(sum(t.amount), 2) as total_amount,
    count(*)                as tx_count
from iceberg.demo.transactions t
inner join iceberg.demo.users u
on u.user_id = t.user_id
group by country
order by country