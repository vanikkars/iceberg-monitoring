select
    country,
    count(*) as user_count
from iceberg.demo.users
group by country
order by user_count desc