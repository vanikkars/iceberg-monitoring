select
    (select count(*)              from iceberg.demo.users)         as total_users_cnt,
    (select count(*)              from iceberg.demo.transactions)  as total_transactions_cnt,
    (select round(sum(amount), 2) from iceberg.demo.transactions)  as total_volume