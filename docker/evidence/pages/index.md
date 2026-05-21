---
title: Bank BI Dashboard
---



This dashboard presents near real-time banking metrics powered by a streaming data pipeline that ingests events from Kafka into Apache Iceberg via Apache Flink. Data is continuously processed and made available with minimal latency, enabling up-to-date visibility into transaction volumes, user activity, and financial trends. Use the dashboards below to explore key performance indicators across the platform.

---

<div style="text-align: center;">

## Key Metrics

<BigValue
  data={kpis}
  value=total_users_cnt
  title="Total Users Count"
/>

<BigValue
  data={kpis}
  value=total_transactions_cnt
  title="Total Transactions Count"
/>

<BigValue
  data={kpis}
  value=total_volume
  title="Transaction Volume ($)"
  fmt=usd2
/>


---


[Transactions Dashboard](/transactions) | [Users Dashboard](/users) | [Iceberg Health](/iceberg-health)

</div>

---
<details>
<summary>KPIs sql defintion </summary>
Thease queries are used as source for the charts above. Thus do not delete them.
The complexity of queries is hidden by the sql files in source. The queries below mostly reference the output fo these quries.

```sql kpis
select * from demo_lh.kpis
```

</details>
