#!/usr/bin/env bash
# Live row counts for every table, refreshing every 2 seconds.
# Ctrl-C to stop.
exec watch -n 2 -t "docker exec nifi-warehouse psql -U etl -d etldemo -c \"
select 'customers'          as table_name, count(*) as rows, 'seeded once'  as filled_by from customers
union all select 'products',           count(*), 'seeded once'  from products
union all select 'orders',             count(*), 'NiFi'         from orders
union all select 'order_items',        count(*), 'NiFi'         from order_items
union all select 'alerts',             count(*), 'NiFi rules'   from alerts
union all select 'quarantine_records', count(*), 'NiFi rejects' from quarantine_records
union all select 'job_runs',           count(*), 'NiFi log'     from job_runs
union all select 'nifi_metrics',       count(*), 'monitor'      from nifi_metrics
union all select 'nifi_bulletins',     count(*), 'monitor'      from nifi_bulletins;\""
