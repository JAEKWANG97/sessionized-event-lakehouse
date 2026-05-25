SELECT
  date_sub(to_date(dt), pmod(datediff(to_date(dt), '1970-01-05'), 7)) AS week_start,
  count(DISTINCT user_id) AS wau_users
FROM ${database}.${table}
GROUP BY date_sub(to_date(dt), pmod(datediff(to_date(dt), '1970-01-05'), 7))
ORDER BY week_start;
