SELECT
  country,
  SUM(line_amount) AS country_revenue
FROM leo_gold.fact_sales
GROUP BY country
ORDER BY country_revenue DESC;
