SELECT
  date_format(CAST(invoice_date AS timestamp), '%Y-%m') AS month,
  SUM(line_amount) AS total_revenue
FROM leo_gold.fact_sales
GROUP BY 1
ORDER BY 1;