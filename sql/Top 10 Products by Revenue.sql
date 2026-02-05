SELECT
  product_description,
  SUM(line_amount) AS product_revenue
FROM leo_gold.fact_sales
GROUP BY product_description
ORDER BY product_revenue DESC
LIMIT 10;
