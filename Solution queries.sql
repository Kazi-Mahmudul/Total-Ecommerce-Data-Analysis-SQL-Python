create database mydb;
use mydb;
DROP DATABASE mydb;
USE totaldb;
SELECT COUNT(order_id)
FROM orders
WHERE YEAR(order_purchase_timestamp) = 2017;

SELECT products.product_category category,
ROUND(SUM(payments.payment_value),2) Total_sales
FROM products 
JOIN order_items on products.product_id = order_items.product_id
JOIN payments ON order_items.order_id = payments.order_id
GROUP BY category;

SELECT (SUM(CASE WHEN payment_installments >= 1 then 1 ELSE 0 END))/COUNT(*)*100
FROM payments;

SELECT customer_state state, COUNT(customer_id) customers
FROM customers
GROUP BY customer_state;

SELECT MONTHNAME(order_purchase_timestamp) Months, COUNT(order_id) Orders
FROM orders
WHERE YEAR(order_purchase_timestamp) = 2018
GROUP BY Months;

WITH count_per_table as (SELECT orders.order_id, orders.customer_id, COUNT(order_items.order_id) as oc
FROM orders
JOIN order_items ON orders.order_id = order_items.order_id
GROUP BY orders.order_id, orders.customer_id)
SELECT customers.customer_city, ROUND(AVG(count_per_table.oc),2) avg_orders
FROM customers 
JOIN count_per_table ON customers.customer_id = count_per_table.customer_id
GROUP BY customers.customer_city;

SELECT products.product_category  category, 
ROUND((SUM(payments.payment_value)/(SELECT SUM(payment_value) FROM payments))*100, 2) revenue
FROM payments
JOIN order_items ON payments.order_id = order_items.order_id
JOIN products ON order_items.product_id = products.product_id
GROUP BY category; 

SELECT products.product_category, 
COUNT(order_items.product_id), ROUND(AVG(order_items.price), 2)
FROM products
JOIN order_items ON products.product_id = order_items.product_id
GROUP BY products.product_category;

SELECT *, DENSE_RANK() OVER(ORDER BY Revenue DESC)
FROM (SELECT order_items.seller_id Seller, ROUND(SUM(payments.payment_value), 2) Revenue
FROM order_items
JOIN payments ON order_items.order_id = payments.order_id
GROUP BY Seller) AS SubQuery;

SELECT customer_id, order_purchase_timestamp, payment_value, AVG(payment_value) 
OVER(
PARTITION BY customer_id
ORDER BY order_purchase_timestamp
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
) AS moving_avg
FROM
(SELECT orders.customer_id, orders.order_purchase_timestamp, payments.payment_value
FROM orders 
JOIN payments ON orders.order_id = payments.order_id) AS Sub;

SELECT Years, Months, Payments, ROUND(SUM(Payments)
OVER(ORDER BY Years, Months), 2) AS Cumulative_sales
FROM
(SELECT YEAR(orders.order_purchase_timestamp) AS Years,
MONTH(orders.order_purchase_timestamp) AS Months,
ROUND(SUM(payments.payment_value), 2) AS Payments 
FROM orders
JOIN payments ON orders.order_id = payments.order_id
GROUP BY Years, Months) AS sub;

SELECT Years, Payments, 
ROUND(((Payments - (LAG(Payments, 1) OVER( ORDER BY Years)))/(LAG(Payments, 1) OVER( ORDER BY Years)))*100, 2) AS Growth FROM
(SELECT YEAR(orders.order_purchase_timestamp) AS Years,
ROUND(SUM(payments.payment_value), 2) AS Payments 
FROM orders
JOIN payments ON orders.order_id = payments.order_id
GROUP BY Years
ORDER BY Years) AS SUB;

WITH a AS (
    SELECT 
        c.customer_unique_id AS Customers,
        MIN(o.order_purchase_timestamp) AS first_order
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_unique_id
),
b AS (
    SELECT 
        a.Customers,
        COUNT(DISTINCT o.order_id) AS next_order
    FROM a
    -- Link to customers to get all customer_ids for the same unique customer
    LEFT JOIN customers c ON a.Customers = c.customer_unique_id
    -- Find orders placed AFTER the first order and within 6 months
    LEFT JOIN orders o ON c.customer_id = o.customer_id
        AND o.order_purchase_timestamp > a.first_order
        AND o.order_purchase_timestamp <= DATE_ADD(a.first_order, INTERVAL 6 MONTH)
    GROUP BY a.Customers
)
SELECT 
    ROUND(100 * (SUM(CASE WHEN b.next_order > 0 THEN 1 ELSE 0 END) / COUNT(DISTINCT a.Customers)), 2) AS Retention_rate
FROM a
LEFT JOIN b ON a.Customers = b.Customers;

SELECT Customers, year, paid, ranking
FROM
(SELECT orders.customer_id Customers, YEAR(orders.order_purchase_timestamp) year, ROUND(SUM(payments.payment_value), 2) paid,
DENSE_RANK() OVER(PARTITION BY YEAR(orders.order_purchase_timestamp) ORDER BY SUM(payments.payment_value) DESC) ranking
FROM orders
JOIN payments ON orders.order_id = payments.order_id
GROUP BY orders.customer_id, orders.order_purchase_timestamp) AS sub
WHERE ranking <= 3;

