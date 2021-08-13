SELECT DISTINCT  client_surr_id
                ,driver_surr_id
                ,avg_revenue
                ,FIRST_VALUE(avg_trip_duration) OVER 
                (PARTITION BY client_surr_id
                             ,driver_surr_id
                             ,avg_revenue
                 ORDER BY client_surr_id DESC
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                           AS "trip_duration"
FROM dw_level.fct_orders_mm
ORDER BY client_surr_id
         ,driver_surr_id
         ,avg_revenue;
         
SELECT * FROM (
    SELECT  client_surr_id
           ,driver_surr_id
           ,RANK() 
                OVER (PARTITION BY avg_revenue 
                    ORDER BY client_surr_id) revenue_rank
     FROM dw_level.fct_orders_mm
     WHERE client_surr_id BETWEEN 1 AND 3000
)   
WHERE revenue_rank<=5
ORDER BY client_surr_id;
 
SELECT * FROM (
    SELECT  client_surr_id
           ,driver_surr_id
           ,DENSE_RANK() OVER (PARTITION BY avg_revenue ORDER BY client_surr_id) revenue_rank
     FROM dw_level.fct_orders_mm
     WHERE client_surr_id BETWEEN 1 AND 3000
)   
WHERE revenue_rank<=5
ORDER BY client_surr_id;
 
SELECT DISTINCT client_surr_id
      ,trip_id
      ,MAX(avg_revenue)
  OVER (PARTITION BY avg_trip_duration ORDER BY client_surr_id DESC
       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
       AS "highest avg_rev per client"
      ,MIN(avg_revenue)
  OVER (PARTITION BY avg_trip_duration ORDER BY client_surr_id DESC
       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
       AS "lowest avg_rev per client"

FROM dw_level.fct_orders_mm;