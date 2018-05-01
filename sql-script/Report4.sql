    SELECT
        TOP 10 part_name,
        sum(supply_cost) as total_supply_cost,
        count(quantity) as no_of_sales,
        avg (ship_date - order_date) as averge_shipping_time_in_days,
        avg (receipt_date- ship_date) as averge_delivery_time_in_days,
        -- Negative dates are ideal for the below column
        avg (receipt_date- commit_date) as averge_delay_time_in_days
     
    FROM
        supply_chain_management.fact_line_item_order fact
    join
        supply_chain_management.dim_part part
            on fact.part_key = part.part_key
    where  fact.supplier_key = 7045232
    group by
        part_name
    ORDER BY
        count(quantity) desc
        
        
        