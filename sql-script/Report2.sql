        select
        	market_segment,
        	customer_name,
        	total_orders,
        	customer_rank
        from
        	(
        		select
        			market_segment,
        			customer_name,
        			count( distinct order_key ) as total_orders,
        			rank() over( partition by market_segment order by count( distinct order_key ) desc ) as customer_rank
        		from
        			supply_chain_management.fact_line_item_order fact
        		join dim_customer cust on
        			fact.customer_key = cust.customer_key
        		group by
        			market_segment,
        			customer_name
        	) customer_rank_tab
        where
        	customer_rank <= 5;