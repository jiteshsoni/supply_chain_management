    select
    	part_name,
    	order_priority,
    	total_parts_delayed::int,
    	avg_days_delayed,
    	product_rank
    from
    	(
    		select
    			part_key,
    			order_priority,
    			sum( quantity ) as total_parts_delayed,
    			avg( receipt_date - commit_date ) as avg_days_delayed,
    			rank() over( partition by order_priority order by sum( quantity ) desc ) as product_rank
    		from
    			supply_chain_management.fact_line_item_order
    		where
    			receipt_date > commit_date
    			and supplier_key = 7045232
    			-- and supplier_key = <supplier_key>
    			-- Alter to match supplier who has logged in
    		group by
    			part_key,
    			order_priority
    	) fact_rank
    join supply_chain_management.dim_part part on
    	part.part_key = fact_rank.part_key
    where
    	product_rank <= 3
    	-- Top N filter
    order by
    	order_priority,
    	product_rank;