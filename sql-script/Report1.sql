    select
    	cust.nation as nation,
    	count( distinct order_key ) as total_orders,
    	sum( quantity ) as total_parts_shiped,
    	sum( extended_price ) as total_extended_price,
    	sum( supply_cost ) as total_supply_cost
    from
    	supply_chain_management.fact_line_item_order fact
    join dim_customer cust on
    	cust.customer_key = fact.customer_key
    join dim_supplier supp on
    	supp.supplier_key = fact.supplier_key
    group by
    	cust.nation;