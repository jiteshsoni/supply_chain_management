    select
    	supplier_name,
    	count( part_key ) as total_parts_shipped_late,
    	avg( receipt_date - commit_date ) as average_days_dealyed
    from
    	supply_chain_management.fact_line_item_order fact
    join supply_chain_management.dim_supplier supp on
    	supp.supplier_key = fact.supplier_key
    where
    	receipt_date > commit_date
    group by
    	supplier_name;