    create
    	schema if not exists supply_chain_management;
     
    create
    	table
    		supply_chain_management.dim_customer(
    			customer_key bigint not null,
    			customer_name varchar(40) not null,
    			address varchar(80),
    			phone varchar(18),
    			account_balance decimal(14,2),
    			market_segment varchar(20),
    			comment varchar(200),
    			nation varchar(40) not null,
    			nation_comment varchar(200),
    			region varchar(40) not null,
    			region_comment varchar(200),
    			primary key(customer_key)
    		) distkey(customer_key) 
    		  compound sortkey(nation, customer_key);
     
     
    create
    	table
    		supply_chain_management.dim_supplier(
    			supplier_key bigint not null,
    			supplier_name varchar(40) not null,
    			address varchar(80),
    			phone varchar(18),
    			account_balance decimal(12,2),
    			comment varchar(200),
    			nation varchar(40) not null,
    			nation_comment varchar(200),
    			region varchar(40) not null,
    			region_comment varchar(200) primary key(supplier_key)
    		) DISTSTYLE all 
    		 compound sortkey (nation, supplier_key);
     
     
    create
    	table
    		supply_chain_management.dim_part(
    			part_key bigint not null,
    			part_name varchar(104) not null,
    			manufacturer varchar(28),
    			brand varchar(40),
    			type varchar(50),
    			size integer,
    			container varchar(20),
    			retail_price decimal(14,2),
    			comment varchar(100),
    			primary key(part_key)
    		) diststyle even 
    		  compound sortkey(brand, part_key);
     
     
    create
    	table
    		supply_chain_management.fact_line_item_order(
    			customer_key bigint not null,
    			part_key bigint not null,
    			supplier_key bigint not null,
    			order_key bigint not null,
    			ship_date date,
    			line_number integer,
    			quantity decimal(12,2),
    			extended_price decimal(12,2),
    			discount decimal(12,2),
    			tax decimal(12,2),
    			return_flag char(1),
    			line_status char(1),
    			commit_date date,
    			receipt_date date,
    			ship_instruct varchar(34),
    			ship_mode varchar(14),
    			order_status char(1),
    			total_price decimal(12,2),
    			order_date date,
    			order_priority varchar(30),
    			clerk varchar(30),
    			ship_priority integer,
    			supply_cost decimal(12,2),
    			foreign key(customer_key) references supply_chain_management.dim_customer(customer_key),
    			foreign key(part_key) references supply_chain_management.dim_part(part_key),
    			foreign key(supplier_key) references supply_chain_management.dim_supplier(supplier_key)
    		) distkey(customer_key) 
    		  sortkey(ship_date, part_key);
     
    commit;