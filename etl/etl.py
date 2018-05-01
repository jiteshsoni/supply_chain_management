from pyspark.sql import SparkSession

spark = SparkSession\
	.builder\
	.appName("ETL for supply chain management")\
	.getOrCreate()

# Config parameters
input_path = "s3://dory-public/tpch/1000/"
output_path = "s3://dory-public-denormalized/supply_chain_management/"


### Read all source datasets# Load Region
region_df = spark.read.load(input_path + "region")# Load Region
nation_df = spark.read.load(input_path + "nation")# Load Nation
supplier_df = spark.read.load(input_path + "supplier")# Load Supplier
customer_df = spark.read.load(input_path + "customer")# Load Customer
part_df = spark.read.load(input_path + "part")# Load Part
orders_df = spark.read.load(input_path + "orders")# Load Orders
lineitem_df = spark.read.load(input_path + "lineitem_partition")# Load lineitems
partsupp_df = spark.read.load(input_path + "partsupp")# Load part supplier


# Column transformations and Joins for Dimension tables
nation_region_df = nation_df.join(region_df, nation_df.n_regionkey == region_df
		.r_regionkey, 'inner')# Merge / Join attributes of Nation and Region
# The below operations might look more cleaner in a single SQL query
nation_region_df = nation_region_df.drop('r_regionkey').drop('n_regionkey')# Drop the region key which was used in the previous join
nation_region_df = nation_region_df.withColumnRenamed('n_name', 'nation').withColumnRenamed(
	'n_comment', 'nation_comment').withColumnRenamed('r_name', 'region').withColumnRenamed(
	'r_comment', 'region_comment')# Rename columns to meet business definitions
nation_region_df.cache()# Cache as it is being used twice

#Create Supplier Dimenion
supplier_df = supplier_df.join(nation_region_df, supplier_df.s_nationkey ==
		nation_region_df.n_nationkey, 'inner')# Merge attributes from nation_region into Supplier Dimension table
		# Drop duplicate columns used in the previous join

supplier_df = supplier_df.drop('n_nationkey').drop('s_nationkey')# Drop duplicate columns used in the previous join
		# Since we are getting rid of Nation and Region tables, there is no need to maintain their keys

supplier_df = supplier_df.withColumnRenamed('s_name', 'supplier_name').withColumnRenamed(
	's_address', 'address').withColumnRenamed('s_phone', 'phone').withColumnRenamed(
	's_comment', 'comment').withColumnRenamed('s_acctbal', 'account_balance').withColumnRenamed(
	's_suppkey', 'supplier_key')# Rename columns to suit business definitions

# Create dataset for Customer Dimension 
customer_df = customer_df.join(nation_region_df, customer_df.c_nationkey ==
		nation_region_df.n_nationkey, 'inner')# Merge attributes from nation_region into Supplier Dimension
		
	
customer_df = customer_df.drop('n_nationkey').drop('c_nationkey')# Drop duplicate columns used in the previous join
	# Since we are getting rid of Nation and Region tables, there is no need to maintain their keys
customer_df = customer_df.withColumnRenamed('c_name', 'customer_name').withColumnRenamed(
	'c_address', 'address').withColumnRenamed('c_phone', 'phone').withColumnRenamed(
	'c_comment', 'comment').withColumnRenamed('c_acctbal', 'account_balance').withColumnRenamed(
	'c_custkey', 'customer_key').withColumnRenamed('c_mktsegment',
	'market_segment')# Rename columns to suit business definitions


# Create dataset for Part Dimesnion

part_df = part_df.withColumnRenamed('p_partkey', 'part_key').withColumnRenamed(
	'p_name', 'part_name').withColumnRenamed('p_mfgr', 'manufacturer').withColumnRenamed(
	'p_brand', 'brand').withColumnRenamed('p_type', 'type').withColumnRenamed(
	'p_size', 'size').withColumnRenamed('p_container', 'container').withColumnRenamed(
	'p_retailprice', 'retail_price').withColumnRenamed('p_comment', 'comment')# Rename columns to suit business definitions

# Column transformations for Orders dataset
orders_df = orders_df.withColumnRenamed('o_orderkey', 'order_key').withColumnRenamed(
	'o_custkey', 'customer_key').withColumnRenamed('o_orderstatus',
	'order_status').withColumnRenamed('o_totalprice', 'total_price').withColumnRenamed(
	'o_orderdate', 'order_date').withColumnRenamed('o_orderpriority',
	'order_priority').withColumnRenamed('o_clerk', 'clerk').withColumnRenamed(
	'o_shippriority', 'ship_priority').drop('o_comment')# Rename columns to suit business definitions
	
# Column transformations for Lineitem dataset
lineitem_df = lineitem_df.withColumnRenamed('l_orderkey', 'order_key').withColumnRenamed(
		'l_partkey', 'part_key').withColumnRenamed('l_suppkey', 'supplier_key').withColumnRenamed(
		'l_linenumber', 'line_number').withColumnRenamed('l_quantity', 'quantity').withColumnRenamed(
		'l_extendedprice', 'extended_price').withColumnRenamed('l_discount',
		'discount').withColumnRenamed('l_tax', 'tax').withColumnRenamed(
		'l_returnflag', 'return_flag').withColumnRenamed('l_linestatus',
		'line_status').withColumnRenamed('l_commitdate', 'commit_date').withColumnRenamed(
		'l_receiptdate', 'receipt_date').withColumnRenamed('l_shipdate', 'ship_date')
lineitem_df =lineitem_df.withColumnRenamed('l_shipinstruct', 'ship_instruct').withColumnRenamed(
		'l_shipmode', 'ship_mode').drop('l_comment')# Rename columns to suit business definitions

lineitem_df = lineitem_df.withColumn("commit_date", lineitem_df.commit_date.cast(
	"date")).withColumn("receipt_date", lineitem_df.receipt_date.cast("date"))# Cast strings to date

# Column transformations for Part_Supplier dataset
partsupp_df = partsupp_df.withColumnRenamed('ps_partkey', 'part_key').withColumnRenamed(
		'ps_suppkey', 'supplier_key').withColumnRenamed('ps_supplycost',
		'supply_cost').drop('ps_availqty').drop('ps_comment')
''' Dropping column 'ps_comment' as it is not a metric and Fact table is at a different granulairty.Thus it does not makes sense to merge in
		the denormailzed fact table. We can drop the column 'ps_availqty'
	as this table does not have a date column and available quanity will change over time '''
### Create De-normailzed fact table 
lineitem_order_df = lineitem_df.join(orders_df, ["order_key"], 'inner') # Merge attributes from  Orders into lineitem_ 

denormalized_fact_df = lineitem_order_df.join(partsupp_df, ["part_key","supplier_key"]) # Join lineitem  with Parts Supplier table

# Write data back to S3 in parquet format
supplier_df.write.partitionBy("nation").format("parquet").save(output_path + "supplier")
customer_df.write.partitionBy("nation").format("parquet").save(output_path + "customer")
part_df.write.partitionBy("brand").format("parquet").save(output_path + "customer")
part_df.write.partitionBy("shipdate","nation",).format("parquet").save(output_path + "fact_lineitem_order")


