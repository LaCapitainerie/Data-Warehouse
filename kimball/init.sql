CREATE TABLE IF NOT EXISTS "fact_sales" (
	"order_info" INTEGER NOT NULL,
	"customer_id" INTEGER NOT NULL,
	"product_id" INTEGER NOT NULL,
	"return_id" INTEGER,
	"qty" INTEGER NOT NULL,
	"price" MONEY NOT NULL,
	"discount" DOUBLE PRECISION,
	"total_price" DOUBLE PRECISION,
	PRIMARY KEY("order_info", "customer_id", "product_id", "return_id")
);




CREATE TABLE IF NOT EXISTS "dim_returns" (
	"return_id" INTEGER NOT NULL UNIQUE,
	"return_year" SMALLINT NOT NULL,
	"return_month" SMALLINT NOT NULL,
	"return_day" SMALLINT NOT NULL,
	"reason" TEXT NOT NULL,
	"amount" MONEY NOT NULL,
	PRIMARY KEY("return_id")
);




CREATE TABLE IF NOT EXISTS "dim_products" (
	"product_id" INTEGER NOT NULL UNIQUE,
	"name" TEXT NOT NULL,
	"category" TEXT NOT NULL,
	"subcategory" TEXT NOT NULL,
	"brand" TEXT NOT NULL,
	"list_price" MONEY NOT NULL,
	PRIMARY KEY("product_id")
);




CREATE TABLE IF NOT EXISTS "dim_orders" (
	"order_id" INTEGER NOT NULL UNIQUE,
	"order_year" SMALLINT NOT NULL,
	"order_month" SMALLINT NOT NULL,
	"order_day" SMALLINT NOT NULL,
	"status" TEXT NOT NULL,
	"channel" TEXT NOT NULL,
	"currency" TEXT NOT NULL,
	"currency_rate" DOUBLE PRECISION NOT NULL,
	PRIMARY KEY("order_id")
);




CREATE TABLE IF NOT EXISTS "dim_customers" (
	"customer_id" INTEGER NOT NULL UNIQUE,
	"gender" CHAR(1) NOT NULL DEFAULT 'U',
	"birthdate_year" SMALLINT NOT NULL,
	"birthdate_month" SMALLINT NOT NULL,
	"birthdate_day" SMALLINT NOT NULL,
	"city" TEXT NOT NULL DEFAULT 'Unknown',
	"country" TEXT NOT NULL DEFAULT 'Unknown',
	"registration_date_year" SMALLINT NOT NULL,
	"registration_date_month" SMALLINT NOT NULL,
	"registration_date_day" SMALLINT NOT NULL,
	PRIMARY KEY("customer_id")
);




CREATE TABLE IF NOT EXISTS "stg_products" (
	"product_id" INTEGER NOT NULL UNIQUE,
	"name" TEXT,
	"category" TEXT,
	"subcategory" TEXT,
	"brand" TEXT,
	"list_price" DOUBLE PRECISION,
	PRIMARY KEY("product_id")
);




CREATE TABLE IF NOT EXISTS "stg_customers" (
	"id" INTEGER NOT NULL UNIQUE,
	"gender" TEXT,
	"birthdate" TEXT,
	"city" TEXT,
	"country" TEXT,
	"registration_date" TEXT,
	PRIMARY KEY("id")
);




CREATE TABLE IF NOT EXISTS "stg_order_items" (
	"order_id" INTEGER,
	"product_id" INTEGER,
	"qty" INTEGER,
	"unit_price" DOUBLE PRECISION,
	"discount" DOUBLE PRECISION
);




CREATE TABLE IF NOT EXISTS "stg_orders" (
	"order_id" INTEGER NOT NULL UNIQUE,
	"customer_id" INTEGER,
	"order_ts" TEXT,
	"status" TEXT,
	"channel" TEXT,
	"currency" TEXT,
	"currency_rate" DOUBLE PRECISION,
	PRIMARY KEY("order_id")
);




CREATE TABLE IF NOT EXISTS "stg_returns" (
	"return_id" INTEGER NOT NULL UNIQUE,
	"order_id" INTEGER,
	"product_id" INTEGER,
	"return_ts" TEXT,
	"reason" TEXT,
	"amount" DOUBLE PRECISION,
	PRIMARY KEY("return_id")
);



ALTER TABLE "fact_sales"
ADD FOREIGN KEY("return_id") REFERENCES "dim_returns"("return_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "fact_sales"
ADD FOREIGN KEY("product_id") REFERENCES "dim_products"("product_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "fact_sales"
ADD FOREIGN KEY("order_info") REFERENCES "dim_orders"("order_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE "fact_sales"
ADD FOREIGN KEY("customer_id") REFERENCES "dim_customers"("customer_id")
ON UPDATE NO ACTION ON DELETE NO ACTION;