CREATE TABLE IF NOT EXISTS "fact_sales" (
	"order_info" INTEGER NOT NULL,
	"customer_id" INTEGER NOT NULL,
	"product_id" INTEGER NOT NULL,
	"return_id" INTEGER,
	"qty" INTEGER NOT NULL,
	"price" DOUBLE PRECISION NOT NULL,
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
	"amount" DOUBLE PRECISION NOT NULL,
	PRIMARY KEY("return_id")
);




CREATE TABLE IF NOT EXISTS "dim_products" (
	"product_id" INTEGER NOT NULL UNIQUE,
	"name" TEXT NOT NULL,
	"category" TEXT NOT NULL,
	"subcategory" TEXT NOT NULL,
	"brand" TEXT NOT NULL,
	"list_price" DOUBLE PRECISION NOT NULL,
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
	"gender" CHAR(1) NOT NULL,
	"birthdate_year" SMALLINT NOT NULL,
	"birthdate_month" SMALLINT NOT NULL,
	"birthdate_day" SMALLINT NOT NULL,
	"city" TEXT NOT NULL,
	"country" TEXT NOT NULL,
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


--------------------------------------------


ALTER TABLE "dim_customers"
ALTER COLUMN "gender" SET DEFAULT 'U';
ALTER TABLE "dim_customers"
ALTER COLUMN "city" SET DEFAULT 'Unknown';
ALTER TABLE "dim_customers"
ALTER COLUMN "country" SET DEFAULT 'Unknown';


-- Fonction trigger pour insérer automatiquement dans dim_orders
CREATE OR REPLACE FUNCTION insert_into_dim_orders()
RETURNS TRIGGER AS $$
BEGIN
	-- Vérifier la condition sur currency_rate
	IF NEW.currency_rate IS NOT NULL AND NEW.currency_rate > 0 THEN
		INSERT INTO dim_orders (order_id, order_year, order_month, order_day, status, channel, currency, currency_rate)
		VALUES (
			NEW.order_id,
			EXTRACT(year FROM CAST(NEW.order_ts as Date)),
			EXTRACT(month FROM CAST(NEW.order_ts as Date)),
			EXTRACT(day FROM CAST(NEW.order_ts as Date)),
			NEW.status,
			NEW.channel,
			NEW.currency,
			NEW.currency_rate
		)
		ON CONFLICT (order_id) DO NOTHING;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Création du trigger qui s'exécute après INSERT sur stg_orders
CREATE TRIGGER trigger_insert_dim_orders
AFTER INSERT ON stg_orders
FOR EACH ROW
EXECUTE FUNCTION insert_into_dim_orders();





-- Fonction trigger pour insérer automatiquement dans dim_customers
CREATE OR REPLACE FUNCTION insert_into_dim_customers()
RETURNS TRIGGER AS $$
BEGIN
	-- Vérifier la condition sur currency_rate
	IF NEW.id IS NOT NULL
        AND NEW.id > 0
        AND CAST(SUBSTRING(NEW.birthdate FROM 1 FOR 2) as INTEGER) BETWEEN 19 AND 20
        AND CAST(SUBSTRING(NEW.registration_date FROM 1 FOR 2) as INTEGER) BETWEEN 19 AND 20
    THEN
		INSERT INTO dim_customers (customer_id, gender, birthdate_year, birthdate_month, birthdate_day, city, country, registration_date_year, registration_date_month, registration_date_day)
		VALUES (
			NEW.id,
            COALESCE(NEW.gender, 'U'),
            EXTRACT(year FROM CAST(NEW.birthdate as Date)),
            EXTRACT(month FROM CAST(NEW.birthdate as Date)),
            EXTRACT(day FROM CAST(NEW.birthdate as Date)),
            COALESCE(NEW.city, 'Unknown'),
            COALESCE(NEW.country, 'Unknown'),
            EXTRACT(year FROM CAST(NEW.registration_date as Date)),
            EXTRACT(month FROM CAST(NEW.registration_date as Date)),
            EXTRACT(day FROM CAST(NEW.registration_date as Date))
		)
		ON CONFLICT (customer_id) DO NOTHING;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Création du trigger qui s'exécute après INSERT sur stg_customers
CREATE TRIGGER trigger_insert_dim_customers
AFTER INSERT ON stg_customers
FOR EACH ROW
EXECUTE FUNCTION insert_into_dim_customers();




-- Fonction trigger pour insérer automatiquement dans dim_products
CREATE OR REPLACE FUNCTION insert_into_dim_products()
RETURNS TRIGGER AS $$
BEGIN
	-- Vérifier la condition sur list_price
	IF NEW.product_id IS NOT NULL
		AND NEW.product_id > 0
		AND NEW.list_price IS NOT NULL
		AND NEW.list_price > 0
		AND NEW.name IS NOT NULL
		AND NEW.name != '' THEN
		INSERT INTO dim_products (product_id, name, category, subcategory, brand, list_price)
		VALUES (
			NEW.product_id,
			NEW.name,
			COALESCE(NEW.category, 'Unknown'),
			COALESCE(NEW.subcategory, 'Unknown'),
			COALESCE(NEW.brand, 'Unknown'),
			NEW.list_price
		)
		ON CONFLICT (product_id) DO NOTHING;
	END IF;
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Création du trigger qui s'exécute après INSERT sur stg_products
CREATE TRIGGER trigger_insert_dim_products
AFTER INSERT ON stg_products
FOR EACH ROW
EXECUTE FUNCTION insert_into_dim_products();