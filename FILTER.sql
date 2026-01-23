


SELECT * FROM
stg_customers WHERE CAST(SUBSTRING(birthdate FROM 1 FOR 2) as INTEGER) BETWEEN 19 AND 20
AND CAST(SUBSTRING(registration_date FROM 1 FOR 2) as INTEGER) BETWEEN 19 AND 20;



SELECT * FROM stg_order_items WHERE unit_price IS NOT NULL AND unit_price > 0;








INSERT INTO dim_orders (order_id, order_year, order_month, order_day, status, channel, currency, currency_rate)
SELECT
	order_id,
	EXTRACT(year FROM CAST(order_ts as Date)) as order_year,
	EXTRACT(month FROM CAST(order_ts as Date)) as order_month,
	EXTRACT(day FROM CAST(order_ts as Date)) as order_day,
	status,
	channel,
	currency,
	currency_rate
FROM stg_orders
WHERE currency_rate IS NOT NULL AND currency_rate > 0;
ON CONFLICT DO NOTHING;

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
