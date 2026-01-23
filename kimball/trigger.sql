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
        AND CAST(SUBSTRING(birthdate FROM 1 FOR 2) as INTEGER) BETWEEN 19 AND 20
        AND CAST(SUBSTRING(registration_date FROM 1 FOR 2) as INTEGER) BETWEEN 19 AND 20
    THEN
		INSERT INTO dim_customers (customer_id, gender, birth_year, birth_month, birth_day, city, country, registration_date_year, registration_date_month, registration_date_day)
		VALUES (
			NEW.id,
            NEW.gender OR 'U',
            EXTRACT(year FROM CAST(NEW.birthdate as Date)),
            EXTRACT(month FROM CAST(NEW.birthdate as Date)),
            EXTRACT(day FROM CAST(NEW.birthdate as Date)),
            NEW.city OR 'Unknown',
            NEW.country OR 'Unknown',
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

