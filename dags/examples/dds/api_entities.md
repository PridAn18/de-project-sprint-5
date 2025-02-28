# DDL API ENTITES
CREATE TABLE IF NOT EXISTS dds.dm_addresses (
	id serial4 NOT NULL,
	address varchar NOT NULL,
	CONSTRAINT dm_address_pkey PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id),
    CONSTRAINT unique_courier_id UNIQUE (courier_id)
);
CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id int4 NOT NULL,
	order_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
    address_id int4 NOT NULL,
	rate int2 NOT NULL,
    tip_sum int4 NOT NULL,
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
    CONSTRAINT unique_delivery_id UNIQUE (delivery_id),
    CONSTRAINT dm_deliveries_rate_check CHECK ((rate >= 1) AND (rate <= 5)),
    CONSTRAINT dm_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
    CONSTRAINT dm_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT dm_deliveries_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id)
    CONSTRAINT dm_deliveries_address_id_fkey FOREIGN KEY (address_id) REFERENCES dds.dm_addresses(id),
);