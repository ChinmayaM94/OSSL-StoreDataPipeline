-- Create STORES table
CREATE TABLE IF NOT EXISTS stores (
    location_id VARCHAR(255) NOT NULL PRIMARY KEY,
    departments VARCHAR(1000),
    operating_status VARCHAR(255),
    is_dug_store BOOLEAN,
    is_delivery_store BOOLEAN,
    is_express_store BOOLEAN,
    is_wgd_enabled BOOLEAN,
    is_flash_dug_enabled BOOLEAN,
    is_flash_delivery_enabled BOOLEAN,
    is_ecom_store BOOLEAN,
    timezone VARCHAR(255),
    brand_name VARCHAR(255),
    division_name VARCHAR(255),
    phone VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    record_version BIGINT
);

-- Create STORE_ADDRESS table
CREATE TABLE IF NOT EXISTS store_address (
    location_id VARCHAR(255) NOT NULL PRIMARY KEY,
    address_line1 VARCHAR(255),
    address_city VARCHAR(255),
    address_state VARCHAR(255),
    address_zipcode VARCHAR(255),
    CONSTRAINT fk_store_address_location FOREIGN KEY (location_id) REFERENCES stores(location_id) ON DELETE CASCADE
);

-- Create indices
CREATE INDEX IF NOT EXISTS idx_stores_location_id ON stores(location_id);
CREATE INDEX IF NOT EXISTS idx_store_address_location_id ON store_address(location_id);

