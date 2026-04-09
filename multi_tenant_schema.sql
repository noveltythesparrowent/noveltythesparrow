-- =======================================================
-- SUPER CLEAN MULTI-TENANT PQSGRESQL SCHEMA MIGRATION
-- Use this file to copy-paste into a brand new Supabase DB
-- =======================================================

-- 1. SaaS Multitenancy Table
CREATE TABLE IF NOT EXISTS tenants (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    subscription_status VARCHAR(50) DEFAULT 'Active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO tenants (id, name) VALUES (1, 'Default System Business') ON CONFLICT (id) DO NOTHING;

-- 2. Company Portal Core Tables
CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    tax_id VARCHAR(50),
    phone VARCHAR(50),
    email VARCHAR(255),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS proforma_invoices (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    invoice_number VARCHAR(100) UNIQUE NOT NULL,
    issue_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expiry_date TIMESTAMP,
    subtotal DECIMAL(12,2),
    markup_type VARCHAR(20),
    markup_value DECIMAL(12,2),
    markup_amount DECIMAL(12,2),
    discount_type VARCHAR(20),
    discount_value DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    tax_amount DECIMAL(12,2) DEFAULT 0,
    total_amount DECIMAL(12,2),
    notes TEXT,
    status VARCHAR(20) DEFAULT 'Sent',
    payment_method VARCHAR(50),
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tax_details JSONB,
    client_name VARCHAR(255),
    customer_id INT
);

CREATE TABLE IF NOT EXISTS proforma_invoice_items (
    id SERIAL PRIMARY KEY,
    proforma_id INTEGER REFERENCES proforma_invoices(id) ON DELETE CASCADE,
    product_id INTEGER,
    barcode VARCHAR(50),
    product_name VARCHAR(255) NOT NULL,
    quantity DECIMAL(12,2) NOT NULL,
    unit_price DECIMAL(12,2) NOT NULL,
    line_total DECIMAL(12,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS company_transactions (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    invoice_id INTEGER,
    transaction_type VARCHAR(50),
    amount DECIMAL(12,2),
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sales_invoices (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    proforma_id INTEGER REFERENCES proforma_invoices(id),
    invoice_number VARCHAR(100) UNIQUE NOT NULL,
    issue_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    due_date TIMESTAMP,
    subtotal DECIMAL(12,2),
    markup_type VARCHAR(20),
    markup_value DECIMAL(12,2),
    markup_amount DECIMAL(12,2),
    discount_type VARCHAR(20),
    discount_value DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    tax_amount DECIMAL(12,2) DEFAULT 0,
    paid_amount DECIMAL(12,2) DEFAULT 0,
    total_amount DECIMAL(12,2),
    notes TEXT,
    payment_method VARCHAR(50),
    status VARCHAR(20) DEFAULT 'Unpaid',
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tax_details JSONB,
    client_name VARCHAR(255),
    customer_id INT
);

CREATE TABLE IF NOT EXISTS sales_invoice_items (
    id SERIAL PRIMARY KEY,
    invoice_id INTEGER REFERENCES sales_invoices(id) ON DELETE CASCADE,
    product_id INTEGER,
    barcode VARCHAR(50),
    product_name VARCHAR(255) NOT NULL,
    quantity DECIMAL(12,2) NOT NULL,
    unit_price DECIMAL(12,2) NOT NULL,
    line_total DECIMAL(12,2) NOT NULL
);

-- 3. Core Store Management
CREATE TABLE IF NOT EXISTS branches (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1
);
INSERT INTO branches (id, name, location) VALUES (1, 'Main Warehouse', 'Headquarters') ON CONFLICT (id) DO NOTHING;
INSERT INTO branches (id, name, location) VALUES (2, 'Accra Branch', 'Accra Central') ON CONFLICT (id) DO NOTHING;
INSERT INTO branches (id, name, location) VALUES (3, 'Kumasi Branch', 'Kumasi') ON CONFLICT (id) DO NOTHING;
SELECT setval(pg_get_serial_sequence('branches', 'id'), COALESCE((SELECT MAX(id) FROM branches), 1));


-- 4. User and Auth Management
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    role VARCHAR(50),
    username VARCHAR(100) UNIQUE,
    phone VARCHAR(50),
    employee_id VARCHAR(50) UNIQUE,
    status VARCHAR(20) DEFAULT 'Active',
    store_location VARCHAR(100),
    store_id INT,
    reset_token VARCHAR(255),
    reset_token_expiry TIMESTAMPTZ,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- 5. Products & Inventory
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    barcode VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2) NOT NULL,
    cost_price DECIMAL(10,2) DEFAULT 0,
    selling_unit VARCHAR(50) DEFAULT 'Unit',
    packaging_unit VARCHAR(50) DEFAULT 'Box',
    conversion_rate DECIMAL(10,2) DEFAULT 1,
    reorder_level INTEGER DEFAULT 10,
    track_batch BOOLEAN DEFAULT TRUE,
    track_expiry BOOLEAN DEFAULT TRUE,
    stock_levels JSONB,
    stock INTEGER DEFAULT 0,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT products_tenant_barcode_name_key UNIQUE (tenant_id, barcode, name)
);

CREATE TABLE IF NOT EXISTS product_batches (
    id SERIAL PRIMARY KEY,
    product_barcode VARCHAR(255),
    batch_number VARCHAR(255),
    expiry_date DATE,
    quantity INT DEFAULT 0,
    quantity_available INT DEFAULT 0,
    quantity_received INT DEFAULT 0,
    branch_id INT DEFAULT 1,
    status VARCHAR(50) DEFAULT 'Active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT product_batches_barcode_batch_branch_key UNIQUE (product_barcode, batch_number, branch_id)
);

CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    branch_id INT,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT categories_name_branch_id_key UNIQUE (name, branch_id)
);

-- 6. Sales, Shifts, & POS
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    store_location VARCHAR(100),
    total_amount DECIMAL(10, 2),
    original_total DECIMAL(10, 2),
    current_total DECIMAL(10, 2),
    payment_method VARCHAR(50),
    receipt_number VARCHAR(100),
    items JSONB,
    tax_breakdown JSONB,
    status VARCHAR(20) DEFAULT 'completed',
    is_return BOOLEAN DEFAULT FALSE,
    original_transaction_id INTEGER,
    return_items JSONB,
    has_returns BOOLEAN DEFAULT FALSE,
    customer_id INT,
    customer_name VARCHAR(255),
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS refunds (
    id SERIAL PRIMARY KEY,
    transaction_id INTEGER REFERENCES transactions(id),
    original_receipt_number VARCHAR(100),
    refund_receipt_number VARCHAR(100),
    refund_amount DECIMAL(10, 2),
    payment_method VARCHAR(50),
    processed_by INTEGER,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shifts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    start_cash DECIMAL(10,2) DEFAULT 0,
    end_cash DECIMAL(10,2),
    notes TEXT,
    status VARCHAR(20) DEFAULT 'open',
    tenant_id INT REFERENCES tenants(id) DEFAULT 1
);

-- 7. Customers & CRM
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    phone VARCHAR(50),
    email VARCHAR(255),
    credit_limit DECIMAL(10,2) DEFAULT 0.00,
    current_balance DECIMAL(10,2) DEFAULT 0.00,
    pending_credit_limit DECIMAL(12, 2),
    account_number VARCHAR(10) UNIQUE,
    status VARCHAR(20) DEFAULT 'Active',
    created_by INT,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_ledger (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description VARCHAR(255),
    type VARCHAR(50),
    debit DECIMAL(12, 2) DEFAULT 0.00,
    credit DECIMAL(12, 2) DEFAULT 0.00,
    balance DECIMAL(12, 2) DEFAULT 0.00,
    transaction_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ledger_customer_date ON customer_ledger(customer_id, date);

CREATE TABLE IF NOT EXISTS customer_payments (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    amount DECIMAL(10, 2) NOT NULL,
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    recorded_by INTEGER
);

-- 8. Logistics and Settings
CREATE TABLE IF NOT EXISTS suppliers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    contact_person VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(255),
    address TEXT,
    rating INTEGER DEFAULT 0,
    branch_id INT,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS purchase_orders (
    id SERIAL PRIMARY KEY,
    supplier_id INTEGER,
    branch_id INT,
    status VARCHAR(50) DEFAULT 'Pending',
    total_amount DECIMAL(10, 2) DEFAULT 0,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS expenses (
    id SERIAL PRIMARY KEY,
    category VARCHAR(100) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    description TEXT,
    expense_date DATE DEFAULT CURRENT_DATE,
    branch_id INT DEFAULT 1,
    created_by INT,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system_settings (
    id INT PRIMARY KEY,
    store_name VARCHAR(255),
    currency_symbol VARCHAR(50),
    vat_rate DECIMAL(5,2),
    receipt_footer TEXT,
    tax_id VARCHAR(50),
    phone VARCHAR(50),
    bank_name VARCHAR(255),
    bank_account_name VARCHAR(255),
    bank_account_number VARCHAR(255),
    bank_branch VARCHAR(255),
    momo_number VARCHAR(255),
    momo_name VARCHAR(255),
    credit_auth_code VARCHAR(50) DEFAULT '123456',
    credit_auth_code_expiry TIMESTAMP,
    monthly_target DECIMAL(12,2) DEFAULT 50000.00,
    branch_id INT DEFAULT 1,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT system_settings_branch_id_key UNIQUE (branch_id)
);

CREATE TABLE IF NOT EXISTS tax_rules (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    rate DECIMAL(5,2) NOT NULL,
    branch_id INTEGER DEFAULT 1,
    status VARCHAR(20) DEFAULT 'Active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS promotions (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    discount_percentage DECIMAL(5,2) NOT NULL,
    total_discounted DECIMAL(10,2) DEFAULT 0.00,
    branch_id INT,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS promotion_usage (
    id SERIAL PRIMARY KEY,
    promotion_code VARCHAR(50),
    branch_id INT,
    total_discounted DECIMAL(10,2) DEFAULT 0.00,
    UNIQUE(promotion_code, branch_id)
);

CREATE TABLE IF NOT EXISTS activity_logs (
    id SERIAL PRIMARY KEY,
    user_id INT,
    action VARCHAR(100),
    details JSONB,
    ip_address VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_sessions (
  sid varchar NOT NULL COLLATE "default",
  sess json NOT NULL,
  expire timestamp(6) NOT NULL,
  CONSTRAINT "session_pkey" PRIMARY KEY ("sid")
) WITH (OIDS=FALSE);

-- 9. Add Missing Stock Transfers Table
CREATE TABLE IF NOT EXISTS stock_transfers (
    id SERIAL PRIMARY KEY,
    transfer_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    from_branch_id INTEGER,
    to_branch_id INTEGER,
    status VARCHAR(50) DEFAULT 'Pending',
    created_by INTEGER,
    tenant_id INT REFERENCES tenants(id) DEFAULT 1
);

-- 10. Add Company Users
CREATE TABLE IF NOT EXISTS company_users (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    company_name VARCHAR(255),
    contact_person VARCHAR(100),
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    phone VARCHAR(50),
    address TEXT,
    status VARCHAR(20) DEFAULT 'Active',
    role VARCHAR(50) DEFAULT 'business_client',
    tenant_id INT REFERENCES tenants(id) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
