require('dotenv').config({ path: require('path').join(__dirname, '.env') });

// Handle unhandled promise rejections & uncaught exceptions at the VERY TOP
process.on('unhandledRejection', (err) => {
    console.error('CRITICAL: Unhandled Rejection:', err);
    console.error('Stack:', err?.stack || 'No stack trace');
    // Keep server running - just log the error
});

process.on('uncaughtException', (err) => {
    console.error('CRITICAL: Uncaught Exception:', err);
    console.error('Stack:', err?.stack || 'No stack trace');
    // Keep server running - just log the error
});

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const nodemailer = require('nodemailer');
const PDFDocument = require('pdfkit');
const JWT_SECRET = process.env.JWT_SECRET || 'footprint-secure-secret-key-2025';
const path = require('path');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
const https = require('https');
const session = require('express-session');
const pgSession = require('connect-pg-simple')(session);
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { body, validationResult } = require('express-validator');
const crypto = require('crypto');
const security = require('./security');
const multer = require('multer');
const xlsx = require('xlsx');
const { AsyncLocalStorage } = require('async_hooks');

// SaaS RLS Wrapper Context Tracking
const tenantContext = new AsyncLocalStorage();
const faviconPath = path.join(__dirname, 'logo.png');

const app = express();
// Override port 5000 to 5001 to avoid macOS AirPlay conflict
const port = (process.env.PORT && process.env.PORT !== '5000') ? process.env.PORT : 5008;

// Configure Multer for memory storage (handling file uploads)
const upload = multer({ storage: multer.memoryStorage() });

// Serve favicon
app.get('/favicon.ico', (req, res) => {
    res.sendFile(faviconPath);
});

// Database connection
let pool;
try {
    const connStr = process.env.DATABASE_URL;

    // Enable SSL for production environments or if connecting to a Supabase URL
    const isProduction = process.env.NODE_ENV === 'production';
    const isSupabase = connStr && (
        connStr.includes('supabase.co') ||
        connStr.includes('supabase.com') ||
        connStr.includes('sslmode=require')
    );
    const ssl = (isProduction || isSupabase) ? { rejectUnauthorized: false } : false;

    if (process.env.PGHOST || process.env.PGUSER || process.env.PGPASSWORD || process.env.PGDATABASE) {
        const poolConfig = {
            host: process.env.PGHOST || 'localhost',
            user: process.env.PGUSER || undefined,
            password: process.env.PGPASSWORD || undefined,
            database: process.env.PGDATABASE || undefined,
            port: process.env.PGPORT ? parseInt(process.env.PGPORT) : undefined,
            ssl
        };
        console.log('Using individual PG env vars for connection (masked).');
        pool = new Pool(poolConfig);
    } else if (connStr) {
        // Supabase Pooler (Port 6543) requires prepare_threshold=0 to avoid "prepared statement" errors
        // We also strip sslmode from the string to ensure our explicit ssl config object works
        let cleanConnStr = connStr.replace(/sslmode=[^&]*/g, '')
            .replace(/\?&/, '?')
            .replace(/&&/g, '&')
            .replace(/[?&]$/, '');

        if (cleanConnStr.includes(':6543')) {
            const separator = cleanConnStr.includes('?') ? '&' : '?';
            if (!cleanConnStr.includes('prepare_threshold')) {
                cleanConnStr += `${separator}prepare_threshold=0`;
            }
        }

        try {
            console.log('Using DATABASE_URL (masked):', cleanConnStr.replace(/:(.*)@/, ':*****@'));
        } catch (e) { console.log('Using DATABASE_URL (masked)'); }
        pool = new Pool({ connectionString: cleanConnStr, ssl });
    } else {
        throw new Error('No database configuration found in environment. Set DATABASE_URL or PGHOST/PGUSER/PGPASSWORD/PGDATABASE.');
    }

    // CRITICAL: Listen for pool errors to prevent Node.js from crashing silently
    pool.on('error', (err) => {
        console.error('DATABASE POOL ERROR:', err);
    });
} catch (err) {
    console.error('Postgres pool init error:', err && err.message ? err.message : err);
    throw err;
}

// SaaS RLS Wrapper: Automatically inject `tenant_id` sandbox into all DB queries executed by Node.
const originalPoolConnect = pool.connect.bind(pool);
pool.connect = async function() {
    const client = await originalPoolConnect();
    
    // Safely wrap the client just once per lifecycle
    if (!client.__saas_patched) {
        const originalClientQuery = client.query.bind(client);
        client.query = async function(...args) {
            const tenantId = tenantContext.getStore();
            const configSet = tenantId !== undefined && tenantId !== null && tenantId !== '';
            
            if (configSet) {
                await originalClientQuery(`SELECT set_config('app.current_tenant', $1, false)`, [tenantId.toString()]);
            }
            
            try {
                return await originalClientQuery(...args);
            } finally {
                if (configSet) {
                    await originalClientQuery(`SELECT set_config('app.current_tenant', '', false)`);
                }
            }
        };
        client.__saas_patched = true;
    }
    return client;
};

// Also patch the direct pool.query fallback which behaves independently in older `pg` versions
const originalPoolQuery = pool.query.bind(pool);
pool.query = async function(...args) {
    const client = await pool.connect();
    try {
        return await client.query(...args);
    } finally {
        client.release();
    }
};

// Audit Logging Helper
async function logActivity(req, action, details = {}) {
    try {
        const userId = req.user?.id || req.session?.user?.id || null;
        const ip = req.headers?.['x-forwarded-for'] || req.socket?.remoteAddress || '0.0.0.0';
        await pool.query(
            'INSERT INTO activity_logs (user_id, action, details, ip_address) VALUES ($1, $2, $3, $4)',
            [userId, action, details, ip]  // Don't stringify for JSONB column
        );
    } catch (err) {
        console.error('Audit Log Error:', err.message);
    }
}

// Initialize/Migrate Database Schema
async function initDb() {
    try {
        await pool.query(`
            -- SaaS Multitenancy Table
            CREATE TABLE IF NOT EXISTS tenants (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                subscription_status VARCHAR(50) DEFAULT 'Active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO tenants (id, name) VALUES (1, 'Default System Business') ON CONFLICT (id) DO NOTHING;
            
            -- Company Portal Core Tables
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                tax_id VARCHAR(50),
                phone VARCHAR(50),
                email VARCHAR(255),
                address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);

        // Force explicit separate creation for company_users to prevent transaction pooling drops
        await pool.query(`
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
        `);

        await pool.query(`
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
                created_by INTEGER REFERENCES users(id),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                created_by INTEGER REFERENCES users(id),
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
                created_by INTEGER REFERENCES users(id),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

            -- Ensure barcode and product_id columns exist for migrations
            ALTER TABLE proforma_invoice_items ADD COLUMN IF NOT EXISTS product_id INTEGER;
            ALTER TABLE proforma_invoice_items ADD COLUMN IF NOT EXISTS barcode VARCHAR(50);
            ALTER TABLE sales_invoice_items ADD COLUMN IF NOT EXISTS product_id INTEGER;
            ALTER TABLE sales_invoice_items ADD COLUMN IF NOT EXISTS barcode VARCHAR(50);

            -- Ensure tax columns exist for migrations
            ALTER TABLE proforma_invoices ADD COLUMN IF NOT EXISTS tax_amount DECIMAL(12,2) DEFAULT 0;
            ALTER TABLE proforma_invoices ADD COLUMN IF NOT EXISTS tax_details JSONB;
            ALTER TABLE proforma_invoices ADD COLUMN IF NOT EXISTS payment_method VARCHAR(50);
            ALTER TABLE sales_invoices ADD COLUMN IF NOT EXISTS payment_method VARCHAR(50);
            ALTER TABLE proforma_invoices ADD COLUMN IF NOT EXISTS client_name VARCHAR(255);
            ALTER TABLE sales_invoices ADD COLUMN IF NOT EXISTS tax_amount DECIMAL(12,2) DEFAULT 0;
            ALTER TABLE sales_invoices ADD COLUMN IF NOT EXISTS tax_details JSONB;
            ALTER TABLE sales_invoices ADD COLUMN IF NOT EXISTS client_name VARCHAR(255);
            ALTER TABLE proforma_invoices ADD COLUMN IF NOT EXISTS customer_id INT;
            ALTER TABLE sales_invoices ADD COLUMN IF NOT EXISTS customer_id INT;

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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            ALTER TABLE product_batches ADD COLUMN IF NOT EXISTS quantity INT DEFAULT 0;
            ALTER TABLE product_batches ADD COLUMN IF NOT EXISTS quantity_available INT DEFAULT 0;
            ALTER TABLE product_batches ADD COLUMN IF NOT EXISTS quantity_received INT DEFAULT 0;
            ALTER TABLE product_batches ADD COLUMN IF NOT EXISTS branch_id INT DEFAULT 1;
            ALTER TABLE product_batches ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'Active';
            
            -- Fix existing data
            UPDATE product_batches SET quantity_available = quantity WHERE quantity_available = 0 AND quantity > 0;
            UPDATE product_batches SET branch_id = 1 WHERE branch_id IS NULL;

            -- Create Branches Table
            CREATE TABLE IF NOT EXISTS branches (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                location VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Seed default branches if empty
            INSERT INTO branches (id, name, location) VALUES (1, 'Novelty The Sparrow Ent', 'Dzorwulu') ON CONFLICT (id) DO NOTHING;
            
            -- Force update legacy initializations
            UPDATE branches SET name = 'Novelty The Sparrow Ent', location = 'Dzorwulu' WHERE id = 1 AND name = 'Main Warehouse';
            -- No additional dummy branches will be forced into the database.
            -- FIX: Sync branches_id_seq with the actual max id to prevent duplicate key errors
            SELECT setval(pg_get_serial_sequence('branches', 'id'), COALESCE((SELECT MAX(id) FROM branches), 1));

            -- Create Products Table if not exists
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Add unique constraint on barcode+name combination (not just barcode)
            -- First check for and handle any existing duplicates
            DO $$ 
            DECLARE
                dup_count INTEGER;
            BEGIN
                -- Count duplicates
                SELECT COUNT(*) INTO dup_count
                FROM (
                    SELECT barcode, name 
                    FROM products 
                    GROUP BY barcode, name 
                    HAVING COUNT(*) > 1
                ) dups;
                
                -- If no duplicates, proceed with constraint change
                IF dup_count = 0 THEN
                    -- Drop the old barcode primary key constraint with CASCADE to remove dependencies
                    ALTER TABLE products DROP CONSTRAINT IF EXISTS products_pkey CASCADE;
                    ALTER TABLE products DROP CONSTRAINT IF EXISTS products_barcode_name_key CASCADE;
                    
                    -- Add new unique constraint on tenant_id+barcode+name if it doesn't exist
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'products_tenant_barcode_name_key'
                    ) THEN
                        ALTER TABLE products ADD CONSTRAINT products_tenant_barcode_name_key UNIQUE (tenant_id, barcode, name);
                    END IF;
                ELSE
                    -- Skip constraint creation if duplicates exist - keep existing structure
                    RAISE NOTICE 'Skipping unique constraint creation - % duplicate (barcode,name) pairs found', dup_count;
                END IF;
            END $$;

            -- 1. Initialize stock_levels from stock if missing (Legacy support)
            UPDATE products 
            SET stock_levels = jsonb_build_object('Main Warehouse', stock)
            WHERE (stock_levels IS NULL OR stock_levels = '{}'::jsonb) AND stock > 0;

            -- 2. Force sync total stock from stock_levels (Ensures Dashboard KPI is accurate)
            UPDATE products 
            SET stock = (
                SELECT COALESCE(SUM(value::int), 0) 
                FROM jsonb_each_text(COALESCE(stock_levels, '{}'::jsonb))
            );

            -- Create Users Table if not exists
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                name VARCHAR(255),
                role VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Ensure users table has all required columns for Staff Management
            ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR(100) UNIQUE;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS phone VARCHAR(50);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS employee_id VARCHAR(50) UNIQUE;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'Active';
            ALTER TABLE users ADD COLUMN IF NOT EXISTS store_location VARCHAR(100);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS store_id INT;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS reset_token VARCHAR(255);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS reset_token_expiry TIMESTAMPTZ;

            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = 'users'
                      AND column_name = 'reset_token_expiry'
                      AND data_type = 'timestamp without time zone'
                ) THEN
                    ALTER TABLE users
                        ALTER COLUMN reset_token_expiry
                        TYPE TIMESTAMPTZ
                        USING reset_token_expiry AT TIME ZONE 'UTC';
                END IF;
            END $$;

            -- Update existing users to have Active status if null
            UPDATE users SET status = 'Active' WHERE status IS NULL;
            
            -- Normalize existing emails and roles to lowercase to prevent case-sensitivity issues
            UPDATE users SET email = LOWER(email), role = LOWER(role);

            -- Ensure reorder_level has default
            UPDATE products SET reorder_level = 10 WHERE reorder_level IS NULL;

            /* Create Promotions Table if not exists */
            CREATE TABLE IF NOT EXISTS promotions (
                id SERIAL PRIMARY KEY,
                code VARCHAR(50) UNIQUE NOT NULL,
                discount_percentage DECIMAL(5,2) NOT NULL,
                total_discounted DECIMAL(10,2) DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Ensure total_discounted column exists
           ALTER TABLE promotions ADD COLUMN IF NOT EXISTS total_discounted DECIMAL(10,2) DEFAULT 0.00;
           ALTER TABLE promotions ADD COLUMN IF NOT EXISTS branch_id INT;

           /* Create Promotion Usage Table for Branch Specific Tracking */
           CREATE TABLE IF NOT EXISTS promotion_usage (
                id SERIAL PRIMARY KEY,
                promotion_code VARCHAR(50),
                branch_id INT,
                total_discounted DECIMAL(10,2) DEFAULT 0.00,
                UNIQUE(promotion_code, branch_id)
           );

           -- Create Categories Table
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                branch_id INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Ensure unique constraint on name + branch for categories to support ON CONFLICT
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'categories_name_branch_id_key') THEN
                    ALTER TABLE categories ADD CONSTRAINT categories_name_branch_id_key UNIQUE (name, branch_id);
                END IF;
            END $$;

            -- Create Suppliers Table
            CREATE TABLE IF NOT EXISTS suppliers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                contact_person VARCHAR(100),
                phone VARCHAR(50),
                email VARCHAR(255),
                address TEXT,
                rating INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Create Purchase Orders Table
            CREATE TABLE IF NOT EXISTS purchase_orders (
                id SERIAL PRIMARY KEY,
                supplier_id INTEGER,
                status VARCHAR(50) DEFAULT 'Pending',
                total_amount DECIMAL(10, 2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Create Transactions Table
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS tax_breakdown JSONB;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS original_total DECIMAL(10, 2);
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS current_total DECIMAL(10, 2);

            CREATE TABLE IF NOT EXISTS refunds (
                id SERIAL PRIMARY KEY,
                transaction_id INTEGER REFERENCES transactions(id),
                original_receipt_number VARCHAR(100),
                refund_receipt_number VARCHAR(100),
                refund_amount DECIMAL(10, 2),
                payment_method VARCHAR(50),
                processed_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

           -- Add branch_id to other inventory tables for isolation
           ALTER TABLE categories ADD COLUMN IF NOT EXISTS branch_id INT;
           ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS branch_id INT;
           ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS branch_id INT;

            -- Ensure transactions table has status column
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'completed';
            UPDATE transactions SET status = 'completed' WHERE status IS NULL;
            
            -- Add return tracking columns
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS is_return BOOLEAN DEFAULT FALSE;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS original_transaction_id INTEGER;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS return_items JSONB;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS has_returns BOOLEAN DEFAULT FALSE;
            
            -- Create Customers Table
            CREATE TABLE IF NOT EXISTS customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                phone VARCHAR(50),
                email VARCHAR(255),
                credit_limit DECIMAL(10,2) DEFAULT 0.00,
                current_balance DECIMAL(10,2) DEFAULT 0.00,
                account_number VARCHAR(10) UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            ALTER TABLE customers ADD COLUMN IF NOT EXISTS account_number VARCHAR(10) UNIQUE;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS customer_id INT;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS customer_name VARCHAR(255);
            ALTER TABLE customers ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'Active';
            ALTER TABLE customers ADD COLUMN IF NOT EXISTS created_by INT;

            ALTER TABLE customers ADD COLUMN IF NOT EXISTS pending_credit_limit DECIMAL(12, 2);

            -- Create Shifts Table
            CREATE TABLE IF NOT EXISTS shifts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                start_cash DECIMAL(10,2) DEFAULT 0,
                end_cash DECIMAL(10,2),
                notes TEXT,
                status VARCHAR(20) DEFAULT 'open'
            );

            -- Create Customer Payments Table (For Statement Generation)
            CREATE TABLE IF NOT EXISTS customer_payments (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id),
                amount DECIMAL(10, 2) NOT NULL,
                payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                recorded_by INTEGER
            );
        `);

        // Fix legacy schema constraints (first_name/last_name) to allow NULLs
        try {
            await pool.query(`
                    DO $$ 
                    BEGIN 
                        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'customers' AND column_name = 'first_name') THEN 
                            ALTER TABLE customers ALTER COLUMN first_name DROP NOT NULL; 
                        END IF;
                        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'customers' AND column_name = 'last_name') THEN 
                            ALTER TABLE customers ALTER COLUMN last_name DROP NOT NULL; 
                        END IF;
                    END $$;
                `);
        } catch (e) { console.error('Schema constraint fix error:', e.message); }

        await pool.query(`
            -- Create Expenses Table for Net Profit Calculation
            CREATE TABLE IF NOT EXISTS expenses (
                id SERIAL PRIMARY KEY,
                category VARCHAR(100) NOT NULL, -- Rent, Utilities, Salary, etc.
                amount DECIMAL(10,2) NOT NULL,
                description TEXT,
                expense_date DATE DEFAULT CURRENT_DATE,
                branch_id INT DEFAULT 1,
                created_by INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Create Activity Logs Table (Audit Trail)
            CREATE TABLE IF NOT EXISTS activity_logs (
                id SERIAL PRIMARY KEY,
                user_id INT,
                action VARCHAR(100),
                details JSONB,
                ip_address VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Create System Settings Table
            CREATE TABLE IF NOT EXISTS system_settings (
                id INT PRIMARY KEY,
                store_name VARCHAR(255),
                currency_symbol VARCHAR(50),
                vat_rate DECIMAL(5,2),
                receipt_footer TEXT,
                tax_id VARCHAR(50),
                phone VARCHAR(50),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Add branch_id to system_settings if not exists
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS branch_id INT DEFAULT 1;
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS tax_id VARCHAR(50);
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS phone VARCHAR(50);
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS bank_name VARCHAR(255);
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS bank_account_name VARCHAR(255);
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS bank_account_number VARCHAR(255);
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS bank_branch VARCHAR(255);
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS momo_number VARCHAR(255);
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS momo_name VARCHAR(255);
            
            -- Add credit_auth_code to system_settings
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS credit_auth_code VARCHAR(50) DEFAULT '123456';
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS credit_auth_code_expiry TIMESTAMP;
            
            -- Add monthly_target to system_settings
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS monthly_target DECIMAL(12,2) DEFAULT 50000.00;

            -- Ensure unique constraint on branch_id
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'system_settings_branch_id_key') THEN
                    ALTER TABLE system_settings ADD CONSTRAINT system_settings_branch_id_key UNIQUE (branch_id);
                END IF;
            END $$;

            -- Insert default settings if not exists
            INSERT INTO system_settings (id, branch_id, store_name, currency_symbol, vat_rate, receipt_footer)
            VALUES (1, 1, 'Footprint Retail Systems', '₵ (GHS)', 15.00, 'Thank you for shopping with us!')
            ON CONFLICT (id) DO NOTHING;

            -- Create Customer Ledger Table (Part 2 Implementation)
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

            -- Backfill Ledger if empty (Ensures existing data appears in statements)
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM customer_ledger LIMIT 1) THEN
                    -- Insert Sales
                    INSERT INTO customer_ledger (customer_id, date, description, type, debit, credit, transaction_id)
                    SELECT customer_id, created_at, COALESCE(receipt_number, 'TRX') || ' (Sale)', 'SALE', total_amount, 0, id
                    FROM transactions WHERE payment_method = 'credit' AND customer_id IS NOT NULL AND status = 'completed';
                    
                    -- Insert Payments
                    INSERT INTO customer_ledger (customer_id, date, description, type, debit, credit)
                    SELECT customer_id, payment_date, 'Payment Received', 'PAYMENT', 0, amount FROM customer_payments;
                END IF;
            END $$;

            -- FIX: Backfill store_id for users who have a location but no ID (Ensures Managers see correct VAT)
            UPDATE users u
            SET store_id = b.id
            FROM branches b
            WHERE (LOWER(u.store_location) = LOWER(b.location) OR LOWER(u.store_location) = LOWER(b.name))
            AND u.store_id IS NULL;
        `);

        // --- ENHANCED INVENTORY TABLES (Fix for 500 Errors) ---
        await pool.query(`
            -- Shelf Management
            CREATE TABLE IF NOT EXISTS shelf_inventory (
                id SERIAL PRIMARY KEY,
                product_barcode VARCHAR(50),
                quantity_on_shelf INTEGER DEFAULT 0,
                store_quantity INTEGER DEFAULT 0,
                branch_id INTEGER,
                last_verified TIMESTAMP,
                staff_id INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(product_barcode, branch_id)
            );
            
            -- Fix: Ensure branch_id exists on shelf_inventory (Migration for existing tables)
            ALTER TABLE shelf_inventory ADD COLUMN IF NOT EXISTS branch_id INTEGER;
            
            -- Fix: Update unique constraint to include branch_id if it was previously just product_barcode
            DO $$ BEGIN
                IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'shelf_inventory_product_barcode_key') THEN
                    ALTER TABLE shelf_inventory DROP CONSTRAINT shelf_inventory_product_barcode_key;
                    ALTER TABLE shelf_inventory ADD CONSTRAINT shelf_inventory_product_barcode_branch_id_key UNIQUE (product_barcode, branch_id);
                END IF;
            END $$;

            CREATE TABLE IF NOT EXISTS shelf_movements (
                id SERIAL PRIMARY KEY,
                product_barcode VARCHAR(50),
                movement_type VARCHAR(50),
                quantity INTEGER,
                staff_id INTEGER,
                from_location VARCHAR(100),
                to_location VARCHAR(100),
                branch_id INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Stock Adjustments
            CREATE TABLE IF NOT EXISTS stock_adjustments (
                id SERIAL PRIMARY KEY,
                product_barcode VARCHAR(50),
                adjustment_type VARCHAR(50),
                quantity_adjusted INTEGER,
                reason TEXT,
                approver_id INTEGER,
                branch_id INTEGER,
                approved_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Stock Takes
            CREATE TABLE IF NOT EXISTS stock_takes (
                id SERIAL PRIMARY KEY,
                stock_take_date DATE,
                branch_id INTEGER,
                created_by INTEGER,
                approved_by INTEGER,
                status VARCHAR(50) DEFAULT 'In Progress',
                variance_total DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS stock_take_items (
                id SERIAL PRIMARY KEY,
                stock_take_id INTEGER REFERENCES stock_takes(id) ON DELETE CASCADE,
                product_barcode VARCHAR(50),
                physical_count INTEGER,
                system_count INTEGER,
                variance INTEGER,
                variance_reason VARCHAR(255),
                counted_by VARCHAR(100),
                counted_at TIMESTAMP
            );

            -- Reorder Alerts
            CREATE TABLE IF NOT EXISTS reorder_alerts (
                id SERIAL PRIMARY KEY,
                product_barcode VARCHAR(50),
                current_stock INTEGER,
                reorder_level INTEGER,
                suggested_quantity INTEGER,
                priority VARCHAR(20),
                status VARCHAR(50) DEFAULT 'Active',
                branch_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                acknowledged_at TIMESTAMP,
                UNIQUE(product_barcode)
            );

            -- Inventory Audit Log
            CREATE TABLE IF NOT EXISTS inventory_audit_log (
                id SERIAL PRIMARY KEY,
                action_type VARCHAR(100),
                product_barcode VARCHAR(50),
                quantity_before INTEGER,
                quantity_after INTEGER,
                reference_id INTEGER,
                reference_type VARCHAR(50),
                user_id INTEGER,
                branch_id INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Goods Received
            CREATE TABLE IF NOT EXISTS goods_received (
                id SERIAL PRIMARY KEY,
                po_id INTEGER,
                product_barcode VARCHAR(50),
                quantity_received INTEGER,
                quantity_packaging_units INTEGER,
                unit_cost DECIMAL(10,2),
                batch_number VARCHAR(100),
                expiry_date DATE,
                received_by INTEGER,
                invoice_number VARCHAR(100),
                branch_id INTEGER,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Create Stock Transfers Table
            CREATE TABLE IF NOT EXISTS stock_transfers (
                id SERIAL PRIMARY KEY,
                transfer_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                from_branch_id INTEGER,
                to_branch_id INTEGER,
                status VARCHAR(50) DEFAULT 'Pending',
                created_by INTEGER,
                tenant_id INT REFERENCES tenants(id) DEFAULT 1
            );
            
            -- Ensure stock_transfers has all columns
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS branch_id INT;
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS confirmed_by INT;
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMP;
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS notes TEXT;
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS from_location VARCHAR(255);
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS to_location VARCHAR(255);
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS items JSONB;

            CREATE TABLE IF NOT EXISTS stock_transfer_items (
                id SERIAL PRIMARY KEY,
                transfer_id INTEGER REFERENCES stock_transfers(id) ON DELETE CASCADE,
                product_barcode VARCHAR(50),
                quantity_sent INTEGER,
                quantity_received INTEGER,
                unit_cost DECIMAL(10,2),
                batch_number VARCHAR(100),
                expiry_date DATE
            );
            
            -- Price Lists
            CREATE TABLE IF NOT EXISTS price_lists (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                list_type VARCHAR(50),
                branch_id INTEGER,
                effective_date DATE,
                status VARCHAR(20) DEFAULT 'Active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS price_list_items (
                id SERIAL PRIMARY KEY,
                price_list_id INTEGER REFERENCES price_lists(id) ON DELETE CASCADE,
                product_barcode VARCHAR(50),
                markup_percentage DECIMAL(5,2),
                selling_price DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Ensure unique constraint on product_batches for ON CONFLICT clauses
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'product_batches_barcode_batch_branch_key') THEN
                    ALTER TABLE product_batches ADD CONSTRAINT product_batches_barcode_batch_branch_key UNIQUE (product_barcode, batch_number, branch_id);
                END IF;
            END $$;
        `);

        // --- TAX MANAGEMENT TABLES ---
        await pool.query(`
            CREATE TABLE IF NOT EXISTS tax_rules (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                rate DECIMAL(5,2) NOT NULL,
                branch_id INTEGER,
                status VARCHAR(20) DEFAULT 'Active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- Add branch_id column if it doesn't exist
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='tax_rules' AND column_name='branch_id') THEN
                    ALTER TABLE tax_rules ADD COLUMN branch_id INTEGER;
                END IF;
            END $$;

            -- Migration: Set branch_id = 1 for any legacy rules where it is currently NULL
            -- This prevents them from leaking into other portals/branches
            UPDATE tax_rules SET branch_id = 1 WHERE branch_id IS NULL;
        `);
        // MULTI-TENANT ISOLATION MIGRATIONS
        await pool.query(`
            ALTER TABLE users ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE branches ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE products ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE categories ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE expenses ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE shifts ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE refunds ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE customers ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;
            ALTER TABLE promotions ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;

            UPDATE users SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE branches SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE products SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE categories SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE suppliers SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE transactions SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE purchase_orders SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE expenses SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE shifts SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE refunds SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE customers SET tenant_id = 1 WHERE tenant_id IS NULL;
            UPDATE promotions SET tenant_id = 1 WHERE tenant_id IS NULL;
        `);

        // Create Default CEO User if not exists
        try {
            const ceoCheck = await pool.query("SELECT id FROM users WHERE email = 'ceo@footprint.com'");
            console.log('CEO check result:', ceoCheck.rows.length);
            if (ceoCheck.rows.length === 0) {
                const defaultPass = process.env.DEFAULT_ADMIN_PASS || 'ceo123';
                console.log('Creating CEO user...');
                const salt = await bcrypt.genSalt(10);
                const hash = await bcrypt.hash(defaultPass, salt);
                await pool.query(
                    "INSERT INTO users (name, email, password, role, store_location, status) VALUES ($1, $2, $3, $4, $5, 'Active')",
                    ['Chief Executive Officer', 'ceo@footprint.com', hash, 'ceo', 'Headquarters']
                );
                console.log('Default CEO user created: ceo@footprint.com');
            }
        } catch (ceoErr) {
            console.error('CEO creation error:', ceoErr);
        }

        // SaaS MULTI-TENANT ARCHITECTURE LOOP
        // Natively inject tenant_id global sandboxes across all 28 operational tables
        const saasTables = [
            'proforma_invoices', 'proforma_invoice_items', 'company_transactions',
            'sales_invoices', 'sales_invoice_items', 'product_batches', 'transactions',
            'refunds', 'customers', 'shifts', 'customer_payments', 'expenses',
            'activity_logs', 'system_settings', 'customer_ledger', 'shelf_inventory',
            'shelf_movements', 'stock_adjustments', 'stock_takes', 'stock_take_items',
            'reorder_alerts', 'inventory_audit_log', 'goods_received', 'stock_transfers',
            'stock_transfer_items', 'price_lists', 'price_list_items', 'tax_rules'
        ];
        
        for (const table of saasTables) {
            try {
                // Ensure table structure exists and forcefully map sandbox parameter
                await pool.query(`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS tenant_id INT REFERENCES tenants(id) DEFAULT 1;`);
                await pool.query(`UPDATE ${table} SET tenant_id = 1 WHERE tenant_id IS NULL;`);
                
                // RLS: Natively lock the database table to the current Express Session context mathematically
                await pool.query(`ALTER TABLE ${table} ENABLE ROW LEVEL SECURITY;`);
                await pool.query(`DROP POLICY IF EXISTS tenant_isolation_policy ON ${table};`);
                await pool.query(`
                    CREATE POLICY tenant_isolation_policy ON ${table}
                    FOR ALL
                    USING (
                        NULLIF(current_setting('app.current_tenant', true), '') IS NULL 
                        OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::int
                    );
                `);
                
                // Magic Mechanism: Missing API INSERT structures automatically inherit the Sandbox Default!
                await pool.query(`
                    ALTER TABLE ${table} ALTER COLUMN tenant_id 
                    SET DEFAULT COALESCE(NULLIF(current_setting('app.current_tenant', true), '')::int, 1);
                `);
            } catch (err) {
                // If the table doesn't exist yet, we can gracefully skip
                if (err.code !== '42P01') { 
                    console.error(`Error enforcing tenant_id on sandbox table ${table}:`, err.message);
                }
            }
        }

        console.log('Database schema checked/updated');
    } catch (err) {
        console.error('DB Init Error:', err);
    }
}
initDb();
console.log('initDb() called');

// Security middleware
app.use(security.securityHeaders);
// app.use(security.limiter);

// Session configuration
const sessionConfig = {
    store: new pgSession({
        pool: pool,
        tableName: 'user_sessions',
        createTableIfMissing: true,
        ttl: 24 * 60 * 60 // 24 hours in seconds
    }),
    secret: process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex'),
    resave: false,
    saveUninitialized: false,
    cookie: {
        secure: process.env.NODE_ENV === 'production',
        httpOnly: true,
        sameSite: process.env.NODE_ENV === 'production' ? 'none' : 'lax',
        maxAge: 24 * 60 * 60 * 1000, // 24 hours
        // Only set domain explicitly if COOKIE_DOMAIN env var is provided
        ...(process.env.COOKIE_DOMAIN ? { domain: process.env.COOKIE_DOMAIN } : {})
    },
    name: 'pos.sid' // Custom session cookie name
};

// Initialize session middleware
app.use(session(sessionConfig));

// CORS configuration
const corsOptions = {
    // In production without CORS_ORIGIN set, allow all origins (Vercel handles routing)
    origin: process.env.CORS_ORIGIN
        ? process.env.CORS_ORIGIN.split(',')
        : (process.env.NODE_ENV === 'production' ? true : 'http://localhost:3000'),
    credentials: true,
    optionsSuccessStatus: 200,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-CSRF-Token']
};

app.use(cors(corsOptions));
app.options(/.*/, cors(corsOptions)); // Enable pre-flight for all routes

// Body parser middleware
app.use(express.json({ limit: '50mb' })); // Limit JSON body size to allow PDF attachments
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Middleware to verify JWT token or Session (Hybrid approach)
const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    let token = authHeader && authHeader.split(' ')[1]; // Bearer <token>

    if (token === 'null' || token === 'undefined') token = null;
    
    // Helper to evaluate SaaS tenant boundaries securely before passing to API logic
    const enforceSaaSBoundary = (userPayload) => {
        if (!userPayload) return res.status(401).json({ message: 'Not authenticated' });
        
        if (userPayload.role) userPayload.role = userPayload.role.toLowerCase();
        req.user = userPayload;

        // CEOs and System Admins bypass the sandbox (Empty String Sandbox Bypass Native SQL mapped)
        const activeTenantSandbox = (userPayload.role === 'ceo' || userPayload.role === 'admin') 
            ? '' 
            : (userPayload.tenant_id ? userPayload.tenant_id.toString() : '1');
            
        // Engulf the remaining endpoint execution inside the Postgres RLS interceptor
        tenantContext.run(activeTenantSandbox, () => next());
    };

    if (token) {
        jwt.verify(token, JWT_SECRET, (err, user) => {
            if (err) return res.status(403).json({ message: 'Invalid or expired token' });
            enforceSaaSBoundary(user);
        });
    } else if (req.session && req.session.user) {
        enforceSaaSBoundary(req.session.user);
    } else if (req.session && req.session.companyUser) {
        enforceSaaSBoundary(req.session.companyUser);
    } else {
        res.status(401).json({ message: 'Not authenticated' });
    }
};

// Root Route - Redirect to login
app.get('/', (req, res) => {
    res.redirect('/login');
});

// Redirect trailing slash requests (e.g. /inventory/ -> /inventory)
app.use((req, res, next) => {
    if (req.path.substr(-1) === '/' && req.path.length > 1) {
        const query = req.url.slice(req.path.length);
        res.redirect(301, req.path.slice(0, -1) + query);
    } else {
        next();
    }
});

// Explicitly serve HTML pages to avoid directory conflicts
app.get('/inventory', (req, res) => {
    res.sendFile(path.join(__dirname, 'inventory.html'));
});
app.get('/promotions', (req, res) => {
    res.sendFile(path.join(__dirname, 'promotions.html'));
});
app.get('/login', (req, res) => {
    res.sendFile(path.join(__dirname, 'login.html'));
});
app.get('/dashboard', (req, res) => {
    res.sendFile(path.join(__dirname, 'dashboard.html'));
});
app.get('/ceo-portal', (req, res) => {
    res.sendFile(path.join(__dirname, 'ceo-portal.html'));
});
app.get('/company-portal', (req, res) => {
    res.sendFile(path.join(__dirname, 'company-portal.html'));
});
app.get('/tax-management', (req, res) => {
    res.sendFile(path.join(__dirname, 'tax-management.html'));
});
app.get('/open-shift', (req, res) => {
    res.sendFile(path.join(__dirname, 'open-shift.html'));
});
app.get('/sales-dashboard', (req, res) => {
    res.sendFile(path.join(__dirname, 'sales-dashboard.html'));
});
app.get('/profitability', (req, res) => {
    res.sendFile(path.join(__dirname, 'profitability.html'));
});
app.get('/pos-register', (req, res) => {
    res.sendFile(path.join(__dirname, 'pos-register.html'));
});
app.get('/credit-customers', (req, res) => {
    res.sendFile(path.join(__dirname, 'credit-customers.html'));
});
app.get('/reports', (req, res) => {
    res.sendFile(path.join(__dirname, 'reports.html'));
});
app.get('/settings', (req, res) => {
    res.sendFile(path.join(__dirname, 'settings.html'));
});
app.get('/bulk-upload', (req, res) => {
    res.sendFile(path.join(__dirname, 'bulk-upload.html'));
});
app.get('/categories', (req, res) => {
    res.sendFile(path.join(__dirname, 'categories.html'));
});
app.get('/close-shift', (req, res) => {
    res.sendFile(path.join(__dirname, 'close-shift.html'));
});
app.get('/customer-display', (req, res) => {
    res.sendFile(path.join(__dirname, 'customer-display.html'));
});
app.get('/customers', (req, res) => {
    res.sendFile(path.join(__dirname, 'customers.html'));
});
app.get('/forgot-password', (req, res) => {
    res.sendFile(path.join(__dirname, 'forgot-password.html'));
});
app.get('/locations', (req, res) => {
    res.sendFile(path.join(__dirname, 'locations.html'));
});
app.get('/payment', (req, res) => {
    res.sendFile(path.join(__dirname, 'payment.html'));
});
app.get('/pos', (req, res) => {
    res.sendFile(path.join(__dirname, 'pos.html'));
});
app.get('/profile', (req, res) => {
    res.sendFile(path.join(__dirname, 'profile.html'));
});
app.get('/purchase-orders', (req, res) => {
    res.sendFile(path.join(__dirname, 'purchase-orders.html'));
});
app.get('/receipt', (req, res) => {
    res.sendFile(path.join(__dirname, 'receipt.html'));
});
app.get('/reset-password', (req, res) => {
    res.sendFile(path.join(__dirname, 'reset-password.html'));
});
app.get('/suppliers', (req, res) => {
    res.sendFile(path.join(__dirname, 'suppliers.html'));
});
app.get('/transactions', (req, res) => {
    res.sendFile(path.join(__dirname, 'transactions.html'));
});

// Forgot Password Endpoint
app.post('/forgot-password', async (req, res) => {
    const { email } = req.body;
    try {
        const userResult = await pool.query('SELECT id, email, name FROM users WHERE LOWER(email) = LOWER($1)', [email]);
        if (userResult.rows.length === 0) {
            // To prevent email enumeration, we send a success response even if the user is not found.
            console.log(`Password reset attempt for non-existent user: ${email}`);
            return res.json({
                success: true,
                message: 'If an account with this email exists, a password reset link has been sent.'
            });
        }
        const user = userResult.rows[0];

        // Generate unique reset token
        const resetToken = crypto.randomBytes(32).toString('hex');
        const hashedToken = crypto.createHash('sha256').update(resetToken).digest('hex');

        const baseUrl = process.env.BASE_URL || `http://localhost:${port}`;
        const resetLink = `${baseUrl}/reset-password?token=${resetToken}`;

        // Store the hashed token and expiry in the database
        const expiry = new Date(Date.now() + 3600000); // Token expires in 1 hour
        await pool.query(
            'UPDATE users SET reset_token = $1, reset_token_expiry = $2 WHERE id = $3',
            [hashedToken, expiry, user.id]
        );

        // Nodemailer setup
        const transporter = nodemailer.createTransport({
            host: process.env.SMTP_HOST,
            port: process.env.SMTP_PORT,
            secure: process.env.SMTP_SECURE === 'true', // true for 465, false for other ports
            auth: {
                user: process.env.SMTP_USER,
                pass: process.env.SMTP_PASS
            }
        });

        // Email content
        const mailOptions = {
            from: `"Footprint Retail Store" <${process.env.ADMIN_EMAIL}>`,
            to: user.email,
            subject: 'Your Password Reset Request',
            html: `
                <p>Hello ${user.name},</p>
                <p>You requested a password reset for your Footprint Retail Store account.</p>
                <p>Please click the following link to reset your password. This link is valid for 1 hour:</p>
                <a href="${resetLink}" style="background-color: #1a2a6c; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px;">Reset Password</a>
                <p>If you did not request a password reset, please ignore this email.</p>
                <p>Thank you,<br>The Footprint Team</p>
            `
        };

        // Send email
        await transporter.sendMail(mailOptions);

        res.json({
            success: true,
            message: 'If an account with this email exists, a password reset link has been sent.'
        });

    } catch (error) {
        console.error('Forgot password error:', error);
        // Generic error to prevent leaking information
        res.status(500).json({ message: 'An error occurred while processing your request' });
    }
});

// Routes

// Login Route
app.post('/login', [
    body('email').isEmail(),
    body('password').isLength({ min: 6 })
], security.validateInput, async (req, res) => {
    const { email, password } = req.body;

    try {
        // Find user by email with branch/store and tenant information (Case Insensitive)
        const userResult = await pool.query(
            'SELECT id, name, email, password, role, store_id, store_location, status, tenant_id FROM users WHERE LOWER(email) = LOWER($1)',
            [email]
        );

        if (userResult.rows.length === 0) {
            return res.status(401).json({
                success: false,
                message: 'Invalid credentials'
            });
        }

        const user = userResult.rows[0];

        // Check if user is active
        if (user.status !== 'Active') {
            return res.status(403).json({
                success: false,
                message: 'Account is inactive. Please contact administrator.'
            });
        }

        // Verify password with bcrypt
        const validPassword = await bcrypt.compare(password, user.password);
        if (!validPassword) {
            return res.status(401).json({
                success: false,
                message: 'Invalid credentials'
            });
        }

        // Normalize role to lowercase for internal processing
        const normalizedRole = user.role.toLowerCase();

        // Set session
        req.session.user = {
            id: user.id,
            email: user.email,
            name: user.name,
            role: normalizedRole,
            tenant_id: user.tenant_id || 1,
            store_id: user.store_id || (normalizedRole === 'admin' || normalizedRole === 'ceo' ? 1 : null), // Ensure High-level users have a default store context
            store_location: user.store_location || (normalizedRole === 'admin' || normalizedRole === 'ceo' ? 'Headquarters' : null) // Ensure High-level users have a default view context
        };

        // Log Login Activity
        await logActivity(req, 'LOGIN', { email: user.email, role: user.role });

        // Generate JWT Token
        const token = jwt.sign(req.session.user, JWT_SECRET, { expiresIn: '7d' });

        // Set redirect path based on role
        const userRole = user.role.toLowerCase();
        let redirectTo = '/open-shift';
        if (userRole === 'admin' || userRole === 'manager') redirectTo = '/dashboard';
        if (userRole === 'ceo') redirectTo = '/ceo-portal';

        // Check for active shift for cashiers to prevent flash
        if (userRole === 'cashier' || userRole === 'teller') {
            const activeShift = await pool.query('SELECT id FROM shifts WHERE user_id = $1 AND end_time IS NULL', [user.id]);
            if (activeShift.rows.length > 0) {
                redirectTo = '/pos';
            }
        }

        res.json({
            success: true,
            token: token,
            redirectTo: redirectTo.replace(/^\//, ''),
            user: {
                id: user.id,
                email: user.email,
                name: user.name,
                role: user.role,
                store_id: user.store_id,
                store_location: user.store_location
            }
        });

    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ success: false, message: 'An error occurred during login' });
    }
});

// Session Check Endpoint
app.get('/api/session', authenticateToken, (req, res) => {
    if (req.user) {
        res.json({ user: req.user });
    } else {
        res.status(401).json({ message: 'Not authenticated' });
    }
});

// Logout Endpoint
app.post('/api/logout', async (req, res) => {
    await logActivity(req, 'LOGOUT');
    req.session.destroy((err) => {
        if (err) {
            return res.status(500).json({ message: 'Could not log out' });
        }
        res.clearCookie('pos.sid');
        res.json({ message: 'Logged out successfully' });
    });
});

// Company Login Endpoint
app.post('/api/company/login', [
    body('email').isEmail(),
    body('password').isLength({ min: 6 })
], security.validateInput, async (req, res) => {
    const { email, password } = req.body;

    try {
        // Find company user by email
        const userResult = await pool.query(
            'SELECT id, company_name, email, password, contact_person, phone, address, status FROM company_users WHERE LOWER(email) = LOWER($1)',
            [email]
        );

        if (userResult.rows.length === 0) {
            return res.status(401).json({
                success: false,
                message: 'Invalid credentials'
            });
        }

        const user = userResult.rows[0];

        // Check if user is active
        if (user.status !== 'Active') {
            return res.status(403).json({
                success: false,
                message: 'Account is inactive. Please contact administrator.'
            });
        }

        // Verify password with bcrypt
        const validPassword = await bcrypt.compare(password, user.password);
        if (!validPassword) {
            return res.status(401).json({
                success: false,
                message: 'Invalid credentials'
            });
        }

        // Set company session
        req.session.companyUser = {
            id: user.id,
            email: user.email,
            company_name: user.company_name,
            contact_person: user.contact_person,
            phone: user.phone,
            address: user.address,
            store_id: user.id + 1000, // Use a unique offset to avoid collision with physical branches
            type: 'company'
        };

        // Generate JWT Token
        const token = jwt.sign(req.session.companyUser, JWT_SECRET, { expiresIn: '7d' });

        res.json({
            success: true,
            token: token,
            user: {
                id: user.id,
                email: user.email,
                company_name: user.company_name,
                contact_person: user.contact_person,
                phone: user.phone,
                address: user.address,
                store_id: user.id + 1000,
                type: 'company'
            }
        });

    } catch (error) {
        console.error('Company login error:', error);
        res.status(500).json({ success: false, message: 'An error occurred during login' });
    }
});

// Company Session Check Endpoint
app.get('/api/company/session', (req, res) => {
    if (req.session.companyUser) {
        res.json({ user: req.session.companyUser });
    } else {
        res.status(401).json({ message: 'Not authenticated' });
    }
});

// Company Logout Endpoint
app.post('/api/company/logout', async (req, res) => {
    req.session.destroy((err) => {
        if (err) {
            return res.status(500).json({ message: 'Could not log out' });
        }
        res.clearCookie('pos.sid');
        res.json({ message: 'Logged out successfully' });
    });
});

// ============ USER MANAGEMENT ENDPOINTS ============

// Get all users
app.get('/api/users', authenticateToken, async (req, res) => {
    try {
        let query = `
            SELECT id, username, name as "fullName", employee_id as "employeeId", 
                   phone, email, role, store_location as store, status, created_at 
            FROM users 
        `;
        const params = [];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_location) {
            query += ` WHERE store_location = $1`;
            params.push(req.user.store_location);
        }

        query += ` ORDER BY created_at DESC`;

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Create user
app.post('/api/users', authenticateToken, async (req, res) => {
    const { username, fullName, employeeId, phone, email, role, store, password } = req.body;

    try {
        const userRole = req.user.role;
        if (userRole !== 'admin' && userRole !== 'manager' && userRole !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });

        // Resolve store_id from branches table to ensure correct settings/VAT linkage
        let storeId = null;
        if (store) {
            const branchRes = await pool.query('SELECT id FROM branches WHERE location = $1 OR name = $1', [store]);
            if (branchRes.rows.length > 0) storeId = branchRes.rows[0].id;
        }

        // Hash password
        const salt = await bcrypt.genSalt(10);
        const hashedPassword = await bcrypt.hash(password, salt);

        await pool.query(`
            INSERT INTO users (username, name, employee_id, phone, email, role, store_location, store_id, password, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'Active')
        `, [username, fullName, employeeId, phone, email.toLowerCase(), role.toLowerCase(), store, storeId, hashedPassword]);

        await logActivity(req, 'CREATE_USER', { username, role, store });

        res.json({ success: true, message: 'User created successfully' });
    } catch (err) {
        console.error(err);

        if (err.code === '23505') { // Unique violation
            return res.status(400).json({ message: 'Username, Email or Employee ID already exists' });
        }
        res.status(500).json({ message: 'Error creating user' });
    }
});

// Update user
app.put('/api/users/:id', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const userRole = req.user.role;
    if (userRole !== 'admin' && userRole !== 'manager' && userRole !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    const { fullName, username, email, phone, role, store, status } = req.body;

    try {
        // Resolve store_id
        let storeId = null;
        if (store) {
            const branchRes = await pool.query('SELECT id FROM branches WHERE location = $1 OR name = $1', [store]);
            if (branchRes.rows.length > 0) storeId = branchRes.rows[0].id;
        }

        // Fetch current status to determine action type
        const userRes = await pool.query('SELECT status FROM users WHERE id = $1', [id]);
        const currentStatus = userRes.rows[0]?.status;

        await pool.query(`
            UPDATE users 
            SET name = $1, username = $2, email = $3, phone = $4, role = $5, store_location = $6, store_id = $7, status = $8
            WHERE id = $9
        `, [fullName, username, email, phone, role.toLowerCase(), store, storeId, status, id]);

        let action = 'UPDATE_USER';
        if (status && currentStatus !== status) {
            if (status === 'Inactive') action = 'DEACTIVATE_USER';
            else if (status === 'Active') action = 'ACTIVATE_USER';
        }

        await logActivity(req, action, { id, username, role, status, previousStatus: currentStatus });

        res.json({ success: true, message: 'User updated successfully' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error updating user' });
    }
});

// Delete user (Permanent delete)
app.delete('/api/users/:id', authenticateToken, async (req, res) => {
    const { id } = req.params;
    try {
        await pool.query("DELETE FROM users WHERE id = $1", [id]);
        await logActivity(req, 'DELETE_USER', { id });
        res.json({ success: true, message: 'User permanently deleted' });
    } catch (err) {
        console.error(err);
        if (err.code === '23503') {
            // If cannot delete due to history, anonymize the user instead
            const timestamp = Date.now();
            const deletedEmail = `deleted_${id}_${timestamp}@void`;
            const deletedUser = `del_${id}_${timestamp}`;

            await pool.query(`
                UPDATE users 
                SET name = $1, username = $2, email = $3, phone = NULL, 
                    employee_id = NULL, status = 'Deleted', password = $4
                WHERE id = $5
            `, [`Former Staff (${id})`, deletedUser, deletedEmail, crypto.randomBytes(16).toString('hex'), id]);
            await logActivity(req, 'DELETE_USER_ANONYMIZED', { id });

            return res.json({ success: true, message: 'User info cleared & access revoked (History preserved)' });
        }
        res.status(500).json({ message: 'Error deleting user' });
    }
});

// Reset Password
app.post('/api/users/:id/reset-password', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const { password } = req.body;

    try {
        const salt = await bcrypt.genSalt(10);
        const hashedPassword = await bcrypt.hash(password, salt);

        await pool.query("UPDATE users SET password = $1 WHERE id = $2", [hashedPassword, id]);
        await logActivity(req, 'RESET_PASSWORD', { userId: id });
        res.json({ success: true, message: 'Password reset successfully' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error resetting password' });
    }
});

// Update Password (Self)
app.post('/api/update-password', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    const { currentPassword, newPassword } = req.body;

    try {
        const result = await pool.query('SELECT password FROM users WHERE id = $1', [req.user.id]);
        if (result.rows.length === 0) return res.status(404).json({ message: 'User not found' });

        const valid = await bcrypt.compare(currentPassword, result.rows[0].password);
        if (!valid) return res.status(400).json({ message: 'Incorrect current password' });

        const salt = await bcrypt.genSalt(10);
        const hash = await bcrypt.hash(newPassword, salt);

        await pool.query('UPDATE users SET password = $1 WHERE id = $2', [hash, req.user.id]);
        await logActivity(req, 'UPDATE_PASSWORD_SELF');
        res.json({ success: true, message: 'Password updated successfully' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Dashboard Data Endpoint
app.get('/api/dashboard', authenticateToken, async (req, res) => {
    if (!req.user || (req.user.role !== 'admin' && req.user.role !== 'manager' && req.user.role !== 'ceo')) {
        return res.status(403).json({ message: 'Unauthorized' });
    }

    try {
        // Fetch fresh user info from DB to ensure we have the latest store assignment
        const userRes = await pool.query('SELECT store_id, store_location FROM users WHERE id = $1', [req.user.id]);
        const dbUser = userRes.rows[0];

        const user = req.user;
        const storeLocation = dbUser?.store_location || user.store_location;

        // Filter parameters
        let locationFilter = "AND t.tenant_id = $" + (storeLocation ? "2" : "1");
        let queryParams = storeLocation ? [storeLocation, req.user.tenant_id] : [req.user.tenant_id];

        if (storeLocation) {
            locationFilter = "AND t.store_location = $1 AND t.tenant_id = $2";
        }

        // Total Sales (Sum of transactions today)
        const salesRes = await pool.query(
            `SELECT COALESCE(SUM(
                CASE WHEN t.is_return THEN -1 ELSE 1 END * 
                (SELECT COALESCE(SUM((item->>'qty')::numeric * (item->>'price')::numeric), 0) FROM jsonb_array_elements(t.items) as item)
             ), 0) as total 
             FROM transactions t WHERE LOWER(t.status) = 'completed' AND t.created_at >= CURRENT_DATE ${locationFilter}`,
            queryParams
        );
        // Total Transactions
        const txnsRes = await pool.query(
            `SELECT COUNT(*) as count FROM transactions t WHERE LOWER(t.status) = 'completed' AND t.is_return = FALSE AND t.created_at >= CURRENT_DATE ${locationFilter}`,
            queryParams
        );

        // Low Stock (stock > 0 AND stock <= reorder_level — excludes out-of-stock items)
        let stockCount = 0;
        if (storeLocation) {
            // Generic branch lookup — fetch BOTH name and location from branches table
            // stock_levels keys may be stored under the branch name, location, or a historical variant
            const branchLookup = await pool.query(
                'SELECT name, location FROM branches WHERE name = $1 OR location = $1 LIMIT 1',
                [storeLocation]
            );
            const branchRow = branchLookup.rows[0];

            // Build comprehensive alias set from all known variants
            const aliases = new Set(
                [storeLocation, branchRow?.name, branchRow?.location].filter(Boolean)
            );

            // Fetch all products (lightweight — only fields needed for the count)
            const productsRes = await pool.query(
                'SELECT stock_levels, reorder_level FROM products WHERE tenant_id = $1',
                [req.user.tenant_id]
            );

            // Count products that are low stock or out of stock for this branch
            for (const product of productsRes.rows) {
                const levels = typeof product.stock_levels === 'string'
                    ? JSON.parse(product.stock_levels || '{}')
                    : (product.stock_levels || {});
                let branchStock = 0;
                for (const alias of aliases) {
                    branchStock += parseInt(levels[alias]) || 0;
                }
                const reorder = product.reorder_level ?? 10;
                if (branchStock <= reorder) stockCount++;
            }
        } else {
            // Admin/CEO global view — sum total stock across all branches per product
            const stockRes = await pool.query(
                `SELECT COUNT(*) as count FROM products 
                 WHERE COALESCE((
                     SELECT SUM(CAST(value AS INTEGER)) 
                     FROM jsonb_each_text(COALESCE(stock_levels, '{}'))
                 ), COALESCE(stock, 0)) <= COALESCE(reorder_level, 10) 
                 AND tenant_id = $1`,
                [req.user.tenant_id]
            );
            stockCount = parseInt(stockRes.rows[0].count);
        }

        // Recent Transactions
        let recentWhere = "WHERE LOWER(t.status) = 'completed' AND t.is_return = FALSE AND t.tenant_id = $" + (storeLocation ? "2" : "1");
        let recentParams = storeLocation ? [storeLocation, req.user.tenant_id] : [req.user.tenant_id];

        if (storeLocation) {
            recentWhere += " AND t.store_location = $1";
        }

        const recentRes = await pool.query(
            `SELECT t.*, 
             (CASE WHEN t.is_return THEN -1 ELSE 1 END * 
              (SELECT COALESCE(SUM((item->>'qty')::numeric * (item->>'price')::numeric), 0) FROM jsonb_array_elements(t.items) as item)
             ) as total_amount,
             CASE WHEN t.is_return THEN 'RETURN' ELSE 'SALE' END as type, u.name as cashier_name 
             FROM transactions t 
             LEFT JOIN users u ON t.user_id = u.id 
             ${recentWhere}
             ORDER BY t.created_at DESC LIMIT 5`,
            recentParams
        );

        // Cashier Performance Stats
        let cashierWhere = "AND t.tenant_id = $" + (storeLocation ? "2" : "1");
        if (storeLocation) {
            cashierWhere += " AND t.store_location = $1";
        }
        
        const cashierRes = await pool.query(
            `SELECT CONCAT(u.name, ' (@', u.username, ' - ', u.role, ')') as cashier, 
                    COUNT(t.id) as transaction_count, 
                    SUM(t.total_amount) as total_sales
             FROM transactions t
             LEFT JOIN users u ON t.user_id = u.id
             WHERE LOWER(t.status) = 'completed' AND t.created_at >= CURRENT_DATE ${cashierWhere}
             GROUP BY u.name, u.username, u.role
             ORDER BY total_sales DESC LIMIT 5`,
            storeLocation ? [storeLocation, req.user.tenant_id] : [req.user.tenant_id]
        );

        res.json({
            stats: {
                totalSales: parseFloat(salesRes.rows[0].total),
                totalTransactions: parseInt(txnsRes.rows[0].count),
                lowStockCount: stockCount
            },
            recentTransactions: recentRes.rows,
            cashierStats: cashierRes.rows,
            branch: storeLocation || 'All Branches'
        });
        await logActivity(req, 'VIEW_DASHBOARD');
    } catch (err) {
        console.error('Dashboard error:', err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Shift Management Endpoints
app.get('/api/shifts/active', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    try {
        const result = await pool.query(
            "SELECT * FROM shifts WHERE user_id = $1 AND end_time IS NULL",
            [req.user.id]
        );
        res.json({ active: result.rows.length > 0, shift: result.rows[0] });
    } catch (err) {
        res.status(500).json({ message: 'Server error' });
    }
});

app.post('/api/shifts/open', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    const { startCash, notes } = req.body;

    try {
        await pool.query(
            "INSERT INTO shifts (user_id, start_cash, notes) VALUES ($1, $2, $3)",
            [req.user.id, startCash || 0, notes]
        );
        await logActivity(req, 'OPEN_SHIFT', { startCash });
        res.json({ success: true });
    } catch (err) {
        console.error('Shift open error:', err);
        res.status(500).json({ message: 'Server error' });
    }
});

app.get('/api/shifts/summary', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    try {
        // Get active shift
        const shiftRes = await pool.query(
            "SELECT * FROM shifts WHERE user_id = $1 AND end_time IS NULL",
            [req.user.id]
        );

        if (shiftRes.rows.length === 0) {
            return res.status(404).json({ message: 'No active shift found' });
        }

        const shift = shiftRes.rows[0];

        // Calculate sales for this shift
        const salesRes = await pool.query(
            `SELECT payment_method, COALESCE(SUM(total_amount), 0) as total 
             FROM transactions 
             WHERE user_id = $1 AND created_at >= $2 AND status = 'completed'
             GROUP BY payment_method`,
            [req.user.id, shift.start_time]
        );

        let cashSales = 0;
        let momoSales = 0;
        let cardSales = 0;

        salesRes.rows.forEach(row => {
            const method = (row.payment_method || '').toLowerCase();
            const amount = parseFloat(row.total);
            if (method === 'cash') cashSales += amount;
            else if (method === 'momo' || method.includes('mobile')) momoSales += amount;
            else if (method === 'card') cardSales += amount;
        });

        const startCash = parseFloat(shift.start_cash || 0);
        const expectedCash = startCash + cashSales;

        res.json({ startCash, cashSales, momoSales, cardSales, expectedCash });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

app.post('/api/shifts/close', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });
    const { endCash, notes } = req.body;
    try {
        const result = await pool.query(
            "UPDATE shifts SET end_time = NOW(), end_cash = $1, notes = $2, status = 'closed' WHERE user_id = $3 AND end_time IS NULL RETURNING id",
            [endCash, notes, req.user.id]
        );
        await logActivity(req, 'CLOSE_SHIFT', { endCash });
        if (result.rowCount === 0) return res.status(400).json({ message: 'No active shift to close' });
        res.json({ success: true });
    } catch (err) { console.error(err); res.status(500).json({ message: 'Server error' }); }
});

// Get shift history for reports
app.get('/api/shifts/history', authenticateToken, async (req, res) => {
    try {
        const isRestricted = req.user.role !== 'admin' && req.user.role !== 'ceo';
        const userBranch = req.user.store_location || 'Main Warehouse';

        let query = `
            SELECT s.*, u.name as user_name, u.store_location as branch,
                   COALESCE(SUM(t.total_amount), 0) as total_sales
            FROM shifts s
            LEFT JOIN users u ON s.user_id = u.id
            LEFT JOIN transactions t ON t.user_id = s.user_id 
                AND t.created_at >= s.start_time 
                AND (s.end_time IS NULL OR t.created_at <= s.end_time)
                AND t.status = 'completed'
        `;
        let params = [];

        // Filter by branch for non-admin/CEO users
        if (isRestricted) {
            query += ` WHERE u.store_location = $1`;
            params.push(userBranch);
        }

        query += ` GROUP BY s.id, u.name, u.store_location ORDER BY s.start_time DESC`;

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) {
        console.error('Shift history error:', err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Promotions Endpoints
app.get('/api/promotions', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });
    try {
        let query;
        let params = [];

        // Filter by branch for non-CEO/Admin users if they are assigned to a store
        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            // For branch users, show only their branch's usage in 'total_discounted'
            query = `
                SELECT p.code, p.discount_percentage, p.created_at, p.branch_id,
                       COALESCE(b.name, 'Global') as branch_name,
                       COALESCE(pu.total_discounted, 0) as total_discounted
                FROM promotions p
                LEFT JOIN branches b ON p.branch_id = b.id
                LEFT JOIN promotion_usage pu ON p.code = pu.promotion_code AND pu.branch_id = $1
                WHERE p.tenant_id = $2 AND p.branch_id = $1
                ORDER BY p.created_at DESC`;
            params.push(req.user.store_id, req.user.tenant_id);
        } else {
            // For Admin/CEO, show global total_discounted
            query = `
                SELECT p.*, COALESCE(b.name, 'Global') as branch_name 
                FROM promotions p
                LEFT JOIN branches b ON p.branch_id = b.id
                WHERE p.tenant_id = $1
                ORDER BY p.created_at DESC`;
            params.push(req.user.tenant_id);
        }

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

app.post('/api/promotions', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });
    const { code, discount } = req.body;
    const branchId = req.user.store_id; // Capture the branch ID of the creator

    try {
        await pool.query(
            'INSERT INTO promotions (code, discount_percentage, branch_id, tenant_id) VALUES ($1, $2, $3, $4)',
            [code, discount, branchId, req.user.tenant_id]
        );
        await logActivity(req, 'CREATE_PROMOTION', { code, discount, branchId, tenantId: req.user.tenant_id });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error creating promotion' });
    }
});

app.delete('/api/promotions/:code', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });
    try {
        let query = 'DELETE FROM promotions WHERE code = $1 AND tenant_id = $2';
        let params = [req.params.code, req.user.tenant_id];

        // Ensure users can only delete promotions from their own branch (Strict Ownership)
        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' AND branch_id = $3'; // Managers cannot delete Global promotions
            params.push(req.user.store_id);
        }

        const result = await pool.query(query, params);
        if (result.rowCount === 0) {
            return res.status(404).json({ message: 'Promotion not found or access denied' });
        }

        await logActivity(req, 'DELETE_PROMOTION', { code: req.params.code });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error deleting promotion' });
    }
});

app.get('/api/promotions/validate/:code', authenticateToken, async (req, res) => {
    try {
        let query = 'SELECT * FROM promotions WHERE code = $1 AND tenant_id = $2';
        let params = [req.params.code, req.user.tenant_id];

        // Validate branch applicability
        if (req.user && req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' AND branch_id = $3';
            params.push(req.user.store_id);
        }

        const result = await pool.query(query, params);
        if (result.rows.length > 0) {
            res.json(result.rows[0]);
        } else {
            res.status(404).json({ message: 'Invalid promo code' });
        }
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Products Endpoints
app.get('/products', authenticateToken, async (req, res) => {
    try {
        const userBranch = req.user.store_location;
        const isRestricted = req.user.role !== 'admin' && req.user.role !== 'ceo';

        // Ensure all products have stock_levels initialized
        await pool.query(`
            UPDATE products 
            SET stock_levels = jsonb_build_object('Main Warehouse', COALESCE(stock, 0))
            WHERE stock_levels IS NULL OR stock_levels = '{}'::jsonb
        `);

        // Support Pagination & Search parameters
        const page = Math.max(1, parseInt(req.query.page) || 1);
        const limitParam = req.query.limit;
        const limit = limitParam === 'all' ? null : Math.max(1, parseInt(limitParam) || 20);
        const offset = limit ? (page - 1) * limit : 0;
        const search = req.query.search ? req.query.search.trim() : null;

        let queryParams = [req.user.tenant_id];
        let whereClause = `WHERE tenant_id = $1`;

        if (search) {
            queryParams.push(`%${search}%`);
            whereClause += ` AND (name ILIKE $2 OR barcode ILIKE $2 OR category ILIKE $2)`;
        }

        let limitClause = '';
        if (limit !== null) {
            queryParams.push(limit);
            limitClause = `LIMIT $${queryParams.length}`;
            queryParams.push(offset);
            limitClause += ` OFFSET $${queryParams.length}`;
        }

        // Get products and ensure total stock matches sum of location stocks
        const result = await pool.query(`
            SELECT 
                *,
                COUNT(*) OVER() AS total_count,
                COALESCE(
                    (SELECT SUM(CAST(value AS INTEGER)) 
                     FROM jsonb_each_text(COALESCE(stock_levels, '{}'))),
                    0
                ) as calculated_total
            FROM products 
            ${whereClause}
            ORDER BY name
            ${limitClause}
        `, queryParams);

        const totalItems = result.rows.length > 0 ? parseInt(result.rows[0].total_count) : 0;
        const totalPages = limit ? Math.ceil(totalItems / limit) : 1;

        // Process rows - recalculate stock from stock_levels to keep in sync
        const rows = result.rows.map(p => {
            if (isRestricted && userBranch) {
                // Branch View: Show only stock for this branch
                const levels = p.stock_levels || {};
                const levelsObj = typeof levels === 'string' ? JSON.parse(levels) : levels;
                let bStock = 0;
                const poolKeys = new Set([userBranch]);
                if (userBranch.toLowerCase().includes('dzorwulu')) { poolKeys.add('Dzorwulu'); poolKeys.add('DZORWULU'); }
                if (userBranch.toLowerCase().includes('lakeside')) { poolKeys.add('Lakeside'); poolKeys.add('LAKESIDE'); }
                for (const key of poolKeys) bStock += parseInt(levelsObj[key]) || 0;
                p.stock = bStock;
            } else {
                // Admin/CEO View: Show total stock
                if (p.stock_levels && typeof p.stock_levels === 'object') {
                    p.stock = p.calculated_total;
                } else if (!p.stock_levels) {
                    p.stock_levels = { 'Main Warehouse': p.stock || 0 };
                }
            }
            delete p.calculated_total; // Remove the helper column
            return p;
        });

        // Pass Pagination Meta Data via Headers
        res.setHeader('X-Total-Count', totalItems);
        res.setHeader('X-Total-Pages', totalPages);
        res.setHeader('X-Current-Page', page);

        await logActivity(req, 'VIEW_INVENTORY_LIST');
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// API-prefixed routes for front-end compatibility
app.get('/api/products', authenticateToken, async (req, res) => {
    try {
        const userBranch = req.user.store_location;
        const isRestricted = req.user.role !== 'admin' && req.user.role !== 'ceo';

        // Ensure all products have stock_levels initialized
        await pool.query(`
            UPDATE products 
            SET stock_levels = jsonb_build_object('Main Warehouse', COALESCE(stock, 0))
            WHERE stock_levels IS NULL OR stock_levels = '{}'::jsonb
        `);

        // Support Pagination & Search parameters
        const page = Math.max(1, parseInt(req.query.page) || 1);
        const limitParam = req.query.limit;
        const limit = limitParam === 'all' ? null : Math.max(1, parseInt(limitParam) || 20);
        const offset = limit ? (page - 1) * limit : 0;
        const search = req.query.search ? req.query.search.trim() : null;

        let queryParams = [req.user.tenant_id || 1];
        let whereClause = `WHERE tenant_id = $1`;

        if (search) {
            queryParams.push(`%${search}%`);
            whereClause += ` AND (name ILIKE $2 OR barcode ILIKE $2 OR category ILIKE $2)`;
        }

        let limitClause = '';
        if (limit !== null) {
            queryParams.push(limit);
            limitClause = `LIMIT $${queryParams.length}`;
            queryParams.push(offset);
            limitClause += ` OFFSET $${queryParams.length}`;
        }

        const result = await pool.query(`
            SELECT 
                *,
                COUNT(*) OVER() AS total_count,
                COALESCE(
                    (SELECT SUM(CAST(value AS INTEGER)) 
                     FROM jsonb_each_text(COALESCE(stock_levels, '{}'))),
                    0
                ) as calculated_total
            FROM products 
            ${whereClause}
            ORDER BY name
            ${limitClause}
        `, queryParams);

        const totalItems = result.rows.length > 0 ? parseInt(result.rows[0].total_count) : 0;
        const totalPages = limit ? Math.ceil(totalItems / limit) : 1;

        const rows = result.rows.map(p => {
            if (isRestricted && userBranch) {
                const levels = p.stock_levels || {};
                const levelsObj = typeof levels === 'string' ? JSON.parse(levels) : levels;
                let bStock = 0;
                const poolKeys = new Set([userBranch]);
                if (userBranch.toLowerCase().includes('dzorwulu')) { poolKeys.add('Dzorwulu'); poolKeys.add('DZORWULU'); }
                if (userBranch.toLowerCase().includes('lakeside')) { poolKeys.add('Lakeside'); poolKeys.add('LAKESIDE'); }
                for (const key of poolKeys) bStock += parseInt(levelsObj[key]) || 0;
                p.stock = bStock;
            } else {
                if (p.stock_levels && typeof p.stock_levels === 'object') {
                    p.stock = p.calculated_total;
                } else if (!p.stock_levels) {
                    p.stock_levels = { 'Main Warehouse': p.stock || 0 };
                }
            }
            delete p.calculated_total;
            return p;
        });

        // Pass Pagination Meta Data via Headers
        res.setHeader('X-Total-Count', totalItems);
        res.setHeader('X-Total-Pages', totalPages);
        res.setHeader('X-Current-Page', page);

        await logActivity(req, 'VIEW_INVENTORY_LIST');
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Get product by barcode (for returns)
app.get('/api/products/barcode/:barcode', authenticateToken, async (req, res) => {
    try {
        const { barcode } = req.params;

        const result = await pool.query(`
            SELECT p.*,
                COALESCE(
                    (SELECT stock_levels::jsonb ->> $2 
                     FROM products p2 
                     WHERE p2.id = p.id AND stock_levels::jsonb ? $2),
                    p.stock::text
                ) as stock_for_branch
            FROM products p
            WHERE (p.barcode = $1 OR p.group_barcode = $1) AND p.tenant_id = $3
            LIMIT 1
        `, [barcode, req.user.store_location || 'Main Warehouse', req.user.tenant_id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ message: 'Product not found' });
        }

        res.json(result.rows[0]);
    } catch (err) {
        console.error('Error fetching product by barcode:', err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Full product endpoint with branch-specific stock_for_branch field
app.get('/api/products/full', authenticateToken, async (req, res) => {
    try {
        // Refresh user store info from DB to ensure accuracy
        const userRes = await pool.query('SELECT store_id, store_location FROM users WHERE id = $1', [req.user.id]);
        const dbUser = userRes.rows[0] || {};

        let userBranch = dbUser.store_location || req.user.store_location || 'Main Warehouse';
        const isRestricted = req.user.role !== 'admin' && req.user.role !== 'ceo';

        // Core mapping: Always link external company portals directly to the primary Dzorwulu warehouse
        if (req.user.role === 'business_client' || req.user.type === 'company') {
            userBranch = 'Novelty The Sparrow Ent Dzorwulu';
        }

        // Ensure all products have stock_levels initialized
        await pool.query(`
            UPDATE products 
            SET stock_levels = jsonb_build_object('Main Warehouse', COALESCE(stock, 0))
            WHERE stock_levels IS NULL OR stock_levels = '{}'::jsonb
        `);

        // Support Pagination & Search parameters
        const page = Math.max(1, parseInt(req.query.page) || 1);
        const limitParam = req.query.limit;
        const lowStockOnly = req.query.lowStock === 'true';
        // When filtering for low stock, we MUST fetch ALL products first (no DB-level pagination)
        // because low-stock items may be on any page and we can only filter after computing branch stock
        const limit = (lowStockOnly || limitParam === 'all') ? null : Math.max(1, parseInt(limitParam) || 20);
        const requestedLimit = limitParam === 'all' ? null : Math.max(1, parseInt(limitParam) || 20);
        const offset = limit ? (page - 1) * limit : 0;
        const search = req.query.search ? req.query.search.trim() : null;

        let queryParams = [req.user.tenant_id || 1];
        let whereClause = `WHERE tenant_id = $1`;

        if (search) {
            queryParams.push(`%${search}%`);
            whereClause += ` AND (name ILIKE $2 OR barcode ILIKE $2 OR category ILIKE $2)`;
        }

        let limitClause = '';
        if (limit !== null) {
            queryParams.push(limit);
            limitClause = `LIMIT $${queryParams.length}`;
            queryParams.push(offset);
            limitClause += ` OFFSET $${queryParams.length}`;
        }

        // Execute Fetch — no LIMIT when lowStock=true so we get all products to filter
        const result = await pool.query(`
            SELECT 
                *,
                COUNT(*) OVER() AS total_count,
                COALESCE(
                    (SELECT SUM(CAST(value AS INTEGER)) 
                     FROM jsonb_each_text(COALESCE(stock_levels, '{}'))),
                    0
                ) as calculated_total,
                (SELECT COUNT(*) FROM product_batches pb WHERE pb.product_barcode = products.barcode AND pb.status = 'Active') as batch_count,
                (SELECT MIN(expiry_date) FROM product_batches pb WHERE pb.product_barcode = products.barcode AND pb.status = 'Active') as next_expiry
            FROM products 
            ${whereClause}
            ORDER BY name
            ${limitClause}
        `, queryParams);

        // Extract total_count from window function
        const totalItems = result.rows.length > 0 ? parseInt(result.rows[0].total_count) : 0;

        // Map branch name for stock lookup (match how stock_levels keys are stored)
        // Query BOTH name and location from branches — keys may be stored under either field
        const branchRes = await pool.query(
            'SELECT id, name, location FROM branches WHERE name = $1 OR location = $1',
            [userBranch]
        );
        const branchRow = branchRes.rows[0];
        const branchStockKey = branchRow?.name || userBranch;

        // Build generic alias set: userBranch + canonical name + canonical location
        // This covers all known key variants without hardcoding branch names
        const branchKeySet = new Set(
            [userBranch, branchStockKey, branchRow?.location].filter(Boolean)
        );

        const rows = result.rows.map(p => {
            const levels = p.stock_levels || {};
            const levelsObj = typeof levels === 'string' ? JSON.parse(levels) : levels;

            // Sum stock across ALL known key aliases for this branch
            let branchStock = 0;
            for (const key of branchKeySet) {
                branchStock += parseInt(levelsObj[key]) || 0;
            }

            // Add stock_for_branch field for frontend
            p.stock_for_branch = branchStock;

            if ((isRestricted && userBranch) || req.user.role === 'business_client') {
                // Branch View or Company Portal: Show only stock for this branch
                p.stock = branchStock;
            } else {
                // Admin/CEO View: Show total stock
                if (p.stock_levels && typeof p.stock_levels === 'object') {
                    p.stock = p.calculated_total;
                }
            }
            delete p.calculated_total;
            return p;
        });

        // Apply lowStock filter AFTER branch stock is resolved for ALL products
        // Then re-apply pagination to the filtered set
        let finalRows = rows;
        let finalTotal = totalItems;

        if (lowStockOnly) {
            // Filter for low stock or out of stock (branch_stock <= reorder_level)
            const allLowStock = rows.filter(p => {
                const s = (p.stock_for_branch !== undefined && p.stock_for_branch !== null)
                    ? p.stock_for_branch
                    : (p.stock || 0);
                const reorder = (p.reorder_level !== null && p.reorder_level !== undefined)
                    ? p.reorder_level
                    : 10;
                return s <= reorder;
            });
            finalTotal = allLowStock.length;
            // Re-paginate the filtered results
            const startIdx = (page - 1) * (requestedLimit || finalTotal);
            finalRows = requestedLimit ? allLowStock.slice(startIdx, startIdx + requestedLimit) : allLowStock;
        }

        // Set header with branch key for frontend reference
        res.setHeader('X-Branch-Stock-Key', branchStockKey);
        
        // Pass Pagination Meta Data via Headers (filtered counts when lowStock=true)
        res.setHeader('X-Total-Count', finalTotal);
        res.setHeader('X-Total-Pages', requestedLimit ? Math.ceil(finalTotal / requestedLimit) : 1);
        res.setHeader('X-Current-Page', page);

        await logActivity(req, 'VIEW_INVENTORY_LIST');
        res.json(finalRows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

app.post('/api/products', authenticateToken, async (req, res) => {
    const { barcode, name, category, price, stock, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry, batch_number, expiry_date } = req.body;
    try {
        // Refresh store info from DB to ensure accuracy
        const userRes = await pool.query('SELECT store_id, store_location FROM users WHERE id = $1', [req.user.id]);
        const dbUser = userRes.rows[0];

        const userBranch = dbUser?.store_location || req.user.store_location || 'Main Warehouse';
        const branchId = dbUser?.store_id || req.user.store_id || 1;

        const stockValue = parseInt(stock) || 0;
        const stockLevels = JSON.stringify({ [userBranch]: stockValue });

        await pool.query(
            `INSERT INTO products (barcode, name, category, price, stock, stock_levels, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry, tenant_id) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
            [barcode, name, category, price, stockValue, stockLevels, cost_price || 0, selling_unit || 'Unit', packaging_unit || 'Box', conversion_rate || 1, reorder_level || 10, track_batch, track_expiry, req.user.tenant_id]
        );

        // Insert Batch if provided and stock > 0
        if (stockValue > 0 && (batch_number || expiry_date)) {
            // Ensure batch number exists if expiry is provided
            const finalBatchNum = batch_number || `BATCH-${Date.now()}`;

            await pool.query(
                `INSERT INTO product_batches (product_barcode, batch_number, expiry_date, quantity, quantity_available, quantity_received, branch_id, status)
                 VALUES ($1, $2, $3, $4, $4, $4, $5, 'Active')
                 ON CONFLICT (product_barcode, batch_number, branch_id) 
                 DO UPDATE SET 
                     expiry_date = EXCLUDED.expiry_date,
                     quantity = EXCLUDED.quantity,
                     quantity_available = EXCLUDED.quantity_available,
                     quantity_received = EXCLUDED.quantity_received`,
                [barcode, finalBatchNum, expiry_date || null, stockValue, branchId]
            );
        }

        await logActivity(req, 'CREATE_PRODUCT', { barcode, name, stock });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error adding product' });
    }
});

// --- FLEXIBLE DATE PARSER HELPER ---
// Intelligently parses dates in format DD/MM/YYYY, MM/DD/YYYY, YYYY-MM-DD, etc.
// Handles any separator (/, -, .) and determines month/day based on numeric values
function parseFlexibleDate(dateInput) {
    if (!dateInput) return null;

    // If it's a number, it's likely an Excel date serial
    if (typeof dateInput === 'number') {
        const excelEpoch = new Date(1900, 0, 1);
        const date = new Date(excelEpoch.getTime() + (dateInput - 2) * 24 * 60 * 60 * 1000);
        if (!isNaN(date.getTime())) {
            return date.toISOString().split('T')[0];
        }
    }

    // If it's a string
    if (typeof dateInput === 'string') {
        dateInput = dateInput.trim();

        // Try common separators: /, -, .
        let parts = [];

        if (dateInput.includes('/')) {
            parts = dateInput.split('/').map(p => parseInt(p));
        } else if (dateInput.includes('-')) {
            parts = dateInput.split('-').map(p => parseInt(p));
        } else if (dateInput.includes('.')) {
            parts = dateInput.split('.').map(p => parseInt(p));
        }

        if (parts.length === 3 && parts.every(p => !isNaN(p))) {
            let day, month, year;
            const p0 = parts[0];
            const p1 = parts[1];
            const p2 = parts[2];

            // Smart detection based on numeric values
            // If any part > 31, it's definitely the year
            // If any part > 12, it must be the day

            if (p0 > 31) {
                // YYYY/MM/DD or YYYY/DD/MM - p0 is year
                year = p0;
                if (p1 > 12) {
                    day = p1;
                    month = p2;
                } else if (p2 > 12) {
                    day = p2;
                    month = p1;
                } else {
                    // Both <= 12, assume MM/DD
                    month = p1;
                    day = p2;
                }
            } else if (p2 > 31) {
                // MM/DD/YYYY or DD/MM/YYYY - p2 is year
                year = p2;
                if (p0 > 12) {
                    day = p0;
                    month = p1;
                } else if (p1 > 12) {
                    month = p0;
                    day = p1;
                } else {
                    // Both <= 12, assume first part is day (DD/MM format is common)
                    day = p0;
                    month = p1;
                }
            } else {
                // None > 31, year might be in different position or 2-digit
                // Check if any looks like a year (> current realistic year or < current year)
                if (p0 > 31 || (p0 > 12 && p1 <= 12 && p2 <= 31)) {
                    year = p0;
                    if (p1 > 12) {
                        day = p1; month = p2;
                    } else if (p2 > 12) {
                        day = p2; month = p1;
                    } else {
                        month = p1; day = p2;
                    }
                } else if (p1 > 31 || (p1 > 12 && p0 <= 12 && p2 <= 31)) {
                    year = p1;
                    day = p0; month = p2;
                } else if (p2 > 31 || p2 > 0) {
                    // p2 is most likely year (most common format)
                    year = p2;
                    if (p0 > 12) {
                        day = p0; month = p1;
                    } else if (p1 > 12) {
                        month = p0; day = p1;
                    } else {
                        day = p0; month = p1;
                    }
                }
            }

            // Validate ranges
            if (!day || !month || !year) return null;
            if (day < 1 || day > 31 || month < 1 || month > 12) {
                return null;
            }

            // Handle 2-digit years
            if (year < 100) {
                year += year < 50 ? 2000 : 1900;
            }

            // Create date (Date constructor: year, month (0-indexed), day)
            const date = new Date(year, month - 1, day);
            if (!isNaN(date.getTime())) {
                return date.toISOString().split('T')[0];
            }
        }
    }

    return null;
}

// --- BULK UPLOAD ENDPOINT ---
app.post('/api/products/bulk', authenticateToken, upload.single('file'), async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    try {
        if (!req.file) return res.status(400).json({ message: 'No file uploaded' });

        // Parse Excel/CSV File
        const fileExt = req.file.originalname?.toLowerCase() || '';
        const isCSV = fileExt.endsWith('.csv');

        const workbook = xlsx.read(req.file.buffer, {
            type: 'buffer',
            raw: isCSV
        });

        const sheetName = workbook.SheetNames[0];
        const data = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName], {
            defval: '',
            raw: false
        });

        console.log(`[BULK UPLOAD] File: ${req.file.originalname}, Rows: ${data.length}`);
        if (data.length > 0) {
            console.log('[BULK UPLOAD] First row columns:', Object.keys(data[0]));
        }

        const results = { success: 0, failed: 0, errors: [] };
        const userBranch = req.user.store_location || 'Main Warehouse';
        const branchId = req.user.store_id || 1;

        // Helper to extract clean numeric value (handles "GHS 10.00", "₵5", "1,000")
        const cleanNum = (val) => {
            if (typeof val === 'number') return val;
            if (!val || typeof val !== 'string') return 0;
            return parseFloat(val.replace(/[^0-9.-]/g, '')) || 0;
        };

        // storage for stock-take items (barcode + physical count)
        const stockTakeItems = [];
        for (const rawRow of data) {
            // Normalize keys: trim and lowercase to handle CSV header variations
            const row = {};
            Object.keys(rawRow).forEach(k => {
                row[k.trim().toLowerCase()] = rawRow[k];
            });

            let name, category, cost, price, markup, stock, sellingUnit, packagingUnit, conversionRate, reorderLevel;
            let batchNumber, expiryDate, barcode, physicalCount, stockLevels;

            try {
                // Map Excel Columns to Database Fields
                // Handle encoding issues by stripping special chars and normalizing
                const normalizeKey = (k) => k.toLowerCase().replace(/[^a-z0-9]/g, '');
                const rowKeys = Object.keys(row);

                const findColumn = (patterns) => {
                    for (const pattern of patterns) {
                        const normalizedPattern = normalizeKey(pattern);
                        const match = rowKeys.find(k => normalizeKey(k).includes(normalizedPattern));
                        if (match) {
                            console.log(`[BULK UPLOAD] Matched "${pattern}" to column "${match}" with value:`, row[match]);
                            return row[match];
                        }
                    }
                    console.log(`[BULK UPLOAD] No match found for patterns:`, patterns);
                    return '';
                };

                name = findColumn(['product name', 'name']).toString().trim();
                if (!name) {
                    console.log('[BULK UPLOAD] Skipping row - no product name found');
                    continue; // Skip empty rows
                }

                category = findColumn(['category', 'type']).toString().trim() || 'General';
                // ensure category exists in categories table
                if (category) {
                    await pool.query(
                        `INSERT INTO categories (name, branch_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
                        [category, branchId]
                    );
                }

                cost = cleanNum(findColumn(['cost', 'costprice']));
                price = cleanNum(findColumn(['price', 'sellingprice']));
                markup = cleanNum(findColumn(['markup', 'margin']));

                // Auto-calculate price if missing but Cost & Markup exist
                if (price === 0 && cost > 0 && markup > 0) {
                    price = cost * (1 + (markup / 100));
                }

                stock = parseInt(findColumn(['currentstock', 'stock', 'quantity']) || 0);
                sellingUnit = findColumn(['sellingunit', 'unit']) || 'Unit';
                packagingUnit = findColumn(['packagingunit', 'box', 'pack']) || 'Box';
                conversionRate = cleanNum(findColumn(['itemsperpackage', 'conversion', 'perpackage']) || 1);
                reorderLevel = parseInt(findColumn(['reorderlevel', 'reorder', 'threshold']) || 10);

                // Extract Batch Information
                batchNumber = findColumn(['batchnumber', 'batch', 'lot']).toString().trim();
                expiryDate = findColumn(['expirydate', 'expiry', 'expirationdate', 'expires']);

                // Parse expiry date using flexible parser (handles any format/separator)
                if (expiryDate) {
                    const parsedDate = parseFlexibleDate(expiryDate);
                    expiryDate = parsedDate || '';
                }

                // --- SMART PRODUCT MATCHING ---
                let existingProduct = null;
                const barcodeProvided = findColumn(['barcode', 'sku', 'productcode']).toString().trim();

                if (barcodeProvided) {
                    const barcodeRes = await pool.query('SELECT * FROM products WHERE barcode = $1 AND tenant_id = $2', [barcodeProvided, req.user.tenant_id]);
                    if (barcodeRes.rows.length > 0) {
                        existingProduct = barcodeRes.rows[0];
                        barcode = barcodeProvided;
                    }
                }

                // If not found by barcode, try finding by Name + Category (Case-Insensitive)
                if (!existingProduct) {
                    const nameCatRes = await pool.query(
                        'SELECT * FROM products WHERE LOWER(name) = LOWER($1) AND LOWER(category) = LOWER($2) AND tenant_id = $3',
                        [name, category, req.user.tenant_id]
                    );
                    if (nameCatRes.rows.length > 0) {
                        existingProduct = nameCatRes.rows[0];
                        barcode = existingProduct.barcode; // Use existing barcode
                    }
                }

                // Final Barcode Resolution for new products
                if (!existingProduct && !barcodeProvided) {
                    const prefix = name.substring(0, 3).toUpperCase().replace(/[^A-Z]/g, 'X');
                    barcode = `${prefix}-${Date.now().toString().slice(-6)}-${Math.floor(Math.random() * 1000)}`;
                } else if (!existingProduct && barcodeProvided) {
                    barcode = barcodeProvided;
                }

                // Optional physical count for stock take
                physicalCount = parseInt(findColumn(['physicalcount', 'stocktake', 'count', 'physical']) || 0);
                if (!isNaN(physicalCount) && physicalCount > 0) {
                    stockTakeItems.push({ barcode, name, physicalCount });
                }

                // --- STOCK LEVEL UPDATES ---
                if (existingProduct) {
                    // 1. Calculate NEW TOTAL STOCK
                    const currentTotalStock = parseInt(existingProduct.stock || 0);
                    const newTotalStock = currentTotalStock + stock;

                    // 2. Update BRANCH-SPECIFIC STOCK in JSONB
                    let currentLevels = existingProduct.stock_levels;
                    if (typeof currentLevels === 'string') {
                        try { currentLevels = JSON.parse(currentLevels); } catch (e) { currentLevels = {}; }
                    } else if (!currentLevels) {
                        currentLevels = {};
                    }

                    const currentBranchStock = parseInt(currentLevels[userBranch] || 0);
                    currentLevels[userBranch] = currentBranchStock + stock;
                    const newStockLevels = JSON.stringify(currentLevels);

                    // 3. Update existing product
                    await pool.query(
                        `UPDATE products SET 
                            name = $1, category = $2, price = $3, cost_price = $4, 
                            stock = $5, stock_levels = $6, selling_unit = $7, 
                            packaging_unit = $8, conversion_rate = $9, reorder_level = $10,
                            track_batch = true, track_expiry = true
                         WHERE id = $11`,
                        [name, category, price, cost, newTotalStock, newStockLevels, sellingUnit, packagingUnit, conversionRate, reorderLevel, existingProduct.id]
                    );
                    console.log(`[BULK UPLOAD] UPDATED/INCREMENTED product: ${name} (${barcode}). New Total: ${newTotalStock}`);
                } else {
                    // NEW: Calculate initial stock levels
                    stockLevels = JSON.stringify({ [userBranch]: stock });

                    // Insert brand new product
                    await pool.query(
                        `INSERT INTO products (barcode, name, category, price, cost_price, stock, stock_levels, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry, tenant_id)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, true, true, $12)`,
                        [barcode, name, category, price, cost, stock, stockLevels, sellingUnit, packagingUnit, conversionRate, reorderLevel, req.user.tenant_id]
                    );
                    console.log(`[BULK UPLOAD] INSERTED product: ${name} (${barcode}). Initial Stock: ${stock}`);
                }

                // If batch number is provided, create/update batch record
                if (batchNumber) {
                    console.log(`[BULK UPLOAD] Creating batch (if new): ${batchNumber} for ${barcode}`);
                    await pool.query(
                        `INSERT INTO product_batches (product_barcode, batch_number, expiry_date, quantity, quantity_available, quantity_received, branch_id, status)
                         VALUES ($1, $2, $3, $4, $4, $4, $5, 'Active')
                         ON CONFLICT (product_barcode, batch_number, branch_id) DO NOTHING`,
                        [barcode, batchNumber, expiryDate || null, stock, branchId]
                    );
                }

                // record barcode for stock take entry after product exists
                if (stockTakeItems.length && stockTakeItems[stockTakeItems.length - 1].name === name && !stockTakeItems[stockTakeItems.length - 1].barcode) {
                    stockTakeItems[stockTakeItems.length - 1].barcode = barcode;
                }

                results.success++;
            } catch (e) {
                results.failed++;
                const productName = name || 'Unknown Product';
                console.error(`[BULK UPLOAD] ERROR processing ${productName}:`, e.message);
                console.error(`[BULK UPLOAD] Full error:`, e);
                results.errors.push(`Error adding ${productName}: ${e.message}`);
            }
        }

        // if physical counts were provided, create a stock take record
        if (stockTakeItems.length) {
            try {
                const stRes = await pool.query(
                    `INSERT INTO stock_takes (stock_take_date, branch_id, created_by, status)
                     VALUES (CURRENT_DATE, $1, $2, 'In Progress') RETURNING id`,
                    [branchId, req.user.id]
                );
                const stId = stRes.rows[0].id;

                for (const item of stockTakeItems) {
                    // fetch current system count (may have been just inserted)
                    const sysRes = await pool.query('SELECT stock FROM products WHERE barcode = $1', [item.barcode]);
                    const systemCount = (sysRes.rows[0] && sysRes.rows[0].stock) || 0;
                    const variance = item.physicalCount - systemCount;
                    await pool.query(
                        `INSERT INTO stock_take_items (stock_take_id, product_barcode, physical_count, system_count, variance, counted_by, counted_at)
                         VALUES ($1,$2,$3,$4,$5,$6,CURRENT_TIMESTAMP)`,
                        [stId, item.barcode, item.physicalCount, systemCount, variance, req.user.id]
                    );
                }
            } catch (stErr) {
                console.error('Error creating stock take from bulk upload', stErr);
            }
        }

        await logActivity(req, 'BULK_PRODUCT_UPLOAD', { count: results.success, failed: results.failed });
        res.json({ success: true, results });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error processing bulk upload' });
    }
});

app.put('/api/products/:id', authenticateToken, async (req, res) => {
    const { barcode: newBarcode, name, category, price, stock, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry, stock_levels: incomingStockLevels } = req.body;
    const { id } = req.params;
    try {
        const userRes = await pool.query('SELECT store_id, store_location FROM users WHERE id = $1', [req.user.id]);
        const dbUser = userRes.rows[0];
        const userBranch = dbUser?.store_location || 'Main Warehouse';
        const branchId = dbUser?.store_id || 1;

        const stockValue = parseInt(stock) || 0;

        // Get existing stock levels and tracking settings using ID
        const checkRes = await pool.query(
            'SELECT stock_levels, track_batch, track_expiry, barcode FROM products WHERE id = $1',
            [id]
        );

        if (checkRes.rows.length === 0) {
            return res.status(404).json({ message: 'Product not found' });
        }
        const current = checkRes.rows[0];
        const currentBarcode = current.barcode; // old barcode from DB
        const finalBarcode = newBarcode || currentBarcode; // use new if provided, else keep old

        // Use incoming stock_levels if provided, otherwise merge with existing
        let stockLevels = incomingStockLevels || current.stock_levels || {};

        if (typeof stockLevels === 'string') {
            try {
                stockLevels = JSON.parse(stockLevels);
            } catch (e) {
                stockLevels = {};
            }
        }

        // Fallback: update branch stock manually using the 'stock' field if stock_levels wasn't explicitly provided
        if (!incomingStockLevels) {
            const existingBranches = Object.keys(stockLevels).filter(k => !isNaN(parseInt(stockLevels[k])));
            const targetBranch = existingBranches.length > 0 ? existingBranches[0] : userBranch;
            stockLevels[targetBranch] = stockValue;
        }

        // Recalculate total stock from all branches
        const totalStock = Object.values(stockLevels).reduce((sum, val) => sum + (parseInt(val) || 0), 0);

        const result = await pool.query(
            `UPDATE products SET 
                barcode = $1, name = $2, category = $3, price = $4, stock = $5, 
                cost_price = $6, selling_unit = $7, packaging_unit = $8, 
                conversion_rate = $9, reorder_level = $10, track_batch = $11, 
                track_expiry = $12, stock_levels = $13
             WHERE id = $14 RETURNING *`,
            [finalBarcode, name, category, price, totalStock, cost_price, selling_unit, packaging_unit,
                conversion_rate, reorder_level, track_batch ?? current.track_batch, track_expiry ?? current.track_expiry,
                JSON.stringify(stockLevels), id]
        );

        // Cascade barcode change to batches if barcode actually changed
        if (newBarcode && newBarcode !== currentBarcode) {
            await pool.query('UPDATE product_batches SET product_barcode = $1 WHERE product_barcode = $2', [newBarcode, currentBarcode]);
        }

        res.json({ message: 'Product updated successfully', product: result.rows[0] });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error updating product' });
    }
});

app.delete('/products/:barcode', authenticateToken, async (req, res) => {
    try {
        const tenantId = req.user.tenant_id || 1;
        const productRes = await pool.query('SELECT name FROM products WHERE (barcode = $1 OR id::text = $1) AND tenant_id = $2', [req.params.barcode, tenantId]);
        const productName = productRes.rows[0]?.name || 'Unknown';

        const deleteRes = await pool.query('DELETE FROM products WHERE (barcode = $1 OR id::text = $1) AND tenant_id = $2', [req.params.barcode, tenantId]);
        
        if (deleteRes.rowCount === 0) {
            return res.status(404).json({ message: 'Product not found or unauthorized' });
        }
        
        await logActivity(req, 'DELETE_PRODUCT', { identifier: req.params.barcode, name: productName });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error deleting product' });
    }
});

// new API prefix delete route for front-end compatibility
app.delete('/api/products/:barcode', authenticateToken, async (req, res) => {
    try {
        const tenantId = req.user.tenant_id || 1;
        const productRes = await pool.query('SELECT name FROM products WHERE (barcode = $1 OR id::text = $1) AND tenant_id = $2', [req.params.barcode, tenantId]);
        const productName = productRes.rows[0]?.name || 'Unknown';

        const deleteRes = await pool.query('DELETE FROM products WHERE (barcode = $1 OR id::text = $1) AND tenant_id = $2', [req.params.barcode, tenantId]);
        
        if (deleteRes.rowCount === 0) {
            return res.status(404).json({ message: 'Product not found or unauthorized' });
        }
        
        await logActivity(req, 'DELETE_PRODUCT', { identifier: req.params.barcode, name: productName });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error deleting product' });
    }
});

// Transactions Endpoint
app.get('/transactions/all', authenticateToken, async (req, res) => {
    if (!req.user || (req.user.role !== 'admin' && req.user.role !== 'manager' && req.user.role !== 'ceo')) {
        return res.status(403).json({ message: 'Unauthorized' });
    }

    try {
        // CEO users see all branches; managers/admins see their location only
        const storeLocation = req.user.role === 'ceo' ? null : req.user.store_location;
        let query = `SELECT t.*, u.name as cashier_name 
             FROM transactions t 
             LEFT JOIN users u ON t.user_id = u.id
             WHERE t.status = 'completed'`;

        const params = [];
        if (storeLocation) {
            query += ` AND t.store_location = $1`;
            params.push(storeLocation);
        }

        query += ` ORDER BY t.created_at DESC`;

        const result = await pool.query(query, params);
        await logActivity(req, 'VIEW_TRANSACTION_HISTORY');
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// --- CATEGORIES MANAGEMENT ---
app.get('/api/categories', authenticateToken, async (req, res) => {
    try {
        let query = 'SELECT * FROM categories WHERE tenant_id = $1';
        let params = [req.user.tenant_id];

        // Filter by branch for non-CEO/Admin users
        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' AND branch_id = $2';
            params.push(req.user.store_id);
        }
        query += ' ORDER BY name';

        // Auto-create table if not exists (for ease of setup)
        await pool.query(`CREATE TABLE IF NOT EXISTS categories (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            branch_id INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) { console.error(err); res.status(500).json({ message: 'Server error' }); }
});

app.post('/api/categories', authenticateToken, async (req, res) => {
    const { name, description } = req.body;
    const branchId = req.user.store_id;
    try {
        await pool.query('INSERT INTO categories (name, description, branch_id, tenant_id) VALUES ($1, $2, $3, $4)', [name, description, branchId, req.user.tenant_id]);
        await logActivity(req, 'CREATE_CATEGORY', { name, branchId });
        res.json({ success: true });
    } catch (err) { console.error(err); res.status(500).json({ message: 'Error creating category' }); }
});

app.delete('/api/categories/:id', authenticateToken, async (req, res) => {
    try {
        let query = 'DELETE FROM categories WHERE id = $1 AND tenant_id = $2';
        let params = [req.params.id, req.user.tenant_id];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' AND branch_id = $3';
            params.push(req.user.store_id);
        }

        const result = await pool.query(query, params);
        if (result.rowCount === 0) return res.status(404).json({ message: 'Category not found or access denied' });

        await logActivity(req, 'DELETE_CATEGORY', { id: req.params.id });
        res.json({ success: true });
    } catch (err) { console.error(err); res.status(500).json({ message: 'Error deleting category' }); }
});

// --- SUPPLIER MANAGEMENT ---
app.get('/api/suppliers', authenticateToken, async (req, res) => {
    try {
        let query = `SELECT s.*, b.name as branch_name, b.location as branch_location 
            FROM suppliers s
            LEFT JOIN branches b ON s.branch_id = b.id
            WHERE s.tenant_id = $1`;
        let params = [req.user.tenant_id];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' AND s.branch_id = $2';
            params.push(req.user.store_id);
        }
        query += ' ORDER BY s.name';

        const result = await pool.query(query, params);
        await logActivity(req, 'VIEW_SUPPLIER_LIST');
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

app.post('/api/suppliers', authenticateToken, async (req, res) => {
    const { name, contact, phone, email, address } = req.body;
    const branchId = req.user.store_id;
    try {
        await pool.query(
            'INSERT INTO suppliers (name, contact_person, phone, email, address, branch_id, tenant_id) VALUES ($1, $2, $3, $4, $5, $6, $7)',
            [name, contact, phone, email, address, branchId, req.user.tenant_id]
        );
        await logActivity(req, 'CREATE_SUPPLIER', { name, branchId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ message: 'Error adding supplier' }); }
});

app.put('/api/suppliers/:id', authenticateToken, async (req, res) => {
    const { name, contact, phone, email, address } = req.body;
    try {
        let query = 'UPDATE suppliers SET name = $1, contact_person = $2, phone = $3, email = $4, address = $5 WHERE id = $6 AND tenant_id = $7';
        let params = [name, contact, phone, email, address, req.params.id, req.user.tenant_id];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' AND branch_id = $8';
            params.push(req.user.store_id);
        }

        const result = await pool.query(query, params);
        if (result.rowCount === 0) return res.status(404).json({ message: 'Supplier not found or access denied' });

        await logActivity(req, 'UPDATE_SUPPLIER', { id: req.params.id, name });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ message: 'Error updating supplier' }); }
});

app.delete('/api/suppliers/:id', authenticateToken, async (req, res) => {
    try {
        let query = 'DELETE FROM suppliers WHERE id = $1 AND tenant_id = $2';
        let params = [req.params.id, req.user.tenant_id];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' AND branch_id = $3';
            params.push(req.user.store_id);
        }

        const result = await pool.query(query, params);
        if (result.rowCount === 0) return res.status(404).json({ message: 'Supplier not found or access denied' });

        await logActivity(req, 'DELETE_SUPPLIER', { id: req.params.id });
        res.json({ success: true });
    } catch (err) { 
        console.error(err); 
        if (err.code === '23503') {
            return res.status(400).json({ message: 'Cannot delete supplier because it is linked to existing purchase orders.' });
        }
        res.status(500).json({ message: 'Error deleting supplier' }); 
    }
});

// --- PRODUCT UPDATES (Consolidated & Fixed) ---

app.put('/api/products/:identifier', authenticateToken, async (req, res) => {
    const { identifier: paramIdentifier } = req.params;
    const { barcode: newBarcode, name, category, price, stock, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry, batch_number, expiry_date, stock_levels: incomingStockLevels } = req.body;

    try {
        const userRes = await pool.query('SELECT store_id, store_location FROM users WHERE id = $1', [req.user.id]);
        const dbUser = userRes.rows[0];
        const userBranch = dbUser?.store_location || 'Main Warehouse';
        const branchId = dbUser?.store_id || 1;

        const stockValue = parseInt(stock) || 0;

        // Determine if identifier is an ID (numeric) or barcode
        const isId = /^\d+$/.test(paramIdentifier);

        // Get current product to manage stock_levels
        let currentRes;
        if (isId) {
            currentRes = await pool.query('SELECT id, barcode, stock_levels, track_batch, track_expiry FROM products WHERE id = $1 AND tenant_id = $2', [paramIdentifier, req.user.tenant_id]);
        } else {
            currentRes = await pool.query('SELECT id, barcode, stock_levels, track_batch, track_expiry FROM products WHERE barcode = $1 AND tenant_id = $2', [paramIdentifier, req.user.tenant_id]);
        }

        if (currentRes.rows.length === 0) return res.status(404).json({ message: 'Product not found' });

        const product = currentRes.rows[0];
        const productId = product.id;
        const productBarcode = product.barcode;

        let stockLevels = incomingStockLevels || product.stock_levels || {};
        if (typeof stockLevels === 'string') stockLevels = JSON.parse(stockLevels);

        // Fallback: update branch stock manually using the 'stock' field if stock_levels wasn't explicitly provided
        if (!incomingStockLevels) {
            const existingBranches = Object.keys(stockLevels).filter(k => !isNaN(parseInt(stockLevels[k])));
            const targetBranch = existingBranches.length > 0 ? existingBranches[0] : userBranch;
            stockLevels[targetBranch] = stockValue;
        }

        const totalStock = Object.values(stockLevels).reduce((sum, val) => sum + (parseInt(val) || 0), 0);

        const oldBarcode = product.barcode;
        const finalBarcode = newBarcode || oldBarcode;

        await pool.query(
            'UPDATE products SET barcode = $1, name = $2, category = $3, price = $4, stock = $5, stock_levels = $6, cost_price = $7, selling_unit = $8, packaging_unit = $9, conversion_rate = $10, reorder_level = $11, track_batch = $12, track_expiry = $13 WHERE id = $14 AND tenant_id = $15',
            [finalBarcode, name, category, price, totalStock, JSON.stringify(stockLevels), cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch ?? product.track_batch, track_expiry ?? product.track_expiry, productId, req.user.tenant_id]
        );

        // Cascade barcode change to batches if needed
        if (newBarcode && newBarcode !== oldBarcode) {
            await pool.query('UPDATE product_batches SET product_barcode = $1 WHERE product_barcode = $2', [newBarcode, oldBarcode]);
        }

        if (batch_number) {
            await pool.query(`
                INSERT INTO product_batches (product_barcode, batch_number, expiry_date, quantity, quantity_available, quantity_received, branch_id, status)
                VALUES ($1, $2, $3, $4, $4, $4, $5, 'Active')
                ON CONFLICT (product_barcode, batch_number, branch_id) 
                DO UPDATE SET expiry_date = EXCLUDED.expiry_date, quantity_available = EXCLUDED.quantity_available
            `, [productBarcode, batch_number, expiry_date || null, stockValue, branchId]);
        }

        await logActivity(req, 'UPDATE_PRODUCT', { id: productId, barcode: productBarcode, name });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error updating product' });
    }
});

// --- PROCUREMENT (Purchase Orders) ---
app.get('/api/purchase-orders', authenticateToken, async (req, res) => {
    try {
        let query = `
            SELECT po.*, s.name as supplier_name, b.name as branch_name
            FROM purchase_orders po 
            LEFT JOIN suppliers s ON po.supplier_id = s.id 
            LEFT JOIN branches b ON po.branch_id = b.id
        `;
        let params = [];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' WHERE po.branch_id = $1';
            params.push(req.user.store_id);
        }

        query += ' ORDER BY po.created_at DESC';

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

app.get('/api/purchase-orders/:id', authenticateToken, async (req, res) => {
    const { id } = req.params;
    try {
        const poRes = await pool.query(`
            SELECT po.*, s.name as supplier_name, b.name as branch_name
            FROM purchase_orders po 
            LEFT JOIN suppliers s ON po.supplier_id = s.id 
            LEFT JOIN branches b ON po.branch_id = b.id
            WHERE po.id = $1
        `, [id]);

        if (poRes.rows.length === 0) return res.status(404).json({ message: 'PO not found' });

        const itemsRes = await pool.query(`
            SELECT poi.*, p.name 
            FROM purchase_order_items poi
            LEFT JOIN products p ON poi.product_barcode = p.barcode
            WHERE poi.po_id = $1
        `, [id]);

        const po = poRes.rows[0];
        po.items = itemsRes.rows;

        res.json(po);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

app.post('/api/purchase-orders', authenticateToken, async (req, res) => {
    const { supplierId, items, total } = req.body;
    let branchId = req.user.store_id;

    // Ensure branchId is set (fallback to location lookup or default)
    if (!branchId) {
        if (req.user.store_location) {
            try {
                const bRes = await pool.query('SELECT id FROM branches WHERE name = $1 OR location = $1', [req.user.store_location]);
                if (bRes.rows.length > 0) branchId = bRes.rows[0].id;
            } catch (e) { console.error('Branch lookup error in PO creation:', e); }
        }
        if (!branchId) branchId = 1; // Default to Main Warehouse
    }

    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const poRes = await client.query(
            'INSERT INTO purchase_orders (supplier_id, total_amount, status, branch_id) VALUES ($1, $2, $3, $4) RETURNING id',
            [supplierId, total, 'Pending', branchId]
        );
        const poId = poRes.rows[0].id;

        for (const item of items) {
            await client.query(
                'INSERT INTO purchase_order_items (po_id, product_barcode, quantity, unit_cost) VALUES ($1, $2, $3, $4)',
                [poId, item.barcode, item.qty, item.cost]
            );
        }
        await client.query('COMMIT');
        await logActivity(req, 'CREATE_PO', { supplierId, total, itemCount: items.length });
        res.json({ success: true });
    } catch (err) {
        await client.query('ROLLBACK');
        res.status(500).json({ message: 'Error creating PO' });
    } finally { client.release(); }
});

app.post('/api/purchase-orders/:id/receive', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const { receivedItems } = req.body; // Array of { product_barcode, batch_number, expiry_date }
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // Get PO details including supplier name for batch generation
        const poRes = await client.query(`
            SELECT po.*, s.name as supplier_name 
            FROM purchase_orders po
            LEFT JOIN suppliers s ON po.supplier_id = s.id
            WHERE po.id = $1
        `, [id]);

        if (poRes.rows.length === 0) throw new Error('PO not found');
        const po = poRes.rows[0];

        // Get PO items
        const itemsRes = await client.query('SELECT * FROM purchase_order_items WHERE po_id = $1', [id]);

        const today = new Date();
        const dateStr = today.toISOString().slice(2, 10).replace(/-/g, ''); // YYMMDD
        const supplierCode = (po.supplier_name || 'SUP').substring(0, 3).toUpperCase().replace(/\s/g, '');

        // Update stock and create batches
        const branchId = req.user.store_id || 1;
        for (const item of itemsRes.rows) {
            // Fetch product details for conversion
            const prodRes = await client.query('SELECT name, conversion_rate FROM products WHERE barcode = $1', [item.product_barcode]);
            const product = prodRes.rows[0] || { name: 'ITEM', conversion_rate: 1 };

            // Determine quantity to receive (use user input if available, else PO quantity)
            let qtyToReceive = item.quantity;
            let finalBatchNum = null;
            let finalExpiry = null;

            if (receivedItems && Array.isArray(receivedItems)) {
                const provided = receivedItems.find(ri => ri.product_barcode === item.product_barcode);
                if (provided) {
                    if (provided.quantity_received) qtyToReceive = parseInt(provided.quantity_received);
                    if (provided.batch_number) finalBatchNum = provided.batch_number;
                    if (provided.expiry_date) finalExpiry = new Date(provided.expiry_date);
                }
            }

            // Use the Qty from Goods Receiving directly for Total Stock
            const totalUnits = qtyToReceive;

            // 1. Update Product Stock with Total Units (Update both total stock and stock_levels for User's Branch)
            const branchName = req.user.store_location || 'Main Warehouse';
            await client.query(`
                UPDATE products 
                SET stock = COALESCE(stock, 0) + $1,
                    stock_levels = jsonb_set(
                        COALESCE(stock_levels, '{}'::jsonb), 
                        ARRAY[$3::text], 
                        to_jsonb(COALESCE((stock_levels->>$3)::int, 0) + $1)
                    )
                WHERE barcode = $2
            `, [totalUnits, item.product_barcode, branchName]);

            // Auto-generate if missing
            const itemCode = product.name.substring(0, 3).toUpperCase().replace(/\s/g, '');
            if (!finalExpiry) {
                finalExpiry = new Date();
                finalExpiry.setFullYear(finalExpiry.getFullYear() + 1);
            }
            const expStr = finalExpiry.toISOString().slice(2, 10).replace(/-/g, '');
            if (!finalBatchNum) finalBatchNum = `${supplierCode}-${dateStr}-${itemCode}-${expStr}`;

            // Check for existing batch with same number but different expiry
            const existingBatchRes = await client.query(
                'SELECT expiry_date FROM product_batches WHERE product_barcode = $1 AND batch_number = $2 AND branch_id = $3',
                [item.product_barcode, finalBatchNum, branchId]
            );

            if (existingBatchRes.rows.length > 0) {
                const existingExpiry = new Date(existingBatchRes.rows[0].expiry_date);
                // Compare dates (ignoring time)
                if (existingExpiry.toISOString().slice(0, 10) !== finalExpiry.toISOString().slice(0, 10)) {
                    // Conflict: Same batch number, different expiry.
                    // Append expiry date to batch number to create a new batch record
                    finalBatchNum = `${finalBatchNum}-${expStr}`;
                }
            }

            await client.query(`
                INSERT INTO product_batches (product_barcode, batch_number, quantity, quantity_available, quantity_received, expiry_date, branch_id, status) 
                VALUES ($1, $2, $3, $3, $3, $4, $5, 'Active')
                ON CONFLICT (product_barcode, batch_number, branch_id) 
                DO UPDATE SET 
                    quantity = product_batches.quantity + $3,
                    quantity_available = product_batches.quantity_available + $3,
                    quantity_received = product_batches.quantity_received + $3
            `, [item.product_barcode, finalBatchNum, totalUnits, finalExpiry, branchId]);

            // Log to inventory_audit_log
            await client.query(`
                INSERT INTO inventory_audit_log (action_type, product_barcode, quantity_before, quantity_after, reference_id, reference_type, user_id, branch_id)
                SELECT 'Stock In', $1::varchar, stock - $2, stock, $3, 'Purchase Order', $4, $5 FROM products WHERE barcode = $1
            `, [item.product_barcode, totalUnits, id, req.user.id, branchId]);
        }

        // Update PO status
        await client.query("UPDATE purchase_orders SET status = 'Received' WHERE id = $1", [id]);

        await client.query('COMMIT');
        await logActivity(req, 'RECEIVE_GOODS', { poId: id, itemsReceived: itemsRes.rows.length });
        res.json({ success: true });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error(err);
        res.status(500).json({ message: 'Error receiving goods' });
    } finally { client.release(); }
});

// Get Goods Received History
app.get('/api/goods-received', authenticateToken, async (req, res) => {
    try {
        let query = `
            SELECT gr.*, p.name as product_name 
            FROM goods_received gr
            JOIN products p ON gr.product_barcode = p.barcode
        `;
        let params = [];
        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' WHERE gr.branch_id = $1';
            params.push(req.user.store_id);
        }
        query += ' ORDER BY gr.received_at DESC LIMIT 100';
        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

// --- DISTRIBUTION ---
app.get('/api/transfers', authenticateToken, async (req, res) => {
    try {
        let query = 'SELECT * FROM stock_transfers';
        let params = [];

        // Filter transfers relevant to the user's branch (either sending or receiving)
        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_location) {
            const userCanonicalLocation = await getBranchNameFromLocation(req.user.store_location, pool);
            query += ' WHERE from_location = $1 OR to_location = $1';
            params.push(userCanonicalLocation);
        }

        query += ' ORDER BY transfer_date DESC';
        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) { 
        console.error('GET /api/transfers Error:', err.message);
        res.status(500).json({ message: 'Server error: ' + err.message }); 
    }
});

app.post('/api/transfers', authenticateToken, async (req, res) => {
    const { from, to, items } = req.body;
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        console.log(`\n📦 TRANSFER REQUEST: ${from} → ${to}`);
        console.log(`Items:`, JSON.stringify(items, null, 2));

        // Get branch mapping to find correct stock_levels keys
        const branchesRes = await client.query('SELECT id, name, location FROM branches');
        const branches = branchesRes.rows;

        // Map the 'from' location to the correct branch name
        let mappedFrom = from;
        const fromMatch = branches.find(b => b.name === from || b.location === from);
        if (fromMatch) mappedFrom = fromMatch.name;

        // Map the 'to' location to the canonical branch name as well
        let mappedTo = to;
        const toMatch = branches.find(b => b.name === to || b.location === to);
        if (toMatch) mappedTo = toMatch.name;

        console.log(`   Branch mapping: ${from} → ${mappedFrom}, ${to} → ${mappedTo}`);

        if (mappedFrom === mappedTo) {
            throw new Error(`Invalid Transfer: Both the 'From' (${from}) and 'To' (${to}) locations map to the exact same physical branch (${mappedFrom}). You cannot transfer stock into the same store.`);
        }

        // Deduct Stock FIRST (validate and update)
        for (const item of items) {
            // Get current product stock data using product_id instead of barcode
            const checkRes = await client.query(`
                SELECT stock, stock_levels, barcode, id FROM products WHERE id = $1 AND tenant_id = $2 FOR UPDATE
            `, [item.product_id, req.user.tenant_id]);

            if (checkRes.rows.length === 0) {
                throw new Error(`Product ${item.barcode || item.product_id} not found`);
            }

            const product = checkRes.rows[0];
            console.log(`\n📍 Product: ${item.barcode}`);
            console.log(`   Total stock in DB: ${product.stock}`);
            console.log(`   stock_levels in DB:`, product.stock_levels);

            // Determine location stock - check multiple possible keys
            let stockLevels = product.stock_levels || {};
            if (typeof stockLevels === 'string') {
                try {
                    stockLevels = JSON.parse(stockLevels);
                } catch (e) {
                    stockLevels = {};
                }
            }

            // FIX: Use a Set to ensure each possible key/alias is only processed ONCE to prevent doubling stock
            const possibleKeys = [...new Set([mappedFrom, from, fromMatch?.location, fromMatch?.name].filter(Boolean))];
            let consolidatedStock = 0;
            let primaryKey = mappedFrom;

            // Consolidate all alias keys into the primary key to prevent split stock
            for (const key of possibleKeys) {
                if (stockLevels[key] !== undefined && stockLevels[key] !== null) {
                    consolidatedStock += parseInt(stockLevels[key]) || 0;
                    if (key !== primaryKey) delete stockLevels[key];
                }
            }
            stockLevels[primaryKey] = consolidatedStock;

            if (consolidatedStock < item.qty) {
                throw new Error(`Insufficient stock at ${from} for product ${item.barcode}. Available: ${consolidatedStock}, Requested: ${item.qty}`);
            }

            // Deduct from Total Stock and Source Location to keep values in sync
            // The items are "In Transit" and thus temporarily removed from available inventory
            console.log(`   📉 Before deduction: Total=${product.stock}, ${primaryKey}=${stockLevels[primaryKey]}`);
            console.log(`   📉 Stock levels object:`, JSON.stringify(stockLevels));
            console.log(`   📉 Product ID being updated: ${product.id}`);

            await client.query('UPDATE products SET stock = stock - $1 WHERE id = $2', [item.qty, product.id]);

            stockLevels[primaryKey] = consolidatedStock - item.qty;
            console.log(`   📉 After local update: ${primaryKey}=${stockLevels[primaryKey]}`);
            console.log(`   📉 Final stock levels to save:`, JSON.stringify(stockLevels));

            await client.query('UPDATE products SET stock_levels = $1 WHERE id = $2', [JSON.stringify(stockLevels), product.id]);

            console.log(`   📉 After deduction: Total=${product.stock - item.qty}, ${primaryKey}=${stockLevels[primaryKey]}`);
        }

        // Insert Transfer AFTER validating and deducting stock
        await client.query(
            'INSERT INTO stock_transfers (from_location, to_location, items, status, branch_id) VALUES ($1, $2, $3, $4, $5)',
            [mappedFrom, mappedTo, JSON.stringify(items), 'In Transit', req.user.store_id || 1]
        );

        await client.query('COMMIT');
        console.log(`✅ Transfer completed successfully\n`);
        await logActivity(req, 'CREATE_TRANSFER', { from, to, itemCount: items.length });
        res.json({ success: true });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error(`❌ Transfer error:`, err.message);
        console.log('');
        res.status(500).json({ message: err.message || 'Error creating transfer' });
    } finally {
        client.release();
    }
});

app.post('/api/transfers/:id/receive', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // Get Transfer
        const resTr = await client.query('SELECT * FROM stock_transfers WHERE id = $1', [id]);
        if (resTr.rows.length === 0) throw new Error('Transfer not found');
        const transfer = resTr.rows[0];

        if (transfer.status !== 'In Transit') throw new Error('Transfer already processed');

        // FIX: Authorization - Allow Admin/CEO or the specific destination branch
        const userBranchName = await getBranchNameFromLocation(req.user.store_location, pool);
        const userRole = (req.user.role || '').toLowerCase();

        const isAuthorized = userRole === 'admin' || userRole === 'ceo' ||
            (userBranchName.toLowerCase().trim() === (transfer.to_location || '').toLowerCase().trim());

        if (!isAuthorized) {
            await client.query('ROLLBACK');
            return res.status(403).json({
                message: `Unauthorized: Only the destination branch (${transfer.to_location}) can receive this transfer. Your current branch is: ${userBranchName}`
            });
        }

        const items = typeof transfer.items === 'string' ? JSON.parse(transfer.items) : transfer.items;

        // Add Stock to Destination Location and restore to Global Total Stock
        for (const item of items) {
            console.log(`   📈 Receiving item: ${item.barcode}, qty: ${item.qty}`);
            console.log(`   📈 Destination: ${transfer.to_location}`);

            // Get current stock before update
            const currentStockRes = await client.query('SELECT stock, stock_levels FROM products WHERE id = $1 AND tenant_id = $2 FOR UPDATE', [item.product_id, req.user.tenant_id]);
            const currentStock = currentStockRes.rows[0];

            let currentBranchStock = 0;
            if (currentStock.stock_levels) {
                let levels = currentStock.stock_levels;
                if (typeof levels === 'string') {
                    try {
                        levels = JSON.parse(levels);
                    } catch (e) {
                        levels = {};
                    }
                }
                currentBranchStock = parseInt(levels[transfer.to_location]) || 0;
            }

            console.log(`   📈 Before receipt: Total=${currentStock.stock}, ${transfer.to_location}=${currentBranchStock}`);

            // Restore to total stock now that items are available in a branch again
            await client.query('UPDATE products SET stock = stock + $1 WHERE id = $2', [item.qty, item.product_id]);

            // Update destination location stock - ensure we use the exact branch name from the transfer
            await client.query(`
                UPDATE products 
                SET stock_levels = jsonb_set(
                    COALESCE(stock_levels, '{}'::jsonb), 
                    ARRAY[$1::text], 
                    to_jsonb(COALESCE((stock_levels->>$1::text)::int, 0) + $2)
                )
                WHERE id = $3
            `, [transfer.to_location, item.qty, item.product_id]);

            // Get updated stock after update
            const updatedStockRes = await client.query('SELECT stock, stock_levels FROM products WHERE id = $1 AND tenant_id = $2', [item.product_id, req.user.tenant_id]);
            const updatedStock = updatedStockRes.rows[0];

            let updatedBranchStock = 0;
            if (updatedStock.stock_levels) {
                let levels = updatedStock.stock_levels;
                if (typeof levels === 'string') {
                    try {
                        levels = JSON.parse(levels);
                    } catch (e) {
                        levels = {};
                    }
                }
                updatedBranchStock = parseInt(levels[transfer.to_location]) || 0;
            }

            console.log(`   📈 After receipt: Total=${updatedStock.stock}, ${transfer.to_location}=${updatedBranchStock}`);
        }

        await client.query("UPDATE stock_transfers SET status = 'Received' WHERE id = $1", [id]);

        await client.query('COMMIT');
        await logActivity(req, 'RECEIVE_TRANSFER', { transferId: id });
        res.json({ success: true });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error(err);
        res.status(500).json({ message: 'Error receiving transfer' });
    } finally {
        client.release();
    }
});

// --- STOCK CONTROL (Expiry & Batches) ---
app.get('/api/batches/expiry', authenticateToken, async (req, res) => {
    try {
        let query = `
            SELECT b.*, 
                   (SELECT name FROM products p WHERE p.barcode = b.product_barcode ORDER BY id LIMIT 1) as name,
                   (SELECT SUM(stock) FROM products p WHERE p.barcode = b.product_barcode) as current_stock
            FROM product_batches b 
            WHERE b.quantity > 0 
        `;
        let params = [];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ` AND b.branch_id = $1`;
            params.push(req.user.store_id);
        }

        query += ` ORDER BY b.expiry_date ASC`;

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

// Stock Take Adjustment
app.post('/api/stock-take', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    const { adjustments } = req.body; // Array of { barcode, physicalQty }
    const userBranch = req.user.store_location || 'Main Warehouse';
    const client = await pool.connect();

    try {
        await client.query('BEGIN');
        for (const adj of adjustments) {
            // Get current stock info to calculate variance
            const resProd = await client.query('SELECT stock, stock_levels FROM products WHERE barcode = $1 AND tenant_id = $2', [adj.barcode, req.user.tenant_id]);
            if (resProd.rows.length === 0) continue;

            const product = resProd.rows[0];
            const stockLevels = product.stock_levels || {};
            const currentBranchStock = parseInt(stockLevels[userBranch] || 0);
            const newBranchStock = parseInt(adj.physicalQty);
            const variance = newBranchStock - currentBranchStock;

            if (variance !== 0) {
                await client.query(`
                    UPDATE products 
                    SET stock = COALESCE(stock, 0) + $1,
                        stock_levels = jsonb_set(
                            COALESCE(stock_levels, '{}'::jsonb), 
                            ARRAY[$2::text], 
                            to_jsonb($3::int)
                        )
                    WHERE barcode = $4
                `, [variance, userBranch, newBranchStock, adj.barcode]);

                // Log audit
                await client.query(`
                    INSERT INTO inventory_audit_log (action_type, product_barcode, quantity_before, quantity_after, reference_id, reference_type, user_id, branch_id, notes)
                    VALUES ('Stock Take', $1, $2, $3, NULL, 'Stock Take', $4, $5, 'Quick Update')
                `, [adj.barcode, currentBranchStock, newBranchStock, req.user.id, req.user.store_id || 1]);
            }
        }
        await client.query('COMMIT');
        await logActivity(req, 'STOCK_TAKE_UPDATE', { adjustmentsCount: adjustments.length });
        res.json({ success: true });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error(err);
        res.status(500).json({ message: 'Error processing stock take' });
    } finally { client.release(); }
});

// Reorder Suggestions
app.get('/api/inventory/reorder', authenticateToken, async (req, res) => {
    try {
        let query;
        let params = [];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_location) {
            // Branch specific check using stock_levels JSON
            query = `
                SELECT * FROM products 
                WHERE (COALESCE(stock_levels->>$1, '0'))::int < COALESCE(reorder_level, 10)
                ORDER BY (COALESCE(stock_levels->>$1, '0'))::int ASC
            `;
            params.push(req.user.store_location);
        } else {
            // Global check
            query = 'SELECT * FROM products WHERE stock < COALESCE(reorder_level, 10) ORDER BY stock ASC';
        }

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

// Negative Stock Analysis
app.get('/api/inventory/negative-analysis', authenticateToken, async (req, res) => {
    try {
        const isAdmin = req.user.role === 'admin' || req.user.role === 'ceo';
        const isCompany = req.user.type === 'company';

        let userBranch = req.user.store_location || 'Main Warehouse';
        const branchRes = await pool.query('SELECT name FROM branches WHERE name = $1 OR location = $1', [userBranch]);
        const branchStockKey = branchRes.rows[0]?.name || userBranch;

        // Fetch all products to safely parse JSON levels in Node.js
        const result = await pool.query(`SELECT id, barcode, name, stock, stock_levels, reorder_level, category FROM products`);

        let items = [];

        result.rows.forEach(row => {
            const levels = typeof row.stock_levels === 'string' ? JSON.parse(row.stock_levels) : (row.stock_levels || {});
            let checkStock = 0;

            if (isCompany) {
                checkStock = parseInt(
                    levels[branchStockKey] !== undefined ? levels[branchStockKey] :
                        levels[userBranch] !== undefined ? levels[userBranch] :
                            levels['NOVELTY'] !== undefined ? levels['NOVELTY'] :
                                levels['Novelty'] !== undefined ? levels['Novelty'] :
                                    levels['Dzorwulu'] !== undefined ? levels['Dzorwulu'] :
                                        levels['DZORWULU'] !== undefined ? levels['DZORWULU'] :
                                            levels['Main Warehouse'] !== undefined ? levels['Main Warehouse'] : 0
                );
            } else if (!isAdmin) {
                const manualFallback = req.user && req.user.store_location ? req.user.store_location : 'Main Warehouse';
                checkStock = parseInt(levels[branchStockKey] !== undefined ? levels[branchStockKey] : levels[manualFallback] !== undefined ? levels[manualFallback] : 0);
            } else {
                checkStock = parseInt(row.stock || 0); // Admins check global stock
            }

            if (checkStock < 0) {
                items.push({
                    id: row.id,
                    name: row.name,
                    barcode: row.barcode,
                    category: row.category,
                    stock: checkStock,
                    units_owed: Math.abs(checkStock),
                    debt_value: Math.abs(checkStock) * 10 // Estimated value
                });
            }
        });

        items.sort((a, b) => a.stock - b.stock); // Most negative first

        // Calculate summary
        const summary = {
            negative_items: items.length,
            total_units_owed: items.reduce((sum, item) => sum + item.units_owed, 0),
            total_debt_value: items.reduce((sum, item) => sum + item.debt_value, 0)
        };

        res.json({ items, summary });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// --- ANALYTICS & YTD ---
app.get('/api/analytics/ytd/all', authenticateToken, async (req, res) => {
    try {
        let query = `
            SELECT items, created_at 
            FROM transactions 
            WHERE status = 'completed' AND created_at >= date_trunc('year', CURRENT_DATE)
        `;
        let params = [];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_location) {
            query += ` AND store_location = $1`;
            params.push(req.user.store_location);
        }

        const result = await pool.query(query, params);

        let totalUnits = 0;
        let totalRevenue = 0;
        const monthlySales = {};
        const productBreakdown = {};

        // Initialize all months for a complete chart
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        months.forEach(m => monthlySales[m] = 0);

        result.rows.forEach(row => {
            const items = typeof row.items === 'string' ? JSON.parse(row.items) : row.items;
            const date = new Date(row.created_at);
            const month = date.toLocaleString('default', { month: 'short' });
            const isReturn = row.is_return || false;

            items.forEach(item => {
                const qty = isReturn ? -parseInt(item.qty) : parseInt(item.qty);
                const price = parseFloat(item.price);
                totalUnits += qty;
                totalRevenue += qty * price;
                if (monthlySales[month] !== undefined) monthlySales[month] += qty;

                // Breakdown logic
                if (!productBreakdown[item.barcode]) {
                    productBreakdown[item.barcode] = {
                        name: item.name,
                        barcode: item.barcode,
                        qty: 0,
                        revenue: 0
                    };
                }
                productBreakdown[item.barcode].qty += qty;
                productBreakdown[item.barcode].revenue += qty * price;
            });
        });

        const breakdown = Object.values(productBreakdown).sort((a, b) => b.revenue - a.revenue);
        res.json({ totalUnits, totalRevenue, monthlySales, breakdown });
    } catch (err) { console.error(err); res.status(500).json({ message: 'Server error' }); }
});

app.get('/api/analytics/ytd/:barcode', authenticateToken, async (req, res) => {
    const { barcode } = req.params;
    try {
        let query = `
            SELECT items, created_at 
            FROM transactions 
            WHERE status = 'completed' AND created_at >= date_trunc('year', CURRENT_DATE)
        `;
        let params = [];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_location) {
            query += ` AND store_location = $1`;
            params.push(req.user.store_location);
        }

        // Fetch all transactions for this year containing the product
        const result = await pool.query(query, params);

        let totalUnits = 0;
        let totalRevenue = 0;
        const monthlySales = {};

        // Initialize months for chart consistency
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        months.forEach(m => monthlySales[m] = 0);

        let productName = '';

        result.rows.forEach(row => {
            const items = typeof row.items === 'string' ? JSON.parse(row.items) : row.items;
            const date = new Date(row.created_at);
            const month = date.toLocaleString('default', { month: 'short' });
            const isReturn = row.is_return || false;

            items.forEach(item => {
                if (item.barcode === barcode) {
                    const qty = isReturn ? -parseInt(item.qty) : parseInt(item.qty);
                    const price = parseFloat(item.price);
                    totalUnits += qty;
                    totalRevenue += qty * price;
                    if (monthlySales[month] !== undefined) monthlySales[month] += qty;
                    if (!productName) productName = item.name;
                }
            });
        });

        const breakdown = [{
            name: productName || 'Unknown',
            barcode: barcode,
            qty: totalUnits,
            revenue: totalRevenue
        }];

        res.json({ barcode, totalUnits, totalRevenue, monthlySales, breakdown });
    } catch (err) { console.error(err); res.status(500).json({ message: 'Server error' }); }
});

// User Transactions Endpoint
app.get('/transactions/me', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    try {
        // Get current shift for the user
        const shiftRes = await pool.query(
            "SELECT id, start_time FROM shifts WHERE user_id = $1 AND end_time IS NULL",
            [req.user.id]
        );

        let query = "SELECT * FROM transactions WHERE user_id = $1 AND status = 'completed'";
        let params = [req.user.id];

        // If user has an active shift, filter transactions from shift start time
        if (shiftRes.rows.length > 0) {
            const currentShift = shiftRes.rows[0];
            query += " AND created_at >= $2";
            params.push(currentShift.start_time);
        }

        query += " ORDER BY created_at DESC";

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Get Single Transaction Endpoint
app.get('/transactions/:id', authenticateToken, async (req, res) => {
    console.log(`[DEBUG] Fetching transaction ID: ${req.params.id}`);
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    let { id } = req.params;
    const isReceiptFormat = id.startsWith('RCP') || id.startsWith('REF') || (id.length > 10 && !isNaN(id));

    try {
        let result;
        if (isReceiptFormat) {
            // Query by receipt_number column
            const receiptNum = (id.startsWith('RCP') || id.startsWith('REF')) ? id : 'RCP' + id;
            result = await pool.query(`
                SELECT t.*, CASE WHEN t.is_return THEN 'RETURN' ELSE 'SALE' END as type, u.name AS cashier_name, u.store_id, c.name AS customer_name, c.current_balance, c.credit_limit
                FROM transactions t
                LEFT JOIN users u ON t.user_id = u.id
                LEFT JOIN customers c ON t.customer_id = c.id
                WHERE t.receipt_number = $1
            `, [receiptNum]);
        } else {
            // Query by numeric ID
            const numericId = parseInt(id);
            if (isNaN(numericId)) return res.status(400).json({ message: 'Invalid transaction ID format' });

            result = await pool.query(`
            SELECT t.*, CASE WHEN t.is_return THEN 'RETURN' ELSE 'SALE' END as type, u.name AS cashier_name, u.store_id, c.name AS customer_name, c.current_balance, c.credit_limit
            FROM transactions t
            LEFT JOIN users u ON t.user_id = u.id
            LEFT JOIN customers c ON t.customer_id = c.id
            WHERE t.id = $1
            `, [numericId]);
        }

        if (result.rows.length === 0) {
            return res.status(404).json({ message: 'Transaction not found' });
        }

        const txn = result.rows[0];
        let taxBreakdown = [];
        let totalTax = 0;
        let subtotal = 0;

        // Use stored breakdown if available (New System)
        // Check for non-null to distinguish new transactions (even with 0 tax) from legacy ones
        if (txn.tax_breakdown !== null) {
            taxBreakdown = Array.isArray(txn.tax_breakdown) ? txn.tax_breakdown : [];
            totalTax = taxBreakdown.reduce((sum, t) => sum + parseFloat(t.amount), 0);
            subtotal = parseFloat(txn.total_amount) - totalTax;
        } else {
            // Legacy Fallback (Only for old transactions where tax_breakdown is NULL)
            const branchId = txn.store_id || req.user.store_id || 1;
            let settingsRes = await pool.query('SELECT vat_rate FROM system_settings WHERE branch_id = $1', [branchId]);
            if (settingsRes.rows.length === 0) settingsRes = await pool.query('SELECT vat_rate FROM system_settings WHERE branch_id = 1');
            const vatRate = settingsRes.rows.length > 0 ? parseFloat(settingsRes.rows[0].vat_rate) : 15.0;

            const total = parseFloat(txn.total_amount);
            totalTax = total - (total / (1 + (vatRate / 100)));
            subtotal = total - totalTax;

            taxBreakdown = [{ name: `VAT (${vatRate}%)`, amount: totalTax }];
        }

        await logActivity(req, 'VIEW_TRANSACTION_DETAIL', { id });
        res.json({
            ...txn,
            subtotal,
            tax: totalTax,
            taxBreakdown,
            discount: 0
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Finalize Transaction Endpoint
app.put('/transactions/:id/finalize', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    const { id } = req.params;
    const { paymentMethod, receiptNumber, customerId } = req.body;

    // Convert customerId to integer if provided
    const parsedCustomerId = customerId ? parseInt(customerId) : null;
    console.log('💳 Finalizing transaction with customerId:', parsedCustomerId, '(type:', typeof parsedCustomerId, ')');

    try {
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            let customerName = null;
            // If paying by credit, update customer balance and get customer name
            if (paymentMethod === 'credit' && parsedCustomerId) {
                const txnRes = await client.query('SELECT total_amount, receipt_number, is_return FROM transactions WHERE id = $1', [id]);
                const total = parseFloat(txnRes.rows[0].total_amount);
                const isReturn = txnRes.rows[0].is_return || false;
                const receiptNum = receiptNumber || txnRes.rows[0].receipt_number || ('RCP' + Date.now());

                // Get customer details
                const custDetailRes = await client.query('SELECT name, current_balance, credit_limit FROM customers WHERE id = $1', [parsedCustomerId]);
                if (custDetailRes.rows.length === 0) throw new Error('Customer not found');

                customerName = custDetailRes.rows[0].name;

                // For returns, subtract from balance (reduce debt). For sales, add to balance.
                const balanceChange = isReturn ? -total : total;

                const custRes = await client.query(
                    'UPDATE customers SET current_balance = current_balance + $1 WHERE id = $2 RETURNING current_balance',
                    [balanceChange, parsedCustomerId]
                );
                const newBalance = custRes.rows[0].current_balance;

                // Add to Ledger (Part 2)
                const ledgerDesc = isReturn ? `Credit Return - ${receiptNum}` : `Credit Sale - ${receiptNum}`;
                const ledgerType = isReturn ? 'RETURN' : 'SALE';

                await client.query(
                    `INSERT INTO customer_ledger (customer_id, date, description, type, debit, credit, balance, transaction_id) 
                     VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7)`,
                    [parsedCustomerId, ledgerDesc, ledgerType, isReturn ? 0 : total, isReturn ? total : 0, newBalance, id]
                );
            }

            await client.query(
                "UPDATE transactions SET payment_method = $1, receipt_number = $2, customer_id = $3, customer_name = $4, status = 'completed' WHERE id = $5",
                [paymentMethod, receiptNumber, parsedCustomerId || null, customerName, id]
            );
            await client.query('COMMIT');
        } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }

        await logActivity(req, 'FINALIZE_TRANSACTION', { id, paymentMethod, receiptNumber });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Get Transaction by Receipt Number (for returns)
app.get('/transactions/receipt/:receiptNumber', authenticateToken, async (req, res) => {
    console.log(`[DEBUG] Fetching transaction by receipt: ${req.params.receiptNumber}`);
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    const { receiptNumber } = req.params;

    try {
        let result;

        // Try to determine if the input is numeric or text
        const isNumeric = /^\d+$/.test(receiptNumber);

        if (isNumeric) {
            // Search by ID if input is numeric
            result = await pool.query(`
                SELECT t.*, u.name AS cashier_name, u.store_id, c.name AS customer_name, c.current_balance, c.credit_limit
                FROM transactions t
                LEFT JOIN users u ON t.user_id = u.id
                LEFT JOIN customers c ON t.customer_id = c.id
                WHERE t.id = $1 AND t.status = 'completed'
            `, [receiptNumber]);
        } else {
            // Search by receipt_number if input contains letters
            result = await pool.query(`
                SELECT t.*, u.name AS cashier_name, u.store_id, c.name AS customer_name, c.current_balance, c.credit_limit
                FROM transactions t
                LEFT JOIN users u ON t.user_id = u.id
                LEFT JOIN customers c ON t.customer_id = c.id
                WHERE t.receipt_number = $1 AND t.status = 'completed'
            `, [receiptNumber]);
        }

        if (result.rows.length === 0) {
            // FALLBACK: Search in sales_invoices (Company Portal Quick Sales)
            const invoiceResult = await pool.query(`
                SELECT 
                    si.id, 
                    si.invoice_number as receipt_number, 
                    si.total_amount, 
                    si.subtotal, 
                    si.tax_amount as tax,
                    si.tax_details as tax_breakdown,
                    si.created_at, 
                    si.client_name as customer_name,
                    si.payment_method,
                    COALESCE(u.name, 'Admin') as cashier_name,
                    'SALE' as type,
                    (SELECT COALESCE(json_agg(json_build_object(
                        'name', product_name,
                        'qty', quantity,
                        'price', unit_price,
                        'barcode', barcode,
                        'total', line_total
                    )), '[]'::json) FROM sales_invoice_items WHERE invoice_id = si.id) as items
                FROM sales_invoices si
                LEFT JOIN users u ON si.created_by = u.id
                WHERE si.invoice_number = $1 OR si.id::text = $1
            `, [receiptNumber]);

            if (invoiceResult.rows.length > 0) {
                result = invoiceResult;
            } else {
                return res.status(404).json({ message: 'Transaction not found' });
            }
        }

        const txn = result.rows[0];
        let taxBreakdown = [];
        let totalTax = 0;
        let subtotal = 0;

        // Use stored breakdown if available (New System)
        if (txn.tax_breakdown !== null) {
            taxBreakdown = Array.isArray(txn.tax_breakdown) ? txn.tax_breakdown : [];
            totalTax = taxBreakdown.reduce((sum, t) => sum + parseFloat(t.amount), 0);
            subtotal = parseFloat(txn.total_amount) - totalTax;
        } else {
            // Legacy fallback
            totalTax = parseFloat(txn.tax) || 0;
            subtotal = parseFloat(txn.total_amount) - totalTax;
        }

        const finalTransaction = {
            ...txn,
            subtotal: subtotal,
            tax: totalTax,
            taxBreakdown: taxBreakdown
        };

        res.json(finalTransaction);
    } catch (err) {
        console.error('Error fetching transaction by receipt:', err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Get All Return Transactions
app.get('/api/transactions/returns', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    try {
        const result = await pool.query(`
            SELECT t.*, u.name AS cashier_name, u.store_id, c.name AS customer_name, c.current_balance, c.credit_limit
            FROM transactions t
            LEFT JOIN users u ON t.user_id = u.id
            LEFT JOIN customers c ON t.customer_id = c.id
            WHERE t.is_return = TRUE AND t.status = 'completed'
            ORDER BY t.created_at DESC
        `);

        res.json({ returns: result.rows });
    } catch (err) {
        console.error('Error fetching returns:', err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Get Sales Summary (Accounts for Returns)
app.get('/api/sales-summary', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });

    try {
        const { startDate, endDate, branchId } = req.query;

        // Build WHERE clause
        let whereClause = 'WHERE t.status = $1';
        let params = ['completed'];
        let paramIndex = 2;

        if (startDate) {
            whereClause += ` AND t.created_at >= $${paramIndex}`;
            params.push(startDate);
            paramIndex++;
        }

        if (endDate) {
            whereClause += ` AND t.created_at <= $${paramIndex}`;
            params.push(endDate);
            paramIndex++;
        }

        if (branchId && req.user.role !== 'admin' && req.user.role !== 'ceo') {
            whereClause += ` AND t.user_id IN (SELECT id FROM users WHERE store_id = $${paramIndex})`;
            params.push(branchId);
            paramIndex++;
        }

        // Get sales data with returns accounted for
        const query = `
            SELECT 
                COUNT(*) as total_transactions,
                COUNT(CASE WHEN t.is_return = FALSE THEN 1 END) as sales_transactions,
                COUNT(CASE WHEN t.is_return = TRUE THEN 1 END) as return_transactions,
                COALESCE(SUM(CASE WHEN t.is_return = FALSE THEN t.total_amount ELSE 0 END), 0) as gross_sales,
                COALESCE(SUM(CASE WHEN t.is_return = TRUE THEN t.total_amount ELSE 0 END), 0) as returns_amount,
                COALESCE(SUM(CASE WHEN t.is_return = FALSE THEN t.total_amount ELSE 0 END), 0) + 
                COALESCE(SUM(CASE WHEN t.is_return = TRUE THEN t.total_amount ELSE 0 END), 0) as net_sales,
                COALESCE(SUM(t.total_amount), 0) as total_amount
            FROM transactions t
            ${whereClause}
        `;

        const result = await pool.query(query, params);

        // Get top selling products (excluding returns)
        const productsQuery = `
            SELECT 
                p.name,
                SUM(CASE WHEN t.is_return = FALSE THEN (item->>'qty')::int ELSE 0 END) as total_sold,
                SUM(CASE WHEN t.is_return = TRUE THEN (item->>'qty')::int ELSE 0 END) as total_returned,
                SUM(CASE WHEN t.is_return = FALSE THEN (item->>'qty')::int ELSE 0 END) - 
                SUM(CASE WHEN t.is_return = TRUE THEN (item->>'qty')::int ELSE 0 END) as net_sold
            FROM transactions t, jsonb_array_elements(t.items) as item
            JOIN products p ON p.barcode = item->>'barcode'
            ${whereClause}
            GROUP BY p.name
            ORDER BY net_sold DESC
            LIMIT 10
        `;

        const productsResult = await pool.query(productsQuery, params);

        res.json({
            summary: result.rows[0],
            topProducts: productsResult.rows
        });

    } catch (err) {
        console.error('Error fetching sales summary:', err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Mock Mobile Money Payment Endpoints
app.post('/pay/momo', (req, res) => {
    res.json({
        success: true,
        transactionId: 'MOMO-' + Date.now(),
        message: 'Payment initiated'
    });
});

app.get('/pay/momo/status/:id', (req, res) => {
    res.json({ status: 'SUCCESSFUL' });
});

// --- DEBUG: Check Database Schema ---
app.get('/api/debug/schema/:tableName', async (req, res) => {
    try {
        const result = await pool.query(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1",
            [req.params.tableName]
        );
        res.json(result.rows);
    } catch (err) { res.status(500).json(err); }
});

// Helper function to get branch name from user location
async function getBranchNameFromLocation(userLocation, pool) {
    try {
        const branchesRes = await pool.query('SELECT id, name, location FROM branches');
        const branches = branchesRes.rows;

        // Check if user location matches any branch name or location
        const match = branches.find(b =>
            b.name === userLocation ||
            b.location === userLocation
        );

        return match ? match.name : userLocation;
    } catch (err) {
        console.error('Error getting branch mapping:', err);
        return userLocation;
    }
}

// --- BRANCH MAPPING ENDPOINT ---
app.get('/api/branch-mapping', authenticateToken, async (req, res) => {
    try {
        const userLocation = req.user.store_location;

        // Get all branches to find the matching one
        const branchesRes = await pool.query('SELECT id, name, location FROM branches');
        const branches = branchesRes.rows;

        // Try to find a match
        let mappedLocation = userLocation;

        // Check if user location matches any branch name or location
        const match = branches.find(b =>
            b.name === userLocation ||
            b.location === userLocation
        );

        if (match) {
            // Use the branch name for stock lookup (as stored in stock_levels JSON)
            mappedLocation = match.name;
        }

        res.json({
            originalLocation: userLocation,
            mappedLocation: mappedLocation,
            branches: branches.map(b => ({ id: b.id, name: b.name, location: b.location }))
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// --- DEBUG: Role Emulator / Switcher ---
app.post('/api/debug/switch-role', authenticateToken, async (req, res) => {
    const { role } = req.body;
    try {
        let targetRole = role.toLowerCase();
        if (targetRole === 'teller') targetRole = 'cashier';

        // Find a user with this role
        let result = await pool.query("SELECT * FROM users WHERE role = $1 LIMIT 1", [targetRole]);

        // Fallback for Manager -> Admin if no manager exists
        if (result.rows.length === 0 && targetRole === 'manager') {
            result = await pool.query("SELECT * FROM users WHERE role = 'admin' LIMIT 1");
        }

        if (result.rows.length === 0) {
            return res.status(404).json({ message: `No user found for role: ${targetRole}` });
        }

        const user = result.rows[0];
        req.session.user = {
            id: user.id,
            email: user.email,
            name: user.name,
            role: user.role,
            store_id: user.store_id,
            store_location: user.store_location
        };

        let redirectTo = '/open-shift';
        if (user.role === 'admin' || user.role === 'manager') redirectTo = '/dashboard';
        if (user.role === 'ceo') redirectTo = '/ceo-portal';

        const token = jwt.sign(req.session.user, JWT_SECRET, { expiresIn: '7d' });
        await logActivity(req, 'DEBUG_SWITCH_ROLE', { targetRole });

        res.json({ success: true, token, redirectTo: redirectTo.replace(/^\//, ''), user: req.session.user });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error switching role' });
    }
});

// ============ CEO PORTAL INTELLIGENCE ============

// 1. Executive Financial Pulse (Global View)
app.get('/api/ceo/financials', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });

    const { branch, startDate, endDate } = req.query;

    try {
        // Build Dynamic Filters
        let txnWhere = "LOWER(status) = 'completed'";
        let txnParams = [];
        let expenseWhere = "1=1";
        let expenseParams = [];

        // Date Filtering
        if (startDate && endDate) {
            txnWhere += ` AND created_at >= $${txnParams.length + 1} AND created_at <= $${txnParams.length + 2}::date + INTERVAL '1 day' - INTERVAL '1 second'`;
            txnParams.push(startDate, endDate);

            expenseWhere += ` AND expense_date >= $${expenseParams.length + 1} AND expense_date <= $${expenseParams.length + 2}`;
            expenseParams.push(startDate, endDate);
        } else {
            // Default to current month if no dates provided
            txnWhere += " AND created_at >= date_trunc('month', CURRENT_DATE)";
            expenseWhere += " AND expense_date >= date_trunc('month', CURRENT_DATE)";
        }

        if (branch && branch !== 'All Branches') {
            txnWhere += ` AND store_location = $${txnParams.length + 1}`;
            txnParams.push(branch);

            // Resolve branch_id for expenses (Expenses use ID, Transactions use Location Name)
            const branchRes = await pool.query("SELECT id FROM branches WHERE location = $1 OR name = $1", [branch]);
            if (branchRes.rows.length > 0) {
                expenseWhere += ` AND branch_id = $${expenseParams.length + 1}`;
                expenseParams.push(branchRes.rows[0].id);
            }
        }

        // Monthly Revenue
        const revenueRes = await pool.query(`
            SELECT COALESCE(SUM(total_amount), 0) as revenue 
            FROM transactions 
            WHERE ${txnWhere}
        `, txnParams);

        // Cost of Goods Sold (Estimate: 70% of revenue for V1 until historical cost tracking is robust)
        const revenue = parseFloat(revenueRes.rows[0].revenue || 0);
        const cogs = revenue * 0.7;

        // Operational Expenses (Monthly)
        const expensesRes = await pool.query(`
            SELECT COALESCE(SUM(amount), 0) as total 
            FROM expenses
            WHERE ${expenseWhere}
        `, expenseParams);
        const opex = parseFloat(expensesRes.rows[0].total);

        // --- NEW: Company Portal Revenue (All Time) ---
        const companyRevRes = await pool.query('SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_invoices');
        const companyRevenue = parseFloat(companyRevRes.rows[0].total) || 0;

        // Total Asset Value (Current Stock * Cost Price)
        let assetValue = 0;
        if (branch && branch !== 'All Branches') {
            // Calculate assets for specific branch using stock_levels JSON
            const assetsRes = await pool.query(`
                SELECT SUM(COALESCE((stock_levels->>$1)::int, 0) * cost_price) as asset_value FROM products
            `, [branch]);
            assetValue = parseFloat(assetsRes.rows[0].asset_value) || 0;
        } else {
            // Global assets
            const assetsRes = await pool.query(`SELECT SUM(stock * cost_price) as asset_value FROM products`);
            assetValue = parseFloat(assetsRes.rows[0].asset_value) || 0;
        }

        // Active Branches Count
        const branchRes = await pool.query("SELECT COUNT(*) as count FROM branches");
        const branchCount = parseInt(branchRes.rows[0].count || 0);

        // Fetch Monthly Target (Global from ID 1)
        let target = 50000;
        const targetRes = await pool.query("SELECT monthly_target FROM system_settings WHERE id = 1");
        if (targetRes.rows.length > 0 && targetRes.rows[0].monthly_target) {
            target = parseFloat(targetRes.rows[0].monthly_target);
        }

        // Top 5 Products by Revenue (Monthly)
        const topProductsRes = await pool.query(`
            SELECT 
                item->>'name' as name,
                SUM(CASE WHEN is_return = TRUE THEN -1 ELSE 1 END * (item->>'qty')::int) as units_sold,
                SUM(CASE WHEN is_return = TRUE THEN -1 ELSE 1 END * ((item->>'qty')::int * (item->>'price')::numeric)) as revenue
            FROM transactions, jsonb_array_elements(items) as item
            WHERE ${txnWhere}
            GROUP BY item->>'name'
            ORDER BY revenue DESC
            LIMIT 5
        `, txnParams);

        // Slow Moving Products (Bottom 5 by Quantity Sold)
        const slowMovingRes = await pool.query(`
            SELECT 
                item->>'name' as name,
                SUM(CASE WHEN is_return = TRUE THEN -1 ELSE 1 END * (item->>'qty')::int) as units_sold,
                SUM(CASE WHEN is_return = TRUE THEN -1 ELSE 1 END * ((item->>'qty')::int * (item->>'price')::numeric)) as revenue
            FROM transactions, jsonb_array_elements(items) as item
            WHERE ${txnWhere}
            GROUP BY item->>'name'
            ORDER BY units_sold ASC
            LIMIT 5
        `, txnParams);

        let slowMoving = slowMovingRes.rows;
        const topProducts = topProductsRes.rows;

        // INTELLIGENT FILL: If "Slow Moving" is identical to "Top Selling" (due to low data volume),
        // fetch "Dead Stock" (Unsold items with stock) instead.
        const topNames = new Set(topProducts.map(p => p.name));
        const allSlowAreTop = slowMoving.length > 0 && slowMoving.every(p => topNames.has(p.name));

        if (allSlowAreTop || slowMoving.length === 0) {
            const excludedNames = [...topNames];
            let deadStockQuery = "SELECT name, 0 as units_sold, 0 as revenue FROM products WHERE stock > 0";
            let deadStockParams = [];

            if (excludedNames.length > 0) {
                const placeholders = excludedNames.map((_, i) => `$${i + 1}`).join(',');
                deadStockQuery += ` AND name NOT IN (${placeholders})`;
                deadStockParams = excludedNames;
            }

            deadStockQuery += " ORDER BY stock DESC LIMIT 5";
            const deadStockRes = await pool.query(deadStockQuery, deadStockParams);
            if (deadStockRes.rows.length > 0) slowMoving = deadStockRes.rows;
        }

        // Payment Method Breakdown (Monthly)
        const paymentRes = await pool.query(`
            SELECT payment_method, SUM(total_amount) as total
            FROM transactions
            WHERE ${txnWhere}
            GROUP BY payment_method
        `, txnParams);

        const grossProfit = revenue - cogs;
        const netProfit = grossProfit - opex;

        await logActivity(req, 'VIEW_CEO_FINANCIALS');
        res.json({
            revenue,
            companyRevenue,
            cogs,
            opex,
            grossProfit,
            netProfit,
            assetValue,
            branchCount,
            target,
            topProducts: topProductsRes.rows,
            slowMoving: slowMoving,
            paymentStats: paymentRes.rows
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// 1b. Update Monthly Revenue Target
app.post('/api/ceo/target', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    const { target } = req.body;
    try {
        await pool.query(`
            INSERT INTO system_settings (id, branch_id, store_name, currency_symbol, vat_rate, receipt_footer, monthly_target)
            VALUES (1, 1, 'Footprint Retail', '₵ (GHS)', 15.00, 'Thank you!', $1)
            ON CONFLICT (id) DO UPDATE SET monthly_target = $1
        `, [target]);
        await logActivity(req, 'UPDATE_REVENUE_TARGET', { target });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// 2. Branch Performance Matrix
app.get('/api/ceo/branches', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });

    const { startDate, endDate } = req.query;

    try {
        let whereClause = "LOWER(status) = 'completed'";
        let params = [];

        if (startDate && endDate) {
            whereClause += " AND created_at >= $1 AND created_at <= $2::date + INTERVAL '1 day' - INTERVAL '1 second'";
            params.push(startDate, endDate);
        }

        const result = await pool.query(`
            SELECT 
                COALESCE(store_location, 'Main Branch') as branch,
                COUNT(CASE WHEN is_return = FALSE THEN 1 END) as txn_count,
                SUM(total_amount) as revenue
            FROM transactions
            WHERE ${whereClause}
            GROUP BY store_location
            ORDER BY revenue DESC
        `, params);
        await logActivity(req, 'VIEW_CEO_BRANCHES');
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ message: 'Server error' });
    }
});

// 3. Risk & Audit Radar
app.get('/api/ceo/risks', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });

    try {
        const risks = [];

        // Risk: Low Stock Only
        const lowStockRes = await pool.query(`
            SELECT COUNT(*) as count FROM products WHERE stock <= reorder_level
        `);

        if (parseInt(lowStockRes.rows[0].count) > 0) {
            risks.push({ type: 'Stockout Risk', message: `${lowStockRes.rows[0].count} items are critically low on stock.`, severity: 'high' });
        }

        await logActivity(req, 'VIEW_CEO_RISKS');
        res.json(risks);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// 4. Analytics Trend (Revenue vs Cost vs Forecast)
app.get('/api/ceo/analytics/trend', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        // Get last 7 days revenue
        const result = await pool.query(`
            SELECT TO_CHAR(created_at, 'Dy') as day, SUM(total_amount) as revenue
            FROM transactions 
            WHERE LOWER(status) = 'completed' AND created_at >= NOW() - INTERVAL '7 days'
            GROUP BY day, DATE(created_at)
            ORDER BY DATE(created_at)
        `);

        // Fill in data (mocking costs/forecast for demo visualization)
        const labels = result.rows.map(r => r.day);
        const revenue = result.rows.map(r => parseFloat(r.revenue));
        const costs = revenue.map(r => r * 0.7); // Est. 70% cost
        const forecast = revenue.map(r => r * 1.1); // Simple +10% forecast

        await logActivity(req, 'VIEW_CEO_ANALYTICS');
        res.json({ labels, revenue, costs, forecast });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Server error" });
    }
});

// 5. Pending Credit Approvals
app.get('/api/ceo/approvals/credit-customers', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const result = await pool.query(`
            SELECT c.*, u.name as created_by_name, u.store_location as branch,
            CASE 
                WHEN c.pending_credit_limit IS NOT NULL THEN 'Limit Increase' 
                ELSE 'New Account' 
            END as request_type
            FROM customers c
            LEFT JOIN users u ON c.created_by = u.id
            WHERE c.status = 'Pending' OR c.pending_credit_limit IS NOT NULL
            ORDER BY c.created_at DESC
        `);
        await logActivity(req, 'VIEW_CEO_APPROVALS_PENDING');
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Server error" });
    }
});

// 5b. Credit Approval History
app.get('/api/ceo/approvals/history', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const result = await pool.query("SELECT * FROM customers WHERE status IN ('Active', 'Rejected') ORDER BY created_at DESC");
        await logActivity(req, 'VIEW_CEO_APPROVALS_HISTORY');
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Server error" });
    }
});

// --- DATA FIX ENDPOINT (Run once to fix Dale Shelly) ---
app.get('/api/fix-dale', async (req, res) => {
    try {
        // 1. Ensure Manager has correct store
        await pool.query("UPDATE users SET store_location = 'Accra Central' WHERE username = 'gzain'");

        // 2. Link Customer to Manager
        await pool.query("UPDATE customers SET created_by = (SELECT id FROM users WHERE username = 'gzain') WHERE name = 'Dale Shelly'");

        res.send("Data fixed for Dale Shelly. Please refresh the CEO Portal.");
    } catch (e) { res.status(500).send(e.message); }
});
// ------------------------------------------------------

// 5b. Credit Approval History
app.get('/api/ceo/approvals/history', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const result = await pool.query("SELECT * FROM customers WHERE status IN ('Active', 'Rejected') ORDER BY created_at DESC");
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Server error" });
    }
});

// 6. Approve Credit Customer
app.post('/api/ceo/approvals/credit-customers/:id/approve', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const { id } = req.params;

        const client = await pool.connect();
        try {
            await client.query('BEGIN');
            // Apply pending limit if exists
            await client.query('UPDATE customers SET credit_limit = COALESCE(pending_credit_limit, credit_limit), pending_credit_limit = NULL WHERE id = $1 AND pending_credit_limit IS NOT NULL', [id]);
            // Activate if pending
            await client.query("UPDATE customers SET status = 'Active' WHERE id = $1 AND status = 'Pending'", [id]);
            await client.query('COMMIT');
        } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }

        await logActivity(req, 'APPROVE_CREDIT_CUSTOMER', { customerId: id });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Server error" });
    }
});

// 7. Reject Credit Customer
app.post('/api/ceo/approvals/credit-customers/:id/reject', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const { id } = req.params;

        const client = await pool.connect();
        try {
            await client.query('BEGIN');
            // Clear pending limit
            await client.query('UPDATE customers SET pending_credit_limit = NULL WHERE id = $1', [id]);
            // Reject if pending status
            await client.query("UPDATE customers SET status = 'Rejected' WHERE id = $1 AND status = 'Pending'", [id]);
            await client.query('COMMIT');
        } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }

        await logActivity(req, 'REJECT_CREDIT_CUSTOMER', { customerId: id });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Server error" });
    }
});

// 12. Peak Sales Hours (New)
app.get('/api/ceo/analytics/peak-hours', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const result = await pool.query(`
            SELECT EXTRACT(HOUR FROM created_at) as hour, COUNT(*) as count, SUM(total_amount) as revenue
            FROM transactions
            WHERE status = 'completed' AND created_at >= date_trunc('month', CURRENT_DATE)
            GROUP BY hour
            ORDER BY hour
        `);
        await logActivity(req, 'VIEW_CEO_PEAK_HOURS');
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: "Server error" }); }
});

// 13. High Value Transactions (New)
app.get('/api/ceo/transactions/high-value', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const result = await pool.query(`
            SELECT t.*, u.name as cashier_name 
            FROM transactions t
            LEFT JOIN users u ON t.user_id = u.id
            WHERE t.status = 'completed' AND t.created_at >= date_trunc('month', CURRENT_DATE)
            ORDER BY t.total_amount DESC
            LIMIT 5
        `);
        await logActivity(req, 'VIEW_CEO_HIGH_VALUE_TXNS');
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: "Server error" }); }
});

// 8. Staff Performance
app.get('/api/ceo/staff-performance', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const result = await pool.query(`
            SELECT u.name, COUNT(CASE WHEN t.is_return = FALSE THEN 1 END) as transactions, SUM(t.total_amount) as sales
            FROM users u
            LEFT JOIN transactions t ON u.id = t.user_id
            WHERE t.status = 'completed' AND t.created_at >= date_trunc('month', CURRENT_DATE)
            GROUP BY u.id, u.name
            ORDER BY sales DESC
            LIMIT 10
        `);

        const staff = result.rows.map(r => ({
            name: r.name,
            transactions: r.transactions,
            sales: r.sales || 0,
            performance: parseFloat(r.sales) > 5000 ? 'High' : 'Normal'
        }));

        const countRes = await pool.query("SELECT COUNT(*) FROM users WHERE status = 'Active'");

        await logActivity(req, 'VIEW_CEO_STAFF_PERFORMANCE');
        res.json({ staff, headcount: countRes.rows[0].count });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Server error" });
    }
});

// 9. Low Stock (CEO View)
app.get('/api/ceo/inventory/low-stock', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const result = await pool.query(`
            SELECT name, stock, reorder_level 
            FROM products 
            WHERE stock <= reorder_level 
            ORDER BY stock ASC 
            LIMIT 10
        `);
        await logActivity(req, 'VIEW_CEO_LOW_STOCK');
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Server error" });
    }
});

// 10. Tech Status (Mock)
app.get('/api/ceo/tech-status', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    await logActivity(req, 'VIEW_CEO_TECH_STATUS');
    res.json([
        { service: 'Database Server', status: 'Operational', latency: '24ms', lastAudit: 'Today' },
        { service: 'API Gateway', status: 'Operational', latency: '45ms', lastAudit: 'Today' },
        { service: 'Backup Systems', status: 'Operational', latency: '-', lastAudit: 'Yesterday' },
        { service: 'Payment Gateway', status: 'Operational', latency: '120ms', lastAudit: 'Today' }
    ]);
});

// 11. Strategy (Mock)
app.get('/api/ceo/strategy', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    await logActivity(req, 'VIEW_CEO_STRATEGY');
    res.json([
        { initiative: 'Northern Expansion', owner: 'COO', budget: '₵500k', burnRate: '12%', status: 'On Track' },
        { initiative: 'Digital Transformation', owner: 'CTO', budget: '₵200k', burnRate: '45%', status: 'At Risk' },
        { initiative: 'Staff Training Q1', owner: 'HR', budget: '₵50k', burnRate: '80%', status: 'Completed' }
    ]);
});

// ============ CEO: Aggregation endpoints (paged, branch-aware) ============

// GET /api/ceo/suppliers?branch=&limit=&offset=
app.get('/api/ceo/suppliers', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const { branch, limit = 50, offset = 0 } = req.query;
        const lim = Math.min(parseInt(limit) || 50, 100);
        const off = parseInt(offset) || 0;

        let params = [];
        let where = '';
        if (branch && branch !== 'All Branches') {
            // try resolve branch id by name or location
            const branchRes = await pool.query('SELECT id FROM branches WHERE name = $1 OR location = $1 LIMIT 1', [branch]);
            if (branchRes.rows.length > 0) {
                params.push(branchRes.rows[0].id);
                where = 'WHERE s.branch_id = $1';
            } else {
                // fallback to no-match
                where = 'WHERE 1=0';
            }
        }

        const q = `SELECT s.id, s.name, s.contact_person, s.phone, s.email, s.address, s.branch_id FROM suppliers s ${where} ORDER BY s.name LIMIT ${lim} OFFSET ${off}`;
        const result = await pool.query(q, params);
        const data = { items: result.rows, count: result.rowCount };

        await logActivity(req, 'VIEW_CEO_SUPPLIERS', { branch });
        res.json(data);
    } catch (e) { console.error(e); res.status(500).json({ message: 'Server error' }); }
});

// GET /api/ceo/purchase-orders?branch=&status=&limit=&offset=
app.get('/api/ceo/purchase-orders', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const { branch, status, limit = 50, offset = 0 } = req.query;
        const lim = Math.min(parseInt(limit) || 50, 200);
        const off = parseInt(offset) || 0;

        const params = [];
        let whereClauses = [];
        if (status) { params.push(status); whereClauses.push(`po.status = $${params.length}`); }
        if (branch && branch !== 'All Branches') {
            const branchRes = await pool.query('SELECT id FROM branches WHERE name = $1 OR location = $1 LIMIT 1', [branch]);
            if (branchRes.rows.length > 0) { params.push(branchRes.rows[0].id); whereClauses.push(`po.branch_id = $${params.length}`); }
            else whereClauses.push('1=0');
        }

        const where = whereClauses.length ? ('WHERE ' + whereClauses.join(' AND ')) : '';
        const q = `SELECT po.id, po.supplier_id, po.total_amount, po.status, po.created_at, s.name as supplier_name, po.branch_id, b.name as branch_name FROM purchase_orders po LEFT JOIN suppliers s ON po.supplier_id = s.id LEFT JOIN branches b ON po.branch_id = b.id ${where} ORDER BY po.created_at DESC LIMIT ${lim} OFFSET ${off}`;
        const result = await pool.query(q, params);
        const data = { items: result.rows, count: result.rowCount };

        await logActivity(req, 'VIEW_CEO_POS', { branch, status });
        res.json(data);
    } catch (e) { console.error(e); res.status(500).json({ message: 'Server error' }); }
});

// GET /api/ceo/top-products?branch=&start=&end=&limit=
app.get('/api/ceo/top-products', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const { branch, start, end, limit = 10 } = req.query;
        const lim = Math.min(parseInt(limit) || 10, 100);
        const startDate = start || (new Date(new Date().getFullYear(), new Date().getMonth(), 1)).toISOString();
        const endDate = end || new Date().toISOString();

        const params = [startDate, endDate];
        let branchFilter = '';
        if (branch && branch !== 'All Branches') {
            params.push(branch);
            branchFilter = `AND store_location = $${params.length}`;
        }
        const q = `
            SELECT item->>'name' as name, SUM(CASE WHEN is_return = TRUE THEN -1 ELSE 1 END * (item->>'qty')::int) as qty, SUM(CASE WHEN is_return = TRUE THEN -1 ELSE 1 END * ((item->>'qty')::int * (item->>'price')::numeric)) as revenue
            FROM transactions, jsonb_array_elements(items) as item
            WHERE status = 'completed' AND created_at BETWEEN $1 AND $2 ${branchFilter}
            GROUP BY item->>'name'
            ORDER BY revenue DESC
            LIMIT ${lim}
        `;
        const result = await pool.query(q, params);
        const data = { items: result.rows };

        await logActivity(req, 'VIEW_CEO_TOP_PRODUCTS', { branch, start: startDate, end: endDate });
        res.json(data);
    } catch (e) { console.error(e); res.status(500).json({ message: 'Server error' }); }
});

// GET /api/ceo/branch-performance?start=&end=&limit=
app.get('/api/ceo/branch-performance', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const { start, end, limit = 50 } = req.query;
        const lim = Math.min(parseInt(limit) || 50, 500);
        const startDate = start || (new Date(new Date().getFullYear(), new Date().getMonth(), 1)).toISOString();
        const endDate = end || new Date().toISOString();

        const q = `
            SELECT COALESCE(store_location, 'Main Branch') as branch, COUNT(CASE WHEN is_return = FALSE THEN 1 END) as txn_count, SUM(total_amount) as revenue
            FROM transactions
            WHERE status = 'completed' AND created_at BETWEEN $1 AND $2
            GROUP BY store_location
            ORDER BY revenue DESC
            LIMIT ${lim}
        `;
        const result = await pool.query(q, [startDate, endDate]);
        const data = result.rows;

        await logActivity(req, 'VIEW_CEO_BRANCH_PERFORMANCE', { start: startDate, end: endDate });
        res.json(data);
    } catch (e) { console.error(e); res.status(500).json({ message: 'Server error' }); }
});

// Create Transaction Endpoint
app.post('/api/transactions', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });
    if (!req.body) return res.status(400).json({ message: 'Invalid request: No body provided' });

    const { items, total, refundAmount, paymentMethod, promoCode, discount, customerId, taxBreakdown, status, isReturn, originalTransactionId, returnItems } = req.body;

    try {
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            // Convert customerId to integer if provided
            const parsedCustomerId = customerId ? parseInt(customerId) : null;
            console.log('📝 Creating transaction with customerId:', parsedCustomerId, '(type:', typeof parsedCustomerId, ')');
            console.log('🔄 Is Return:', isReturn || false, 'Original Transaction:', originalTransactionId);

            // Fetch customer name if ID is provided (Ensures data consistency)
            let customerName = null;
            if (parsedCustomerId) {
                const cRes = await client.query('SELECT name FROM customers WHERE id = $1', [parsedCustomerId]);
                if (cRes.rows.length > 0) customerName = cRes.rows[0].name;
            }

            // 1. Verify products exist (but allow negative stock)
            for (const item of items) {
                const res = await client.query('SELECT name FROM products WHERE barcode = $1 AND tenant_id = $2', [item.barcode, req.user.tenant_id]);
                if (res.rows.length === 0) throw new Error(`Product ${item.barcode} not found`);
            }

            // Create transaction
            const receiptNumber = (isReturn ? 'REF' : 'RCP') + Date.now();
            const txnRes = await client.query(
                `INSERT INTO transactions
                (user_id, store_location, total_amount, original_total, current_total, payment_method, receipt_number, items, created_at, customer_id, customer_name, status, tax_breakdown, is_return, original_transaction_id, return_items)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9, $10, $11, $12, $13, $14, $15) RETURNING id`,
                [req.user.id, req.user.store_location, total, isReturn ? 0 : total, total, paymentMethod, receiptNumber, JSON.stringify(items), parsedCustomerId, customerName, status || 'pending', JSON.stringify(taxBreakdown || []), isReturn || false, originalTransactionId || null, JSON.stringify(returnItems || [])]
            );

            // Handle Stock Management (Deduction for sales, Restoration for returns)
            if (isReturn) {
                if (originalTransactionId) {
                    // Update original transaction totals
                    const origTxn = await client.query('SELECT total_amount, receipt_number FROM transactions WHERE id = $1', [originalTransactionId]);
                    const originalTotal = parseFloat(origTxn.rows[0].total_amount);
                    const newTotal = originalTotal - refundAmount;

                    await client.query(
                        'UPDATE transactions SET has_returns = TRUE, original_total = $1, current_total = $2 WHERE id = $3',
                        [originalTotal, newTotal, originalTransactionId]
                    );

                    // Record entry in refunds table
                    await client.query(
                        `INSERT INTO refunds (transaction_id, original_receipt_number, refund_receipt_number, refund_amount, payment_method, processed_by)
                         VALUES ($1, $2, $3, $4, $5, $6)`,
                        [originalTransactionId, origTxn.rows[0].receipt_number, receiptNumber, refundAmount, paymentMethod, req.user.id]
                    );
                }

                // For returns, we need to ADD back stock (reverse the original sale)
                for (const item of items) {
                    // Resolve canonical branch name from user's store_location
                    let storeLoc = req.user.store_location || 'Main Warehouse';
                    const branchRes = await client.query('SELECT name FROM branches WHERE name = $1 OR location = $1', [storeLoc]);
                    if (branchRes.rows.length > 0) {
                        storeLoc = branchRes.rows[0].name;
                    }

                    // Get product ID - use item.id if available, otherwise lookup by barcode
                    let productId = item.id;
                    if (!productId && item.barcode) {
                        const prodRes = await client.query('SELECT id FROM products WHERE barcode = $1 AND tenant_id = $2 LIMIT 1', [item.barcode, req.user.tenant_id]);
                        if (prodRes.rows.length > 0) {
                            productId = prodRes.rows[0].id;
                            console.log(`[RETURN] Found product ID ${productId} by barcode ${item.barcode}`);
                        }
                    }

                    if (!productId) {
                        console.error(`[RETURN] Cannot restore stock: No product ID or barcode for item ${item.name}`);
                        continue; // Skip this item
                    }

                    const updateRes = await client.query(`
                        UPDATE products 
                        SET stock = COALESCE(stock, 0) + $1,
                            stock_levels = jsonb_set(
                                COALESCE(stock_levels, '{}'::jsonb), 
                                ARRAY[$3::text], 
                                to_jsonb(COALESCE((stock_levels->>$3::text)::int, 0) + $1)
                            )
                        WHERE id = $2
                        RETURNING id, name, stock
                    `, [item.qty, productId, storeLoc]);

                    if (updateRes.rows.length > 0) {
                        console.log(`[RETURN] Stock restored for ${updateRes.rows[0].name}: +${item.qty} units, new stock: ${updateRes.rows[0].stock}`);
                    } else {
                        console.error(`[RETURN] Failed to update stock for product ID ${productId}`);
                    }

                    const batches = await client.query(
                        'SELECT * FROM product_batches WHERE product_barcode = $1 ORDER BY expiry_date ASC FOR UPDATE',
                        [item.barcode]
                    );

                    let remainingQty = item.qty;
                    for (const batch of batches.rows) {
                        if (remainingQty <= 0) break;

                        const addQty = Math.min(remainingQty, 999999); // Add to first available batch
                        await client.query(
                            'UPDATE product_batches SET quantity = quantity + $1 WHERE id = $2',
                            [addQty, batch.id]
                        );
                        remainingQty -= addQty;
                    }
                }

                console.log('✅ Return transaction processed, stock restored');
            } else {
                // Normal sale - deduct stock
                // Resolve canonical branch name for POS user
                let storeLoc = req.user.store_location || 'Main Warehouse';
                const branchRes = await client.query('SELECT id, name FROM branches WHERE name = $1 OR location = $1', [storeLoc]);
                let branchId = req.user.store_id || 1;

                if (branchRes.rows.length > 0) {
                    storeLoc = branchRes.rows[0].name;
                    branchId = branchRes.rows[0].id;
                }

                // Update stock and batches
                for (const item of items) {
                    // Deduct from total stock and specific location using product ID (not barcode)
                    // This prevents affecting other products sharing the same barcode

                    await client.query(`
                        UPDATE products 
                        SET stock = COALESCE(stock, 0) - $1,
                            stock_levels = jsonb_set(
                                COALESCE(stock_levels, '{}'::jsonb), 
                                ARRAY[$3::text], 
                                to_jsonb(COALESCE((stock_levels->>$3::text)::int, 0) - $1)
                            )
                        WHERE id = $2
                    `, [item.qty, item.id, storeLoc]);

                    // FIFO/FEFO Batch Deduction
                    // Get batches ordered by expiry (FEFO)
                    const batches = await client.query(
                        'SELECT * FROM product_batches WHERE product_barcode = $1 AND quantity > 0 ORDER BY expiry_date ASC FOR UPDATE',
                        [item.barcode]
                    );

                    let remainingQty = item.qty;
                    for (const batch of batches.rows) {
                        if (remainingQty <= 0) break;
                        const deduct = Math.min(remainingQty, batch.quantity);
                        await client.query(
                            'UPDATE product_batches SET quantity = quantity - $1 WHERE id = $2',
                            [deduct, batch.id]
                        );
                        remainingQty -= deduct;
                    }

                    // Log audit for each item sold (using product id for accurate lookup)
                    await client.query(`
                        INSERT INTO inventory_audit_log (action_type, product_barcode, quantity_before, quantity_after, reference_id, reference_type, user_id, branch_id)
                        SELECT 'Sale', $1, stock + $2, stock, $3, 'Transaction', $4, $5 FROM products WHERE id = $6
                    `, [item.barcode, item.qty, txnRes.rows[0].id, req.user.id, branchId, item.id]);
                }
            }

            // Update Promotion Usage (for both sales and returns)
            if (promoCode && discount > 0) {
                const discountChange = isReturn ? -discount : discount;

                // Update global total
                await client.query(
                    'UPDATE promotions SET total_discounted = COALESCE(total_discounted, 0) + $1 WHERE code = $2',
                    [discountChange, promoCode]
                );

                // Update branch specific usage
                const branchId = req.user.store_id || 1;
                await client.query(`
                    INSERT INTO promotion_usage (promotion_code, branch_id, total_discounted)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (promotion_code, branch_id)
                    DO UPDATE SET total_discounted = promotion_usage.total_discounted + $3
                `, [promoCode, branchId, discountChange]);
            }

            await client.query('COMMIT');

            // Log appropriate activity
            if (isReturn) {
                await logActivity(req, 'RETURN_SALE', { total: -total, receiptNumber, itemCount: items.length, originalTransactionId });
            } else {
                await logActivity(req, 'POS_SALE', { total, receiptNumber, itemCount: items.length });
            }

            res.json({ success: true, receiptNumber, transactionId: txnRes.rows[0].id });
        } catch (e) {
            await client.query('ROLLBACK');
            throw e;
        } finally {
            client.release();
        }
    } catch (err) {
        console.error('Transaction error:', err);
        res.status(500).json({ message: 'Error processing transaction' });
    }
});

// Get batches for a specific product (for dropdowns)
app.get('/api/batches/product/:barcode', authenticateToken, async (req, res) => {
    const { barcode } = req.params;
    try {
        const result = await pool.query(`
            SELECT * FROM product_batches 
            WHERE product_barcode = $1 AND quantity_available > 0 
            ORDER BY expiry_date ASC
        `, [barcode]);
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error fetching product batches' });
    }
});

// 8. Get expiring batches (FEFO alert)
app.get('/api/batches/expiring/:days', authenticateToken, async (req, res) => {
    const { days } = req.params;
    const userBranchId = req.user.store_id;
    const isRestricted = req.user.role !== 'admin' && req.user.role !== 'ceo';

    try {
        let query = `
            SELECT pb.*, p.name, p.selling_unit,
                EXTRACT(DAY FROM pb.expiry_date - CURRENT_DATE) as days_to_expiry
            FROM product_batches pb
            JOIN products p ON pb.product_barcode = p.barcode
            WHERE pb.status = 'Active'
            AND pb.expiry_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '1 day' * $1
        `;
        let params = [days];

        if (isRestricted && userBranchId) {
            query += ` AND pb.branch_id = $2`;
            params.push(userBranchId);
        }

        query += ` ORDER BY pb.expiry_date ASC`;

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ message: 'Error fetching expiring batches' });
    }
});

// 9. Get next available batch (FIFO/FEFO)
app.get('/api/batches/next/:product_barcode', authenticateToken, async (req, res) => {
    const { product_barcode } = req.params;
    try {
        const result = await pool.query(`
            SELECT * FROM product_batches
            WHERE product_barcode = $1 AND status = 'Active' AND quantity_available > 0
            ORDER BY expiry_date ASC, created_at ASC
            LIMIT 1
        `, [product_barcode]);
        res.json(result.rows[0] || null);
    } catch (err) {
        res.status(500).json({ message: 'Error fetching next batch' });
    }
});

// Get all batches for a user// Get shelf/batch inventory for a specific branch
app.get('/api/shelf/inventory/:branch_id', authenticateToken, async (req, res) => {
    try {
        const { branch_id } = req.params;
        const result = await pool.query(`
            SELECT b.*, p.name, p.selling_unit, p.stock_levels 
            FROM product_batches b 
            JOIN products p ON b.product_barcode = p.barcode 
            WHERE b.branch_id = $1 OR b.branch_id IS NULL OR b.branch_id = 1
            ORDER BY b.created_at DESC
        `, [branch_id]);

        // Get exact branch name for stock map
        const branchRes = await pool.query('SELECT name FROM branches WHERE id = $1', [branch_id]);
        const branchName = branchRes.rows[0]?.name;

        const rows = result.rows.map(row => {
            const levels = typeof row.stock_levels === 'string' ? JSON.parse(row.stock_levels) : (row.stock_levels || {});

            let bStock = 0;
            if (req.user && req.user.type === 'company') {
                bStock = parseInt(
                    levels[branchName] ||
                    levels['NOVELTY'] ||
                    levels['Novelty'] ||
                    levels['Dzorwulu'] ||
                    levels['DZORWULU'] ||
                    levels['Main Warehouse'] ||
                    0
                );
            } else {
                const manualFallback = req.user && req.user.store_location ? req.user.store_location : 'Main Warehouse';
                bStock = parseInt(levels[branchName] || levels[manualFallback] || levels['Main Warehouse'] || 0);
            }

            row.branch_stock = bStock;
            delete row.stock_levels;
            return row;
        });

        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error fetching shelf inventory' });
    }
});

app.get('/api/batches', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT b.*, p.name 
            FROM product_batches b 
            JOIN products p ON b.product_barcode = p.barcode 
            ORDER BY b.created_at DESC
        `);
        res.json(result.rows || []);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error fetching batches' });
    }
});

// Create or update a batch
app.post('/api/batches', authenticateToken, async (req, res) => {
    const { product_barcode, batch_number, expiry_date, quantity, quantity_available } = req.body;

    if (!product_barcode || !batch_number) {
        return res.status(400).json({ message: 'product_barcode and batch_number are required' });
    }

    try {
        // Check if batch already exists
        const existing = await pool.query(`
            SELECT id FROM product_batches 
            WHERE product_barcode = $1 AND batch_number = $2
        `, [product_barcode, batch_number]);

        if (existing.rows.length > 0) {
            // Update existing batch
            const result = await pool.query(`
                UPDATE product_batches 
                SET expiry_date = $1, quantity = $2, quantity_available = $3
                WHERE product_barcode = $4 AND batch_number = $5
                RETURNING *
            `, [expiry_date || null, quantity || 0, quantity_available || quantity || 0, product_barcode, batch_number]);
            res.json(result.rows[0]);
        } else {
            // Create new batch
            const result = await pool.query(`
                INSERT INTO product_batches (product_barcode, batch_number, expiry_date, quantity, quantity_available, status)
                VALUES ($1, $2, $3, $4, $5, 'Active')
                RETURNING *
            `, [product_barcode, batch_number, expiry_date || null, quantity || 0, quantity_available || quantity || 0]);
            res.json(result.rows[0]);
        }
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error saving batch', error: err.message });
    }
});

// 10. Create stock adjustment
app.post('/api/stock-adjustments', authenticateToken, async (req, res) => {
    const { product_barcode, adjustment_type, quantity_adjusted, reason, approver_id, branch_id, user_id } = req.body;
    const branchName = branch_id == 2 ? 'Accra Branch' : (branch_id == 3 ? 'Kumasi Branch' : 'Main Warehouse');

    try {
        const result = await pool.query(`
            INSERT INTO stock_adjustments (product_barcode, adjustment_type, quantity_adjusted, reason, approver_id, branch_id, approved_at)
            VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
            RETURNING id
        `, [product_barcode, adjustment_type, quantity_adjusted, reason, approver_id, branch_id]);

        await pool.query(`
            UPDATE products 
            SET stock = COALESCE(stock, 0) + $1,
                stock_levels = jsonb_set(
                    COALESCE(stock_levels, '{}'::jsonb), 
                    ARRAY[$3::text], 
                    to_jsonb(COALESCE((stock_levels->>$3::text)::int, 0) + $1)
                )
            WHERE barcode = $2
        `, [quantity_adjusted, product_barcode, branchName]);

        await pool.query(`
            INSERT INTO inventory_audit_log (action_type, product_barcode, quantity_after, reference_id, reference_type, user_id, branch_id, notes)
            SELECT 'Stock Adjustment', $1::varchar, stock - $6, stock, $2, 'Adjustment', $3, $4, $5 FROM products WHERE barcode = $1
        `, [product_barcode, result.rows[0].id, user_id, branch_id, reason, quantity_adjusted]);

        await logActivity(req, 'STOCK_ADJUSTMENT', { product_barcode, quantity_adjusted, reason });
        res.json({ success: true, id: result.rows[0].id });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error creating adjustment' });
    }
});

// Get Stock Adjustments History
app.get('/api/stock-adjustments', authenticateToken, async (req, res) => {
    try {
        let query = `
            SELECT sa.*, p.name as product_name, u.name as approver_name
            FROM stock_adjustments sa
            JOIN products p ON sa.product_barcode = p.barcode
            LEFT JOIN users u ON sa.approver_id = u.id
        `;
        let params = [];
        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ` WHERE sa.branch_id = $1`;
            params.push(req.user.store_id);
        }
        query += ` ORDER BY sa.created_at DESC`;
        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

// 11. Start stock take
app.post('/api/stock-takes', authenticateToken, async (req, res) => {
    const { branch_id, created_by } = req.body;
    try {
        const result = await pool.query(`
            INSERT INTO stock_takes (stock_take_date, branch_id, created_by, status)
            VALUES (CURRENT_DATE, $1, $2, 'In Progress')
            RETURNING id
        `, [branch_id, created_by]);
        await logActivity(req, 'START_STOCK_TAKE', { branch_id });
        res.json({ success: true, id: result.rows[0].id });
    } catch (err) {
        res.status(500).json({ message: 'Error starting stock take' });
    }
});

// Get Stock Takes History
app.get('/api/stock-takes', authenticateToken, async (req, res) => {
    try {
        let query = `
            SELECT st.*, u.name as created_by_name 
            FROM stock_takes st
            LEFT JOIN users u ON st.created_by = u.id
        `;
        let params = [];
        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ` WHERE st.branch_id = $1`;
            params.push(req.user.store_id);
        }
        query += ` ORDER BY st.stock_take_date DESC`;
        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

// 12. Record stock take item
app.post('/api/stock-takes/:id/items', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const { product_barcode, physical_count, counted_by } = req.body;
    try {
        const systemCount = await pool.query('SELECT stock FROM products WHERE barcode = $1', [product_barcode]);
        const variance = physical_count - systemCount.rows[0].stock;

        await pool.query(`
            INSERT INTO stock_take_items (stock_take_id, product_barcode, physical_count, system_count, variance, counted_by, counted_at)
            VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
        `, [id, product_barcode, physical_count, systemCount.rows[0].stock, variance, counted_by]);

        res.json({ success: true, variance });
    } catch (err) {
        res.status(500).json({ message: 'Error recording stock count' });
    }
});

// 13. Approve stock take
app.post('/api/stock-takes/:id/approve', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const { approved_by } = req.body;
    try {
        const branchRes = await pool.query('SELECT branch_id FROM stock_takes WHERE id = $1', [id]);
        const bId = branchRes.rows[0]?.branch_id || 1;
        const branchName = bId == 2 ? 'Accra Branch' : (bId == 3 ? 'Kumasi Branch' : 'Main Warehouse');

        const items = await pool.query('SELECT * FROM stock_take_items WHERE stock_take_id = $1', [id]);
        for (const item of items.rows) {
            if (item.variance !== 0) {
                await pool.query(`
                    UPDATE products 
                    SET stock = COALESCE(stock, 0) + $1,
                        stock_levels = jsonb_set(
                            COALESCE(stock_levels, '{}'::jsonb), 
                            ARRAY[$3::text], 
                            to_jsonb(COALESCE((stock_levels->>$3::text)::int, 0) + $1)
                        )
                    WHERE barcode = $2
                `, [item.variance, item.product_barcode, branchName]);

                await pool.query(`
                    INSERT INTO inventory_audit_log (action_type, product_barcode, quantity_before, quantity_after, reference_id, reference_type, user_id)
                    VALUES ('Stock Take Variance', $1, $2, $3, $4, 'Stock Take', $5)
                `, [item.product_barcode, item.system_count, item.physical_count, id, approved_by]);
            }
        }
        const varianceResult = await pool.query('SELECT COALESCE(SUM(variance), 0) as total_variance FROM stock_take_items WHERE stock_take_id = $1', [id]);
        await pool.query(`
            UPDATE stock_takes SET status = 'Approved', approved_by = $1, completed_at = CURRENT_TIMESTAMP, variance_total = $2 WHERE id = $3
        `, [approved_by, varianceResult.rows[0].total_variance, id]);
        await logActivity(req, 'APPROVE_STOCK_TAKE', { id, total_variance: varianceResult.rows[0].total_variance });
        res.json({ success: true, message: 'Stock take approved' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error approving stock take' });
    }
});

// 14. Check reorder alerts
app.post('/api/reorder/check', authenticateToken, async (req, res) => {
    const { branch_id } = req.body;
    try {
        const result = await pool.query(`
            SELECT p.barcode, p.name, p.stock, p.reorder_level,
                CASE 
                    WHEN p.stock = 0 THEN 'Critical'
                    WHEN p.stock <= p.reorder_level THEN 'High'
                    ELSE 'Normal'
                END as priority,
                (p.reorder_level * 2) as suggested_quantity
            FROM products p
            WHERE p.stock <= p.reorder_level
            ORDER BY p.stock ASC
        `);

        for (const product of result.rows) {
            await pool.query(`
                INSERT INTO reorder_alerts (product_barcode, current_stock, reorder_level, suggested_quantity, priority, branch_id, status)
                VALUES ($1, $2, $3, $4, $5, $6, 'Active')
                ON CONFLICT (product_barcode) DO UPDATE SET
                    current_stock = $2, priority = $5, created_at = CURRENT_TIMESTAMP
            `, [product.barcode, product.stock, product.reorder_level, product.suggested_quantity, product.priority, branch_id]);
        }
        await logActivity(req, 'CHECK_REORDER_ALERTS', { branch_id, alertsFound: result.rows.length });
        res.json({ success: true, alerts: result.rows });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error checking reorder alerts' });
    }
});

// 15. Get reorder alerts
app.get('/api/reorder/alerts/:branch_id', authenticateToken, async (req, res) => {
    const { branch_id } = req.params;
    try {
        const result = await pool.query(`
            SELECT ra.*, p.name, p.selling_unit, p.price
            FROM reorder_alerts ra
            JOIN products p ON ra.product_barcode = p.barcode
            WHERE ra.branch_id = $1 AND ra.status = 'Active'
            ORDER BY 
                CASE WHEN ra.priority = 'Critical' THEN 1 WHEN ra.priority = 'High' THEN 2 ELSE 3 END,
                ra.current_stock ASC
        `, [branch_id]);
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ message: 'Error fetching alerts' });
    }
});

// 16. Confirm transfer receipt
app.post('/api/transfers/:id/confirm', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const { confirmed_by, items_received } = req.body;
    try {
        for (const item of items_received) {
            await pool.query(`
                UPDATE stock_transfer_items SET quantity_received = $1 WHERE id = $2
            `, [item.quantity_received, item.id]);
        }
        await pool.query(`
            UPDATE stock_transfers SET status = 'Confirmed', confirmed_by = $1, confirmed_at = CURRENT_TIMESTAMP
            WHERE id = $2
        `, [confirmed_by, id]);
        await logActivity(req, 'CONFIRM_TRANSFER', { id });
        res.json({ success: true, message: 'Transfer confirmed' });
    } catch (err) {
        res.status(500).json({ message: 'Error confirming transfer' });
    }
});

// 17. Low stock report
app.get('/api/reports/low-stock/:branch_id', authenticateToken, async (req, res) => {
    const { branch_id } = req.params;
    try {
        const result = await pool.query(`
            SELECT p.barcode, p.name, p.category, p.stock, p.reorder_level,
                (p.reorder_level - p.stock) as shortage,
                ROUND((p.stock::decimal / NULLIF(p.reorder_level, 0)) * 100, 2) as stock_percentage
            FROM products p
            WHERE p.stock < p.reorder_level
            ORDER BY shortage DESC
        `);
        await logActivity(req, 'VIEW_REPORT_LOW_STOCK', { branch_id });
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ message: 'Error generating report' });
    }
});

// 18. Expiry report
app.get('/api/reports/expiry/:branch_id', authenticateToken, async (req, res) => {
    const { branch_id } = req.params;
    try {
        const result = await pool.query(`
            SELECT pb.*, p.name, p.selling_unit,
                EXTRACT(DAY FROM pb.expiry_date - CURRENT_DATE) as days_to_expiry,
                CASE 
                    WHEN pb.expiry_date < CURRENT_DATE THEN 'Expired'
                    WHEN pb.expiry_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days' THEN 'Expiring Soon'
                    WHEN pb.expiry_date BETWEEN CURRENT_DATE + INTERVAL '7 days' AND CURRENT_DATE + INTERVAL '30 days' THEN 'Near Expiry'
                    ELSE 'OK'
                END as status
            FROM product_batches pb
            JOIN products p ON pb.product_barcode = p.barcode
            WHERE pb.branch_id = $1 AND pb.status = 'Active'
            ORDER BY pb.expiry_date ASC
        `, [branch_id]);
        await logActivity(req, 'VIEW_REPORT_EXPIRY', { branch_id });
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ message: 'Error generating expiry report' });
    }
});

// 20. Stock summary
app.get('/api/reports/stock-summary/:branch_id', authenticateToken, async (req, res) => {
    const { branch_id } = req.params;
    try {
        const result = await pool.query(`
            SELECT 
                COUNT(*) as total_products,
                SUM(stock) as total_units,
                SUM(stock * cost_price) as total_cost_value,
                SUM(stock * price) as total_retail_value,
                COUNT(CASE WHEN stock = 0 THEN 1 END) as out_of_stock_count,
                COUNT(CASE WHEN stock <= reorder_level THEN 1 END) as low_stock_count
            FROM products p
            WHERE p.tenant_id = $1
            ORDER BY p.name ASC
        `, [req.user.tenant_id]);
        await logActivity(req, 'VIEW_REPORT_SUMMARY', { branch_id });
        res.json(result.rows[0]);
    } catch (err) {
        res.status(500).json({ message: 'Error generating summary' });
    }
});

// 21. Get product for POS with FEFO batch selection
app.get('/api/pos/product/:barcode', authenticateToken, async (req, res) => {
    const { barcode } = req.params;
    try {
        const product = await pool.query('SELECT * FROM products WHERE barcode = $1', [barcode]);
        if (product.rows.length === 0) {
            return res.status(404).json({ message: 'Product not found' });
        }
        const prod = product.rows[0];
        if (prod.stock <= 0) {
            return res.status(400).json({ message: 'Out of stock' });
        }
        let batches = [];
        if (prod.track_batch) {
            const batchResult = await pool.query(`
                SELECT * FROM product_batches
                WHERE product_barcode = $1 AND status = 'Active' AND quantity_available > 0
                ORDER BY expiry_date ASC, created_at ASC
            `, [barcode]);
            batches = batchResult.rows;
        }
        res.json({ ...prod, batches });
    } catch (err) {
        res.status(500).json({ message: 'Error fetching product' });
    }
});

// 22. Process POS sale with batch tracking
app.post('/api/pos/sale/:barcode', authenticateToken, async (req, res) => {
    const { barcode } = req.params;
    const { quantity, batch_id, user_id, branch_id } = req.body;
    const branchName = branch_id == 2 ? 'Accra Branch' : (branch_id == 3 ? 'Kumasi Branch' : 'Main Warehouse');

    try {
        const product = await pool.query('SELECT * FROM products WHERE barcode = $1', [barcode]);
        if (product.rows.length === 0 || product.rows[0].stock < quantity) {
            return res.status(400).json({ message: 'Insufficient stock' });
        }
        if (batch_id) {
            await pool.query(
                'UPDATE product_batches SET quantity_sold = quantity_sold + $1, quantity_available = quantity_available - $1 WHERE id = $2',
                [quantity, batch_id]
            );
        }

        await pool.query(`
            UPDATE products 
            SET stock = COALESCE(stock, 0) - $1,
                stock_levels = jsonb_set(
                    COALESCE(stock_levels, '{}'::jsonb), 
                    ARRAY[$3::text], 
                    to_jsonb(COALESCE((stock_levels->>$3::text)::int, 0) - $1)
                )
            WHERE barcode = $2
        `, [quantity, barcode, branchName]);

        await pool.query(`
            INSERT INTO inventory_audit_log (action_type, product_barcode, quantity_before, quantity_after, reference_id, reference_type, user_id, branch_id)
            SELECT 'Sale', $1::varchar, $2, stock, NULL, 'POS Sale', $4, $5 FROM products WHERE barcode = $1
        `, [barcode, product.rows[0].stock, quantity, user_id, branch_id]); // $2 is old stock (quantity_before)
        await logActivity(req, 'POS_SALE_ITEM', { barcode, quantity, batch_id });
        res.json({ success: true, remaining_stock: product.rows[0].stock - quantity });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error processing sale' });
    }
});

// ============ SYSTEM SETTINGS ENDPOINTS ============

app.get('/api/settings', authenticateToken, async (req, res) => {
    try {
        // Company portal users always manage the master settings (branch_id=1)
        let branchId = 1;
        if (req.user.type !== 'company') {
            const userRes = await pool.query('SELECT store_id FROM users WHERE id = $1', [req.user.id]);
            const dbStoreId = userRes.rows[0]?.store_id;
            branchId = dbStoreId || req.user.store_id || 1;
        }

        let result = await pool.query('SELECT * FROM system_settings WHERE branch_id = $1', [branchId]);
        if (result.rows.length === 0) {
            result = await pool.query('SELECT * FROM system_settings WHERE branch_id = 1');
        }

        res.json(result.rows[0] || {});
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});


app.post('/api/settings', authenticateToken, async (req, res) => {
    // Allow admin, ceo, or company users
    if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.type !== 'company') {
        return res.status(403).json({ message: 'Unauthorized. Only authorized users can change settings.' });
    }

    const {
        storeName, currencySymbol, vatRate, receiptFooter, taxId, phone, monthlyTarget,
        bankName, bankAccountName, bankAccountNumber, bankBranch, momoNumber, momoName
    } = req.body;
    // Company portal users always manage the master settings (branch_id=1)
    const branchId = req.user.type === 'company' ? 1 : (req.user.store_id || 1);

    try {
        await pool.query(`
            INSERT INTO system_settings (id, branch_id, store_name, currency_symbol, vat_rate, receipt_footer, tax_id, phone, monthly_target, bank_name, bank_account_name, bank_account_number, bank_branch, momo_number, momo_name, updated_at)
            VALUES ($1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO UPDATE SET 
                store_name = COALESCE($2, system_settings.store_name), 
                currency_symbol = COALESCE($3, system_settings.currency_symbol), 
                vat_rate = COALESCE($4, system_settings.vat_rate), 
                receipt_footer = COALESCE($5, system_settings.receipt_footer),
                tax_id = COALESCE($6, system_settings.tax_id),
                phone = COALESCE($7, system_settings.phone),
                monthly_target = COALESCE($8, system_settings.monthly_target),
                bank_name = COALESCE($9, system_settings.bank_name),
                bank_account_name = COALESCE($10, system_settings.bank_account_name),
                bank_account_number = COALESCE($11, system_settings.bank_account_number),
                bank_branch = COALESCE($12, system_settings.bank_branch),
                momo_number = COALESCE($13, system_settings.momo_number),
                momo_name = COALESCE($14, system_settings.momo_name),
                updated_at = CURRENT_TIMESTAMP
        `, [branchId, storeName || null, currencySymbol || null, vatRate || null, receiptFooter || null, taxId || null, phone || null, monthlyTarget || null, bankName || null, bankAccountName || null, bankAccountNumber || null, bankBranch || null, momoNumber || null, momoName || null]);
        await logActivity(req, 'UPDATE_SETTINGS', { storeName, vatRate });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error saving settings' });
    }
});

// Delete System VAT (Set to 0)
app.delete('/api/settings/vat', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    const branchId = req.user.store_id || 1;
    try {
        // Upsert to ensure row exists, setting VAT to 0
        await pool.query(`
            INSERT INTO system_settings (id, branch_id, vat_rate)
            VALUES ($1, $1, 0)
            ON CONFLICT (id) DO UPDATE SET vat_rate = 0
        `, [branchId]);
        await logActivity(req, 'DELETE_SYSTEM_VAT');
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error removing VAT' });
    }
});

// Add Branch Management Endpoints
app.get('/api/branches', authenticateToken, async (req, res) => {
    try {
        // Join with system_settings to get VAT
        const result = await pool.query(`
            SELECT b.*, COALESCE(ss.vat_rate, 15.00) as vat_rate 
            FROM branches b
            LEFT JOIN system_settings ss ON b.id = ss.branch_id
            ORDER BY b.id
        `);
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

app.post('/api/branches', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    const { name, location, vat_rate } = req.body;
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const resBranch = await client.query(
            'INSERT INTO branches (name, location) VALUES ($1, $2) RETURNING id',
            [name, location]
        );
        const branchId = resBranch.rows[0].id;

        // Create settings for this branch
        await client.query(
            `INSERT INTO system_settings (id, branch_id, store_name, currency_symbol, vat_rate, receipt_footer)
             VALUES ($1, $1, $2, '₵ (GHS)', $3, 'Thank you for shopping with us!')`,
            [branchId, name, vat_rate || 15.0]
        );

        await client.query('COMMIT');
        await logActivity(req, 'CREATE_BRANCH', { name, branchId });
        res.json({ success: true, id: branchId });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error(err);
        res.status(500).json({ message: 'Error creating branch' });
    } finally { client.release(); }
});

app.put('/api/branches/:id', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    const { id } = req.params;
    const { name, location, vat_rate } = req.body;
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        await client.query('UPDATE branches SET name = $1, location = $2 WHERE id = $3', [name, location, id]);

        // Update VAT in settings
        await client.query(`
            INSERT INTO system_settings (id, branch_id, store_name, currency_symbol, vat_rate, receipt_footer)
            VALUES ($1, $1, $2, '₵ (GHS)', $3, 'Thank you for shopping with us!')
            ON CONFLICT (id) DO UPDATE SET vat_rate = $3, store_name = $2
        `, [id, name, vat_rate]);

        await client.query('COMMIT');
        await logActivity(req, 'UPDATE_BRANCH', { id, vat_rate });
        res.json({ success: true });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error(err);
        res.status(500).json({ message: 'Error updating branch' });
    } finally { client.release(); }
});

// Delete Branch
app.delete('/api/branches/:id', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') {
        return res.status(403).json({ success: false, message: 'Unauthorized' });
    }
    
    const branchId = parseInt(req.params.id, 10);
    
    if (branchId === 1) {
        return res.status(403).json({ success: false, message: 'Cannot delete the Main Warehouse (ID 1) as it is a core system requirement.' });
    }
    
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        
        // Ensure this branch belongs to the user's tenant
        const checkBranch = await client.query('SELECT name FROM branches WHERE id = $1 AND tenant_id = $2', [branchId, req.user.tenant_id]);
        if (checkBranch.rows.length === 0) {
            await client.query('ROLLBACK');
            return res.status(404).json({ success: false, message: 'Branch not found or belongs to another tenant.' });
        }
        
        // Remove system settings reference first to prevent Foreign Key blocks
        await client.query('DELETE FROM system_settings WHERE branch_id = $1', [branchId]);
        
        // Delete the branch
        await client.query('DELETE FROM branches WHERE id = $1 AND tenant_id = $2', [branchId, req.user.tenant_id]);
        
        await client.query('COMMIT');
        
        await logActivity(req, 'DELETE_BRANCH', { branchId, branchName: checkBranch.rows[0].name });
        res.json({ success: true, message: 'Branch deleted successfully.' });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error('Delete Branch Error:', err);
        
        // If a DB constrained error occurs (e.g. Sales exist pointing to branch ID)
        if (err.code === '23503') {
            return res.status(400).json({ success: false, message: 'Cannot delete branch because historical transaction data or active users are currently tied to it.' });
        }
        res.status(500).json({ success: false, message: 'Server error' });
    } finally {
        client.release();
    }
});

// --- CREDIT AUTHORIZATION CODE ---

app.post('/api/verify-auth-code', authenticateToken, async (req, res) => {
    const { code } = req.body;

    try {
        // Refresh user store_id from DB to ensure accuracy
        const userCheck = await pool.query('SELECT store_id FROM users WHERE id = $1', [req.user.id]);
        const dbStoreId = userCheck.rows[0]?.store_id;
        const branchId = dbStoreId || req.user.store_id || 1;

        console.log(`[AUTH-VERIFY] User: ${req.user.id}, Branch: ${branchId}, Code: '${code}'`);
        const inputCode = String(code).trim();

        // 1. Check User's Branch
        const branchRes = await pool.query('SELECT credit_auth_code, credit_auth_code_expiry FROM system_settings WHERE branch_id = $1', [branchId]);

        // 2. Check Main Branch (Fallback)
        const mainRes = await pool.query('SELECT credit_auth_code, credit_auth_code_expiry FROM system_settings WHERE branch_id = 1');

        const checkCode = (row, source) => {
            if (!row || !row.credit_auth_code) return false;
            const dbCode = String(row.credit_auth_code).trim();

            if (dbCode === inputCode) {
                if (row.credit_auth_code_expiry && new Date() > new Date(row.credit_auth_code_expiry)) {
                    console.log(`[AUTH-VERIFY] ${source}: Code expired`);
                    return 'expired';
                }
                return 'valid';
            }
            return false;
        };

        let status = false;

        // Check local branch
        if (branchRes.rows.length > 0) {
            console.log(`[AUTH-VERIFY] Found settings for Branch ${branchId}. DB has: '${branchRes.rows[0].credit_auth_code}'`);
            status = checkCode(branchRes.rows[0], 'Local Branch');
        } else {
            console.log(`[AUTH-VERIFY] No settings found for Branch ${branchId}.`);
        }

        // If not valid locally, and we are not already at branch 1, check main branch
        if (status !== 'valid' && branchId !== 1 && mainRes.rows.length > 0) {
            console.log(`[AUTH-VERIFY] Checking Main Branch (1) fallback. DB has: '${mainRes.rows[0].credit_auth_code}'`);
            const mainStatus = checkCode(mainRes.rows[0], 'Main Branch');
            if (mainStatus === 'valid') status = 'valid';
            else if (mainStatus === 'expired' && status !== 'expired') status = 'expired';
        }

        // Legacy/Default fallback
        if (status !== 'valid' && inputCode === '123456') {
            status = 'valid';
        }

        if (status === 'valid') {
            res.json({ success: true });
        } else if (status === 'expired') {
            res.status(401).json({ success: false, message: 'Authorization code has expired' });
        } else {
            console.log(`[AUTH-VERIFY] Failed. Input '${inputCode}' did not match DB.`);
            res.status(401).json({ success: false, message: 'Invalid authorization code' });
        }
    } catch (err) {
        console.error('[AUTH-VERIFY] Error:', err);
        res.status(500).json({ message: 'Server error' });
    }
});

app.get('/api/settings/auth-code', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'manager') return res.status(403).json({ message: 'Unauthorized' });
    const branchId = req.user.store_id || 1;
    try {
        const result = await pool.query('SELECT credit_auth_code, credit_auth_code_expiry FROM system_settings WHERE branch_id = $1', [branchId]);
        if (result.rows.length > 0) {
            res.json(result.rows[0]);
        } else {
            res.json({});
        }
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

app.post('/api/settings/auth-code', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'manager') return res.status(403).json({ message: 'Unauthorized' });

    // Refresh store info from DB
    const userCheck = await pool.query('SELECT store_id FROM users WHERE id = $1', [req.user.id]);
    const branchId = userCheck.rows[0]?.store_id || req.user.store_id || 1;

    // Generate 6-digit code and set 24h expiry
    const code = crypto.randomInt(100000, 1000000).toString();
    const expiry = new Date(Date.now() + 24 * 60 * 60 * 1000); // 24 hours from now

    try {
        // Check if row exists
        const check = await pool.query('SELECT id FROM system_settings WHERE branch_id = $1', [branchId]);

        if (check.rows.length > 0) {
            await pool.query('UPDATE system_settings SET credit_auth_code = $1, credit_auth_code_expiry = $2 WHERE branch_id = $3', [code, expiry, branchId]);
        } else {
            // Insert new row if missing (Fix for new branches)
            const maxIdRes = await pool.query('SELECT MAX(id) as max_id FROM system_settings');
            const nextId = (maxIdRes.rows[0].max_id || 0) + 1;

            await pool.query(`
                INSERT INTO system_settings (id, branch_id, credit_auth_code, credit_auth_code_expiry, store_name, currency_symbol, vat_rate)
                VALUES ($1, $2, $3, $4, 'Footprint Retail', '₵ (GHS)', 15.00)
            `, [nextId, branchId, code, expiry]);
        }

        console.log(`[AUTH-GEN] Generated code ${code} for Branch ${branchId}`);
        await logActivity(req, 'GENERATE_AUTH_CODE');
        res.json({ success: true, code, expiry });
    } catch (err) {
        console.error('[AUTH-GEN] Error:', err);
        res.status(500).json({ message: 'Server error' });
    }
});

// ============ CUSTOMER MANAGEMENT ENDPOINTS ============

app.get('/api/customers', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM customers ORDER BY name');
        await logActivity(req, 'VIEW_CUSTOMER_LIST');
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

app.post('/api/customers', authenticateToken, async (req, res) => {
    const { name, phone, email, creditLimit, status } = req.body;
    try {
        // Verify creator exists (handles cases where DB was reset but token persists)
        const userCheck = await pool.query('SELECT id FROM users WHERE id = $1', [req.user.id]);
        if (userCheck.rows.length === 0) return res.status(401).json({ message: 'User session invalid. Please logout and login again.' });

        if (!name) return res.status(400).json({ message: 'Customer name is required' });

        const limit = parseFloat(creditLimit) || 0;

        // Generate 10-digit sequential account number
        // Use MAX(account_number) to avoid duplicates when customers are deleted
        const maxRes = await pool.query("SELECT MAX(CAST(account_number AS BIGINT)) as max_acc FROM customers WHERE account_number ~ '^[0-9]+$'");
        const currentMax = maxRes.rows[0].max_acc ? parseInt(maxRes.rows[0].max_acc) : 1000000000;
        const accountNumber = String(currentMax + 1);

        await pool.query(
            'INSERT INTO customers (name, phone, email, credit_limit, account_number, status, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7)',
            [name, phone, email, limit, accountNumber, status || 'Active', req.user.id]
        );
        await logActivity(req, 'CREATE_CUSTOMER', { name, accountNumber });
        res.json({ success: true, accountNumber });
    } catch (err) {
        console.error('Error creating customer:', err);
        res.status(500).json({ message: 'Error creating customer', error: err.message });
    }
});

app.post('/api/customers/:id/payment', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const { amount } = req.body;
    const userId = req.user.id;

    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // Update Balance and get new balance
        const custRes = await client.query(
            'UPDATE customers SET current_balance = current_balance - $1 WHERE id = $2 RETURNING current_balance',
            [amount, id]
        );
        const newBalance = custRes.rows[0].current_balance;

        // Record Payment for Statement
        await client.query(
            'INSERT INTO customer_payments (customer_id, amount, recorded_by) VALUES ($1, $2, $3)',
            [id, amount, userId]
        );

        // Add to Ledger (Part 2)
        await client.query(
            `INSERT INTO customer_ledger (customer_id, date, description, type, credit, balance) 
             VALUES ($1, NOW(), 'Payment Received', 'PAYMENT', $2, $3)`,
            [id, amount, newBalance]
        );

        await client.query('COMMIT');
        await logActivity(req, 'CUSTOMER_DEBT_PAYMENT', { customerId: id, amount });
        res.json({ success: true });
    } catch (err) {
        await client.query('ROLLBACK');
        res.status(500).json({ message: 'Error processing payment' });
    } finally {
        client.release();
    }
});

app.get('/api/customers/:id', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
        if (result.rows.length === 0) return res.status(404).json({ message: 'Customer not found' });
        res.json(result.rows[0]);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

app.put('/api/customers/:id', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const { name, phone, email, creditLimit } = req.body;

    try {
        const currentRes = await pool.query('SELECT * FROM customers WHERE id = $1', [id]);
        if (currentRes.rows.length === 0) return res.status(404).json({ message: 'Customer not found' });

        const current = currentRes.rows[0];
        const newName = name || current.name;
        const newPhone = phone || current.phone;
        const newEmail = email || current.email;

        let message = 'Customer updated successfully';
        let newLimit = current.credit_limit;

        // Handle Credit Limit Change
        if (creditLimit !== undefined && parseFloat(creditLimit) !== parseFloat(current.credit_limit)) {
            if (req.user.role === 'ceo' || req.user.role === 'admin') {
                // Direct update for CEO/Admin
                newLimit = parseFloat(creditLimit);
                await pool.query('UPDATE customers SET name = $1, phone = $2, email = $3, credit_limit = $4, pending_credit_limit = NULL WHERE id = $5', [newName, newPhone, newEmail, newLimit, id]);
            } else {
                // Request approval for others
                await pool.query('UPDATE customers SET name = $1, phone = $2, email = $3, pending_credit_limit = $4 WHERE id = $5', [newName, newPhone, newEmail, parseFloat(creditLimit), id]);
                message = 'Credit limit increase requested. Sent to CEO for approval.';
            }
        } else {
            await pool.query('UPDATE customers SET name = $1, phone = $2, email = $3 WHERE id = $4', [newName, newPhone, newEmail, id]);
        }

        await logActivity(req, 'UPDATE_CUSTOMER', { id, creditLimit: newLimit });
        res.json({ success: true, message });
    } catch (err) { res.status(500).json({ message: 'Error updating customer' }); }
});

app.delete('/api/customers/:id', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        // Delete related records first (in correct order to avoid foreign key issues)
        await client.query('DELETE FROM customer_payments WHERE customer_id = $1', [id]);
        await client.query('DELETE FROM customer_ledger WHERE customer_id = $1', [id]);
        await client.query('DELETE FROM transactions WHERE customer_id = $1', [id]);

        // Finally delete the customer
        const result = await client.query('DELETE FROM customers WHERE id = $1 RETURNING id', [id]);

        if (result.rows.length === 0) {
            await client.query('ROLLBACK');
            return res.status(404).json({ message: 'Customer not found' });
        }

        await client.query('COMMIT');
        await logActivity(req, 'DELETE_CUSTOMER', { id });
        res.json({ success: true, message: 'Customer and all related records deleted successfully' });
    } catch (err) {
        await client.query('ROLLBACK');
        console.error('Error deleting customer:', err);
        res.status(500).json({ message: 'Error deleting customer: ' + err.message });
    } finally {
        client.release();
    }
});

// --- CUSTOMER STATEMENTS & REPORTING ---

async function getCustomerStatementData(customerId, month, year) {
    const now = new Date();
    const targetYear = year || now.getFullYear();
    const targetMonth = month || (now.getMonth() + 1); // 1-12

    const startDate = `${targetYear}-${targetMonth}-01`;
    // Calculate end date (last day of the month)
    const endDate = new Date(targetYear, targetMonth, 0).toISOString().split('T')[0];

    // Get Customer
    const custRes = await pool.query('SELECT * FROM customers WHERE id = $1', [customerId]);
    if (custRes.rows.length === 0) throw new Error('Customer not found');
    const customer = custRes.rows[0];

    // Get Purchases (Credit Transactions)
    const purchasesRes = await pool.query(`
        SELECT id, receipt_number, total_amount, created_at, items 
        FROM transactions 
        WHERE customer_id = $1 AND payment_method = 'credit' 
        AND created_at >= $2::date AND created_at <= ($3::date + INTERVAL '1 day')
        ORDER BY created_at ASC
    `, [customerId, startDate, endDate]);

    // Get Payments
    const paymentsRes = await pool.query(`
        SELECT id, amount, payment_date 
        FROM customer_payments 
        WHERE customer_id = $1 
        AND payment_date >= $2::date AND payment_date <= ($3::date + INTERVAL '1 day')
        ORDER BY payment_date ASC
    `, [customerId, startDate, endDate]);

    return { customer, purchases: purchasesRes.rows, payments: paymentsRes.rows, period: { month: targetMonth, year: targetYear } };
}

app.get('/api/customers/:id/statement', authenticateToken, async (req, res) => {
    try {
        const data = await getCustomerStatementData(req.params.id, req.query.month, req.query.year);
        res.json(data);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: err.message || 'Error generating statement' });
    }
});

// New endpoint for detailed statement data (with transaction-level debit/credit/balance)
app.get('/api/customers/:id/statement-data', authenticateToken, async (req, res) => {
    try {
        const customerId = req.params.id;
        const month = req.query.month || new Date().getMonth() + 1;
        const year = req.query.year || new Date().getFullYear();

        const startDate = `${year}-${String(month).padStart(2, '0')}-01`;
        const endDate = new Date(year, month, 0).toISOString().split('T')[0];

        // Get Customer
        const custRes = await pool.query('SELECT * FROM customers WHERE id = $1', [customerId]);
        if (custRes.rows.length === 0) {
            return res.status(404).json({ message: 'Customer not found' });
        }
        const customer = custRes.rows[0];

        // Get opening balance - all credit purchases minus all payments before this month
        const prevMonthEnd = new Date(year, month - 1, 0).toISOString().split('T')[0];
        const prevBalanceRes = await pool.query(`
            SELECT COALESCE(SUM(debit - credit), 0) as balance 
            FROM customer_ledger 
            WHERE customer_id = $1 AND date <= $2::date
        `, [customerId, prevMonthEnd]);

        const openingBalance = parseFloat(prevBalanceRes.rows[0].balance || 0);

        // Get all transactions for the month from Ledger
        const transactionsRes = await pool.query(`
            SELECT * FROM customer_ledger 
            WHERE customer_id = $1 
            AND date >= $2::date AND date < ($3::date + INTERVAL '1 day')
            ORDER BY date ASC
        `, [customerId, startDate, endDate]);

        // Calculate running balance
        let runningBalance = openingBalance;
        const transactions = transactionsRes.rows.map(t => {
            const debit = parseFloat(t.debit) || 0;
            const credit = parseFloat(t.credit) || 0;
            runningBalance += debit - credit;
            return {
                date: t.date,
                description: t.description,
                debit: debit,
                credit: credit,
                balance: runningBalance
            };
        });

        const closingBalance = runningBalance;

        res.json({
            customer: {
                id: customer.id,
                name: customer.name,
                phone: customer.phone,
                email: customer.email
            },
            openingBalance,
            transactions,
            closingBalance,
            period: { month: parseInt(month), year: parseInt(year) }
        });
    } catch (err) {
        console.error('Error fetching statement data:', err);
        res.status(500).json({ message: err.message || 'Error fetching statement data' });
    }
});

app.post('/api/customers/:id/email-statement', authenticateToken, async (req, res) => {
    try {
        console.log('📧 Starting email-statement request for customer ID:', req.params.id);
        const customerId = req.params.id;
        let { month, year, pdfData: clientPdf } = req.body;

        // Validate and default month/year
        if (!month || !year) {
            const now = new Date();
            month = month || now.getMonth() + 1;
            year = year || now.getFullYear();
        }

        // Validate month and year ranges
        month = parseInt(month);
        year = parseInt(year);

        if (isNaN(month) || month < 1 || month > 12) {
            return res.status(400).json({ message: 'Invalid month. Please provide month between 1 and 12' });
        }

        if (isNaN(year) || year < 2020 || year > new Date().getFullYear() + 1) {
            return res.status(400).json({ message: 'Invalid year' });
        }

        console.log('📅 Statement Request - Customer ID:', customerId, '| Month:', month, '| Year:', year);

        // Calculate month name (needed for PDF and email)
        const monthName = new Date(year, month - 1).toLocaleString('default', { month: 'long' });

        // Get detailed statement data for PDF
        const startDate = `${year}-${String(month).padStart(2, '0')}-01`;
        const endDate = new Date(year, month, 0).toISOString().split('T')[0];

        // Get Customer
        const custRes = await pool.query('SELECT * FROM customers WHERE id = $1', [customerId]);
        if (custRes.rows.length === 0) {
            return res.status(404).json({ message: 'Customer not found' });
        }
        const customer = custRes.rows[0];
        const customerName = customer.name;

        if (!customer.email) return res.status(400).json({ message: 'Customer has no email address' });

        // Use current balance for "Amount Used" display (all-time debt)
        const amountUsed = parseFloat(customer.current_balance || 0);
        console.log(`💰 Statement Generation - Amount Used: ${amountUsed}`);

        let pdfData;

        // Force server-side generation to ensure accuracy (Ignore frontend PDF)
        if (false && clientPdf) {
            console.log('📄 Using client-provided PDF data');
            pdfData = Buffer.from(clientPdf, 'base64');
        } else {
            console.log('🔄 GENERATING NEW PDF STATEMENT (v3.0 - Ledger Rebuild)');

            // 1. REBUILD LEDGER FOR THIS CUSTOMER (Ensures "Table" is full and accurate)
            // This fixes missing transactions by pulling everything fresh from source tables
            await pool.query('DELETE FROM customer_ledger WHERE customer_id = $1', [customerId]);

            // Insert Sales (Credit Transactions)
            await pool.query(`
                INSERT INTO customer_ledger (customer_id, date, description, type, debit, credit, transaction_id)
                SELECT customer_id, created_at, COALESCE(receipt_number, 'TRX') || ' (Sale)', 'SALE', total_amount, 0, id
                FROM transactions 
                WHERE customer_id = $1 AND status = 'completed'
            `, [customerId]);

            // Insert Payments
            await pool.query(`
                INSERT INTO customer_ledger (customer_id, date, description, type, debit, credit)
                SELECT customer_id, payment_date, 'Payment Received', 'PAYMENT', 0, amount 
                FROM customer_payments 
                WHERE customer_id = $1
            `, [customerId]);

            // 2. CALCULATE OPENING BALANCE
            // Sum of all activity BEFORE the start date
            const openBalRes = await pool.query(`
                SELECT COALESCE(SUM(debit - credit), 0) as balance 
                FROM customer_ledger 
                WHERE customer_id = $1 AND date < $2::date
            `, [customerId, startDate]);
            const openingBalance = parseFloat(openBalRes.rows[0].balance || 0);

            // 3. FETCH STATEMENT TRANSACTIONS
            // Get ledger entries for the month, joined with transactions to get item details
            const ledgerRes = await pool.query(`
                SELECT cl.*, t.items
                FROM customer_ledger cl
                LEFT JOIN transactions t ON cl.transaction_id = t.id
                WHERE cl.customer_id = $1 
                AND cl.date >= $2::date AND cl.date <= $3::date::date + INTERVAL '1 day' - INTERVAL '1 second'
                ORDER BY cl.date ASC, cl.id ASC
            `, [customerId, startDate, endDate]);

            console.log(`📊 Ledger Rebuilt. Opening Balance: ${openingBalance}. Transactions found: ${ledgerRes.rows.length}`);

            // Fetch VAT rate from system settings
            let vatRate = 15.0; // Default VAT rate
            try {
                const vatRes = await pool.query('SELECT vat_rate FROM system_settings WHERE branch_id = 1 LIMIT 1');
                if (vatRes.rows.length > 0) {
                    vatRate = parseFloat(vatRes.rows[0].vat_rate);
                }
            } catch (e) {
                console.warn('Could not fetch VAT rate, using default 15%');
            }

            // Generate PDF with password protection
            const pdfPromise = new Promise((resolve, reject) => {
                try {
                    const pdfBuffer = [];

                    const doc = new PDFDocument({
                        size: 'A4',
                        margin: 50,
                        permissions: {
                            printing: 'highResolution',
                            modifying: false,
                            copying: false
                        }
                    });

                    // Collect PDF output
                    doc.on('data', (chunk) => pdfBuffer.push(chunk));
                    doc.on('end', () => {
                        resolve(Buffer.concat(pdfBuffer));
                    });

                    doc.on('error', (err) => {
                        reject(err);
                    });

                    // ===== BUILD PDF =====

                    // Header
                    doc.fontSize(18).font('Helvetica-Bold').fillColor('#1a2a6c').text('CUSTOMER STATEMENT', { align: 'center' });
                    doc.fontSize(11).font('Helvetica').fillColor('#666').text(`${monthName} ${year}`, { align: 'center' });
                    doc.moveDown(0.5);

                    // Account Details Section
                    doc.fontSize(11).font('Helvetica-Bold').fillColor('#1a2a6c').text('ACCOUNT DETAILS', { underline: true });
                    doc.fontSize(9).font('Helvetica').fillColor('#000');

                    const creditAvailable = parseFloat(customer.credit_limit) - amountUsed;

                    const detailsBox = [
                        ['Customer Name:', customer.name],
                        ['Account Number:', customer.account_number || `#${customer.id}`],
                        ['Phone Number:', customer.phone || 'N/A'],
                        ['Statement Date:', new Date().toLocaleDateString('en-GB', { day: '2-digit', month: '2-digit', year: 'numeric' })]
                    ];

                    detailsBox.forEach(([label, value]) => {
                        doc.text(`${label} ${value}`);
                    });
                    doc.moveDown(0.5);

                    // Summary Box with credit information
                    const summaryY = doc.y;
                    doc.rect(50, summaryY, 495, 85).stroke('#1a2a6c');

                    doc.fontSize(10).font('Helvetica-Bold').fillColor('#fff').rect(50, summaryY, 495, 20).fill('#1a2a6c');
                    doc.fillColor('#fff').text('ACCOUNT SUMMARY', 60, summaryY + 3);

                    doc.fontSize(9).font('Helvetica').fillColor('#000').text(`Credit Limit`, 60, summaryY + 25);
                    doc.fontSize(10).font('Helvetica-Bold').text(`GHS ${parseFloat(customer.credit_limit).toFixed(2)}`, 280, summaryY + 25);

                    doc.fontSize(9).font('Helvetica').text(`Amount Owed`, 60, summaryY + 40);
                    doc.fontSize(10).font('Helvetica-Bold').fillColor('#c62828').text(`GHS ${amountUsed.toFixed(2)}`, 280, summaryY + 40);

                    doc.fontSize(9).font('Helvetica').fillColor('#000').text(`Available Credit`, 60, summaryY + 55);
                    doc.fontSize(10).font('Helvetica-Bold').fillColor('#2e7d32').text(`GHS ${creditAvailable.toFixed(2)}`, 280, summaryY + 55);

                    doc.fillColor('#000');
                    doc.y = summaryY + 90;
                    doc.moveDown(0.3);

                    // Item-based Table Header
                    const tableY = doc.y;
                    const tableX = 50;
                    const pageWidth = doc.page.width - 100;

                    // International Statement Title - aligned to the left
                    doc.fontSize(13).font('Helvetica-Bold').fillColor('#1a2a6c').text('STATEMENT OF PURCHASES', tableX, tableY, { width: pageWidth, align: 'left' });
                    doc.moveDown(0.4);

                    doc.rect(tableX, tableY, pageWidth, 18).fill('#1a2a6c');

                    // Table Headers - International style
                    doc.fontSize(9).font('Helvetica-Bold').fillColor('#fff');
                    doc.text('Date', tableX + 5, tableY + 2, { width: 60 });
                    doc.text('Item Description', tableX + 70, tableY + 2, { width: 210 });
                    doc.text('Qty', tableX + 290, tableY + 2, { width: 40, align: 'center' });
                    doc.text('Amount', tableX + 340, tableY + 2, { width: 80, align: 'right' });

                    doc.y = tableY + 20;

                    // Collect all items from all transactions for clean display
                    let allItems = [];
                    let totalAmount = 0;

                    // 1. Add Opening Balance Row if exists
                    if (Math.abs(openingBalance) > 0.01) {
                        allItems.push({
                            date: '',
                            name: 'Opening Balance (Previous Debt)',
                            qty: '-',
                            total: openingBalance,
                            isBold: true
                        });
                        totalAmount += openingBalance;
                    }

                    ledgerRes.rows.forEach((t) => {
                        const txnDate = new Date(t.date); // Ledger date
                        const formattedDate = txnDate.toLocaleDateString('en-GB', { day: '2-digit', month: '2-digit', year: '2-digit' });

                        if (t.type === 'PAYMENT') {
                            // Handle Payments
                            const amount = parseFloat(t.credit);
                            allItems.push({
                                date: formattedDate,
                                name: 'Payment Received',
                                qty: '-',
                                total: -amount, // Negative to reduce debt
                                isPayment: true
                            });
                            totalAmount -= amount;
                        } else if (t.type === 'SALE') {
                            let itemsArray = t.items;
                            if (typeof t.items === 'string') {
                                try {
                                    itemsArray = JSON.parse(t.items);
                                } catch (e) {
                                    itemsArray = [];
                                }
                            }

                            if (Array.isArray(itemsArray) && itemsArray.length > 0) {
                                itemsArray.forEach((item) => {
                                    const qty = parseFloat(item.quantity || item.qty || 0);
                                    const price = parseFloat(item.price || item.unit_price || 0);
                                    const subtotal = qty * price;
                                    // Calculate tax on the item (VAT is typically added on top)
                                    const taxAmount = subtotal * (vatRate / 100);
                                    const itemTotal = subtotal + taxAmount;
                                    totalAmount += itemTotal;

                                    allItems.push({
                                        date: formattedDate,
                                        name: item.name || item.product_name || 'Item',
                                        qty: qty,
                                        price: price,
                                        subtotal: subtotal,
                                        tax: taxAmount,
                                        total: itemTotal
                                    });
                                });
                            } else {
                                // Fallback for transactions with no items (Legacy/Manual)
                                const txnTotal = parseFloat(t.debit || 0);
                                totalAmount += txnTotal;
                                allItems.push({
                                    date: formattedDate,
                                    name: t.description || 'Transaction (No details)',
                                    qty: '-',
                                    total: txnTotal,
                                    isBold: false
                                });
                            }
                        }
                    });

                    // Display all items in clean international format
                    doc.fontSize(8).font('Helvetica').fillColor('#000');
                    let rowIdx = 0;

                    allItems.forEach((item) => {
                        if (doc.y > 700) {
                            doc.addPage();
                        }

                        const rowBg = rowIdx % 2 === 0 ? '#ffffff' : '#f8f8f8';
                        const itemY = doc.y;

                        doc.fillColor(rowBg).rect(tableX, itemY, pageWidth, 16).fill();
                        doc.rect(tableX, itemY, pageWidth, 16).stroke('#e8e8e8');

                        doc.fillColor(item.isPayment ? '#2e7d32' : '#000').fontSize(8).font(item.isBold ? 'Helvetica-Bold' : 'Helvetica');
                        doc.text(item.date, tableX + 5, itemY + 3, { width: 60 });
                        doc.text(item.name.substring(0, 45), tableX + 70, itemY + 3, { width: 210 });
                        doc.text(item.qty.toString(), tableX + 290, itemY + 3, { width: 40, align: 'center' });

                        doc.fillColor('#1a2a6c').font('Helvetica-Bold');
                        if (item.isPayment) doc.fillColor('#2e7d32'); // Green for payments
                        doc.text(`GHS ${Math.abs(item.total).toFixed(2)} ${item.isPayment ? '(CR)' : ''}`, tableX + 340, itemY + 3, { width: 80, align: 'right' });

                        doc.y = itemY + 17;
                        rowIdx++;
                    });

                    // Total Summary Row
                    if (doc.y > 680) {
                        doc.addPage();
                    }
                    const totalY = doc.y;
                    doc.fillColor('#1a2a6c').rect(tableX, totalY, pageWidth, 20).fill();
                    doc.fillColor('#fff').fontSize(9).font('Helvetica-Bold');
                    doc.text('CLOSING BALANCE', tableX + 5, totalY + 2, { width: 325 });
                    doc.fontSize(11).text(`GHS ${totalAmount.toFixed(2)}`, tableX + 340, totalY + 2, { width: 80, align: 'right' });

                    doc.y = totalY + 30;
                    doc.moveDown(0.5);

                    // Footer
                    doc.fontSize(9).font('Helvetica').fillColor('#1a2a6c').text('Thank you for your business.', { align: 'center' });
                    doc.fontSize(8).fillColor('#999').text('For account enquiries, please contact us at accounts@footprint.com', { align: 'center' });

                    doc.end();
                } catch (err) {
                    reject(err);
                }
            });

            // Wait for PDF to be generated
            pdfData = await pdfPromise;
        }

        // Prepare email with PDF attachment
        const transporter = nodemailer.createTransport({
            host: process.env.SMTP_HOST,
            port: process.env.SMTP_PORT,
            secure: process.env.SMTP_SECURE === 'true',
            auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
        });

        console.log('📨 Preparing email for:', customer.email);
        console.log('📄 PDF Size:', (pdfData.length / 1024).toFixed(2), 'KB');

        const emailHtml = `
            <div style="font-family: 'Arial', sans-serif; line-height: 1.6; color: #333; max-width: 600px;">
                <div style="background: linear-gradient(135deg, #1a2a6c, #b21f1f); padding: 30px; border-radius: 8px 8px 0 0; color: white; text-align: center;">
                    <h1 style="margin: 0; font-size: 28px;">FOOTPRINT RETAIL SYSTEMS</h1>
                    <p style="margin: 10px 0 0 0; font-size: 14px; opacity: 0.9;">Account Statement</p>
                </div>
                
                <div style="background: #f9f9f9; padding: 30px; border-radius: 0 0 8px 8px; border: 1px solid #e0e0e0;">
                    <p style="font-size: 14px; margin-bottom: 20px;">Dear <strong>${customer.name}</strong>,</p>
                    
                    <p style="font-size: 14px; margin-bottom: 20px;">
                        Please find attached your account statement for <strong style="color: #1a2a6c;">${monthName} ${year}</strong>.
                    </p>
                    
                    <div style="background: white; padding: 20px; border-left: 4px solid #b21f1f; margin: 20px 0; border-radius: 4px;">
                        <div style="display: flex; justify-content: space-between; margin-bottom: 15px;">
                            <span style="font-size: 13px; color: #666;">Credit Limit:</span>
                            <span style="font-size: 14px; font-weight: bold;">GHS ${parseFloat(customer.credit_limit).toFixed(2)}</span>
                        </div>
                            <div style="display: flex; justify-content: space-between; margin-bottom: 15px;">
                            <span style="font-size: 13px; color: #666;">Amount Used:</span>
                            <span style="font-size: 14px; font-weight: bold; color: #c62828;">GHS ${amountUsed.toFixed(2)}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between; margin-bottom: 15px;">
                            <span style="font-size: 13px; color: #666;">Available Credit:</span>
                            <span style="font-size: 14px; font-weight: bold; color: #2e7d32;">GHS ${(parseFloat(customer.credit_limit) - amountUsed).toFixed(2)}</span>
                        </div>
                    </div>
                    
                    <p style="font-size: 13px; color: #666; margin: 25px 0 15px 0;">
                        If you have any questions regarding your statement or account, please don't hesitate to contact us.
                    </p>
                    
                    <p style="font-size: 13px; color: #666; margin: 15px 0;">
                        <strong>Contact Us:</strong><br>
                        Phone: +233 (0) XXX XXX XXX<br>
                        Email: accounts@footprint.com
                    </p>
                    
                    <p style="font-size: 12px; color: #999; margin-top: 30px; border-top: 1px solid #eee; padding-top: 20px;">
                        This is an automated message. Please do not reply to this email.<br>
                        © 2026 Footprint Retail Systems. All rights reserved.
                    </p>
                </div>
            </div>
        `;

        const mailOptions = {
            from: `"Footprint Accounts" <${process.env.ADMIN_EMAIL}>`,
            to: customer.email,
            subject: `Account Statement - ${monthName} ${year}`,
            html: emailHtml,
            attachments: [{
                filename: `Statement_${monthName}_${year}_${customer.name.replace(/\s+/g, '_')}.pdf`,
                content: pdfData,
                contentType: 'application/pdf'
            }]
        };

        console.log('📤 Sending email...');
        const info = await transporter.sendMail(mailOptions);
        console.log('✅ Email sent successfully!');
        console.log('📧 Message ID:', info.messageId);

        await logActivity(req, 'EMAIL_STATEMENT', { customerId: req.params.id, month, year, messageId: info.messageId });
        res.json({
            success: true,
            message: 'Statement emailed successfully to ' + customer.email,
            details: {
                recipient: customer.email,
                month: monthName,
                year: year,
                messageId: info.messageId
            }
        });
    } catch (err) {
        console.error('❌ Email Statement Error:', err.message);
        console.error('Stack:', err.stack);

        // Provide specific error messages
        let errorMessage = 'Error sending statement: ' + err.message;
        let errorCode = 'STATEMENT_ERROR';

        if (err.message.includes('SMTP') || err.message.includes('connect')) {
            errorMessage = 'Email configuration error. Please check SMTP settings.';
            errorCode = 'SMTP_CONFIG_ERROR';
        } else if (err.message.includes('Authentication')) {
            errorMessage = 'Email authentication failed. Please check SMTP credentials.';
            errorCode = 'SMTP_AUTH_ERROR';
        } else if (err.message.includes('Customer not found')) {
            errorMessage = 'Customer not found';
            errorCode = 'CUSTOMER_NOT_FOUND';
        } else if (err.message.includes('no email')) {
            errorMessage = 'Customer does not have an email address on file';
            errorCode = 'NO_CUSTOMER_EMAIL';
        }

        res.status(500).json({
            success: false,
            message: errorMessage,
            code: errorCode,
            details: process.env.NODE_ENV === 'development' ? err.message : undefined
        });
    }
});

// Reset Password Route (after email confirmation)
app.post('/api/reset-password', async (req, res) => {
    const { token, newPassword } = req.body;

    try {
        // Hash the token from the user to compare with the stored hashed token
        const hashedToken = crypto.createHash('sha256').update(token).digest('hex');

        // 1. Validate token and expiry
        const userRes = await pool.query(
            'SELECT id FROM users WHERE reset_token = $1 AND reset_token_expiry > NOW()',
            [hashedToken]
        );

        if (userRes.rows.length === 0) {
            return res.status(400).json({ message: 'Invalid or expired reset link. Please try again.' });
        }

        const userId = userRes.rows[0].id;

        // 2. Hash new password
        const salt = await bcrypt.genSalt(10);
        const hashedPassword = await bcrypt.hash(newPassword, salt);

        // 3. Update password and clear reset token fields
        await pool.query(
            'UPDATE users SET password = $1, reset_token = NULL, reset_token_expiry = NULL WHERE id = $2',
            [hashedPassword, userId]
        );

        // Log activity
        await logActivity({ user: { id: userId } }, 'PASSWORD_RESET_CONFIRMED', { userId });

        res.json({ success: true, message: 'Password has been reset successfully.' });

    } catch (error) {
        console.error('Password reset confirmation error:', error);
        res.status(500).json({ message: 'Error resetting password. Please try again.' });
    }
});

// ============ AUDIT LOGS ENDPOINT ============
app.get('/api/audit-logs', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.role !== 'manager') {
        return res.status(403).json({ message: 'Unauthorized' });
    }
    try {
        const result = await pool.query(`
            SELECT a.*, 
                COALESCE(u.name, cu.company_name) as user_name, 
                CASE 
                    WHEN u.role IS NOT NULL THEN u.role
                    WHEN cu.id IS NOT NULL THEN 'Company'
                    ELSE 'System'
                END as user_role 
            FROM activity_logs a 
            LEFT JOIN users u ON a.user_id = u.id 
            LEFT JOIN company_users cu ON a.user_id = cu.id
            ORDER BY a.created_at DESC LIMIT 200
        `);
        await logActivity(req, 'VIEW_AUDIT_LOGS');
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Generic Client-Side Logging Endpoint
app.post('/api/audit/log', authenticateToken, async (req, res) => {
    const { action, details } = req.body;
    try {
        await logActivity(req, action, details);
        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ message: 'Error logging activity' });
    }
});

// ============ TAX MANAGEMENT (CEO Portal) ============

// Get active tax rules (Public for POS - isolated per branch)
app.get('/api/taxes/active', authenticateToken, async (req, res) => {
    try {
        const branchId = req.user.store_id || 1;
        const userType = req.user.type || 'staff';

        // 1. Fetch Additional Taxes
        let query = "SELECT id, name, rate FROM tax_rules WHERE status = 'Active' AND branch_id = $1 ORDER BY name ASC";
        let params = [branchId];

        // Refined Isolation: Global taxes (Branch 1) flow to ALL physical branches but NOT to Company Portals
        if (userType !== 'company') {
            query = "SELECT id, name, rate FROM tax_rules WHERE status = 'Active' AND (branch_id = $1 OR branch_id = 1) ORDER BY name ASC";
        }

        const rulesRes = await pool.query(query, params);
        let rules = rulesRes.rows;

        // 2. Fetch System VAT (for consistency with POS expectations)
        let settingsRes = await pool.query("SELECT vat_rate FROM system_settings WHERE branch_id = $1", [branchId]);
        if (settingsRes.rows.length === 0) {
            settingsRes = await pool.query("SELECT vat_rate FROM system_settings WHERE branch_id = 1");
        }

        const vatRate = settingsRes.rows.length > 0 ? parseFloat(settingsRes.rows[0].vat_rate) : 0;

        // 3. Combine (add VAT if it's not already explicitly in the list)
        if (vatRate > 0 && !rules.some(r => r.name.toUpperCase() === 'VAT')) {
            rules.unshift({ id: 'vat', name: 'VAT', rate: vatRate, status: 'Active' });
        }

        res.json(rules);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Get all tax rules for Management (isolated per portal)
app.get('/api/taxes', authenticateToken, async (req, res) => {
    // Allow admin, ceo, or company users
    if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.type !== 'company') {
        return res.status(403).json({ message: 'Unauthorized' });
    }

    try {
        const branchId = req.user.store_id || 1;
        const userType = req.user.type || 'staff';

        let query = 'SELECT * FROM tax_rules WHERE branch_id = $1 ORDER BY created_at DESC';
        let params = [branchId];

        // Management Isolation: CEO/Admin can see Global (Branch 1) and their specific context
        if (userType !== 'company' && (req.user.role === 'ceo' || req.user.role === 'admin')) {
            query = 'SELECT * FROM tax_rules WHERE (branch_id = $1 OR branch_id = 1) ORDER BY created_at DESC';
        }

        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Add tax rule (auto-assign to current branch/portal)
app.post('/api/taxes', authenticateToken, async (req, res) => {
    // Allow admin, ceo, or company users
    if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.type !== 'company') {
        return res.status(403).json({ message: 'Unauthorized' });
    }

    const { name, rate } = req.body;
    try {
        const branchId = req.user.store_id || 1;
        await pool.query('INSERT INTO tax_rules (name, rate, branch_id) VALUES ($1, $2, $3)', [name, rate, branchId]);
        await logActivity(req, 'CREATE_TAX_RULE', { name, rate, branchId });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error creating tax rule' });
    }
});

// Update tax rule
app.put('/api/taxes/:id', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    const { id } = req.params;
    const { name, rate, status } = req.body;
    try {
        await pool.query(
            'UPDATE tax_rules SET name = COALESCE($1, name), rate = COALESCE($2, rate), status = COALESCE($3, status) WHERE id = $4',
            [name, rate, status, id]
        );
        await logActivity(req, 'UPDATE_TAX_RULE', { id, name, rate, status });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error updating tax rule' });
    }
});

// Delete tax rule
app.delete('/api/taxes/:id', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    const { id } = req.params;
    try {
        await pool.query('DELETE FROM tax_rules WHERE id = $1', [id]);
        await logActivity(req, 'DELETE_TAX_RULE', { id });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error deleting tax rule' });
    }
});

// Calculate Tax Liability Report
app.get('/api/ceo/tax-report', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });

    const { startDate, endDate, branch } = req.query;

    try {
        // 1. Get Active Tax Rules (for Columns) - Isolated from Company Portal
        const branchId = req.user.store_id || 1;
        const rulesRes = await pool.query(
            "SELECT * FROM tax_rules WHERE status = 'Active' AND (branch_id = $1 OR branch_id = 1) ORDER BY created_at DESC",
            [branchId]
        );
        const activeRules = rulesRes.rows;

        // Fetch System VAT (Default to Branch 1)
        const settingsRes = await pool.query("SELECT vat_rate FROM system_settings WHERE branch_id = 1");
        const systemVat = settingsRes.rows.length > 0 ? parseFloat(settingsRes.rows[0].vat_rate) : 15.0;

        // Define Report Columns (VAT + Active Rules)
        // Using 'VAT' to match POS storage naming
        const reportRules = [{ name: 'VAT', rate: systemVat }, ...activeRules];

        // 2. Build Transaction Query
        let whereClause = "status = 'completed'";
        let params = [];

        if (startDate && endDate) {
            whereClause += ` AND created_at >= $${params.length + 1} AND created_at <= $${params.length + 2}::date + INTERVAL '1 day' - INTERVAL '1 second'`;
            params.push(startDate, endDate);
        } else {
            whereClause += ` AND created_at >= date_trunc('month', CURRENT_DATE)`;
        }

        if (branch && branch !== 'All Branches') {
            whereClause += ` AND store_location = $${params.length + 1}`;
            params.push(branch);
        }

        // 3. Aggregate Data in DB (Optimized)

        // Query A: Legacy Transactions (No Breakdown) -> Calculate VAT manually later
        const legacyQuery = `
            SELECT COALESCE(store_location, 'Main Branch') as branch, SUM(total_amount) as gross
            FROM transactions
            WHERE ${whereClause} AND tax_breakdown IS NULL
            GROUP BY store_location
        `;

        // Query B: New Transactions (Has Breakdown) -> Gross Revenue
        const newGrossQuery = `
            SELECT COALESCE(store_location, 'Main Branch') as branch, SUM(total_amount) as gross
            FROM transactions
            WHERE ${whereClause} AND tax_breakdown IS NOT NULL
            GROUP BY store_location
        `;

        // Query C: New Transactions -> Tax Breakdown Sums
        const newTaxQuery = `
            SELECT 
                COALESCE(t.store_location, 'Main Branch') as branch,
                elem->>'name' as tax_name,
                SUM((elem->>'amount')::numeric) as tax_amount
            FROM transactions t
            CROSS JOIN LATERAL jsonb_array_elements(t.tax_breakdown) as elem
            WHERE ${whereClause} AND t.tax_breakdown IS NOT NULL AND jsonb_typeof(t.tax_breakdown) = 'array'
            GROUP BY t.store_location, elem->>'name'
        `;

        const [legacyRes, newGrossRes, newTaxRes] = await Promise.all([
            pool.query(legacyQuery, params),
            pool.query(newGrossQuery, params),
            pool.query(newTaxQuery, params)
        ]);

        const branchData = {};
        const initBranch = (b) => { if (!branchData[b]) branchData[b] = { gross: 0, taxes: {} }; };

        // Process Legacy (Calculate VAT manually)
        legacyRes.rows.forEach(row => {
            const b = row.branch;
            const gross = parseFloat(row.gross);
            initBranch(b);
            // Legacy Calc: Net = Gross / (1 + rate), VAT = Gross - Net
            const net = gross / (1 + (systemVat / 100));
            const vat = gross - net;
            branchData[b].gross += gross;
            branchData[b].taxes['VAT'] = (branchData[b].taxes['VAT'] || 0) + vat;
        });

        // Process New Data
        newGrossRes.rows.forEach(row => { initBranch(row.branch); branchData[row.branch].gross += parseFloat(row.gross); });
        newTaxRes.rows.forEach(row => { initBranch(row.branch); branchData[row.branch].taxes[row.tax_name] = (branchData[row.branch].taxes[row.tax_name] || 0) + parseFloat(row.tax_amount); });

        // 4. Format Output
        const report = Object.keys(branchData).map(bName => {
            const data = branchData[bName];
            const totalTax = Object.values(data.taxes).reduce((a, b) => a + b, 0);
            const netRevenue = data.gross - totalTax;

            // Map to columns defined in reportRules
            const breakdown = reportRules.map(rule => ({
                name: rule.name,
                rate: rule.rate,
                amount: data.taxes[rule.name] || 0
            }));

            return {
                branch: bName,
                grossRevenue: data.gross,
                netRevenue: netRevenue,
                totalTax: totalTax,
                breakdown: breakdown
            };
        });

        await logActivity(req, 'GENERATE_TAX_REPORT', { startDate, endDate, branch });

        res.json({
            period: { start: startDate || 'Month Start', end: endDate || 'Now' },
            rules: reportRules,
            report: report
        });

    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error generating tax report' });
    }
});

// ============ COMPANY PORTAL ENDPOINTS ============

// Company Dashboard Data
app.get('/api/company/dashboard', authenticateToken, async (req, res) => {
    try {
        const companyId = req.user.id;

        // Get dashboard statistics including credit metrics
        const [proformaResult, salesResult, revenueResult, balanceResult, draftProformaResult, creditResult] = await Promise.all([
            pool.query('SELECT COUNT(*) as count FROM proforma_invoices WHERE created_by = $1', [companyId]),
            pool.query('SELECT COUNT(*) as count FROM sales_invoices WHERE created_by = $1', [companyId]),
            pool.query('SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_invoices WHERE created_by = $1', [companyId]),
            pool.query('SELECT COALESCE(SUM(total_amount - paid_amount), 0) as total FROM sales_invoices WHERE created_by = $1 AND status != $2', [companyId, 'Paid']),
            pool.query("SELECT COUNT(*) as count FROM proforma_invoices WHERE created_by = $1 AND status ILIKE 'Draft'", [companyId]),
            pool.query(`
                SELECT 
                    COALESCE(SUM(current_balance), 0) as total_owed,
                    COALESCE(SUM(credit_limit), 0) as total_limit,
                    COUNT(*) FILTER (WHERE current_balance > 0) as active_debtors
                FROM customers
            `)
        ]);

        // Get recent activity
        const recentQuery = `
            SELECT 'Proforma' as type, invoice_number, issue_date as date, total_amount as amount, status
            FROM proforma_invoices 
            WHERE created_by = $1
            UNION ALL
            SELECT 'Sales' as type, invoice_number, issue_date as date, total_amount as amount, status
            FROM sales_invoices 
            WHERE created_by = $1
            ORDER BY date DESC 
            LIMIT 10
        `;
        const recentResult = await pool.query(recentQuery, [companyId]);

        const totalOwed = parseFloat(creditResult.rows[0].total_owed);
        const totalLimit = parseFloat(creditResult.rows[0].total_limit);
        const usageRate = totalLimit > 0 ? (totalOwed / totalLimit) * 100 : 0;

        res.json({
            totalProforma: parseInt(proformaResult.rows[0].count),
            totalSales: parseInt(salesResult.rows[0].count),
            totalRevenue: parseFloat(revenueResult.rows[0].total),
            outstandingBalance: parseFloat(balanceResult.rows[0].total),
            pendingPayments: parseInt(draftProformaResult.rows[0].count),
            totalCreditOwed: totalOwed,
            creditUsageRate: usageRate.toFixed(1),
            activeDebtors: parseInt(creditResult.rows[0].active_debtors),
            recentActivity: recentResult.rows
        });

    } catch (error) {
        console.error('Company dashboard error:', error);
        res.status(500).json({ message: 'Error loading dashboard data' });
    }
});

// Get Proforma Invoices
app.get('/api/company/proforma-invoices', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT * FROM proforma_invoices WHERE created_by = $1 ORDER BY issue_date DESC',
            [req.user.id]
        );

        res.json(result.rows);

    } catch (error) {
        console.error('Proforma invoices error:', error);
        res.status(500).json({ message: 'Error loading proforma invoices' });
    }
});

// Create Proforma Invoice
app.post('/api/company/proforma-invoices', authenticateToken, async (req, res) => {
    try {
        const {
            invoice_number, expiry_date, items, subtotal,
            markup_type, markup_value, markup_amount,
            discount_type, discount_value, discount_amount,
            total_amount, notes
        } = req.body;

        const client = await pool.connect();

        try {
            await client.query('BEGIN');

            // Get company ID
            const companyResult = await client.query('SELECT id FROM companies ORDER BY id LIMIT 1');
            const companyId = companyResult.rows[0].id;

            // Create proforma invoice
            const proformaResult = await client.query(`
                INSERT INTO proforma_invoices (
                    company_id, invoice_number, issue_date, expiry_date,
                    subtotal, markup_type, markup_value, markup_amount,
                    discount_type, discount_value, discount_amount,
                    total_amount, notes, created_by
                ) VALUES ($1, $2, NOW(), $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                RETURNING *
            `, [
                companyId, invoice_number, expiry_date, subtotal,
                markup_type, markup_value, markup_amount,
                discount_type, discount_value, discount_amount,
                total_amount, notes, req.user.id
            ]);

            const proformaId = proformaResult.rows[0].id;

            // Insert items
            for (const item of items) {
                await client.query(`
                    INSERT INTO proforma_invoice_items (
                        proforma_id, product_name, quantity, unit_price, line_total, product_id, barcode
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                `, [proformaId, item.product_name, item.quantity, item.unit_price, item.line_total, item.product_id, item.barcode]);
            }

            await client.query('COMMIT');
            res.json({ success: true, id: proformaId });

        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }

    } catch (error) {
        console.error('Create proforma invoice error:', error);
        res.status(500).json({ message: 'Error creating proforma invoice' });
    }
});

// Get Sales Invoices
app.get('/api/company/sales-invoices', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query(
            'SELECT * FROM sales_invoices WHERE created_by = $1 ORDER BY issue_date DESC',
            [req.user.id]
        );

        res.json(result.rows);

    } catch (error) {
        console.error('Sales invoices error:', error);
        res.status(500).json({ message: 'Error loading sales invoices' });
    }
});

// Create Sales Invoice
app.post('/api/company/sales-invoices', authenticateToken, async (req, res) => {
    try {
        const {
            invoice_number, due_date, items, subtotal,
            markup_type, markup_value, markup_amount,
            discount_type, discount_value, discount_amount,
            total_amount, notes
        } = req.body;

        const client = await pool.connect();

        try {
            await client.query('BEGIN');

            // Get company ID
            const companyResult = await client.query('SELECT id FROM companies ORDER BY id LIMIT 1');
            const companyId = companyResult.rows[0].id;

            // Create sales invoice
            const salesResult = await client.query(`
                INSERT INTO sales_invoices (
                    company_id, invoice_number, issue_date, due_date,
                    subtotal, markup_type, markup_value, markup_amount,
                    discount_type, discount_value, discount_amount,
                    total_amount, notes, created_by
                ) VALUES ($1, $2, NOW(), $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                RETURNING *
            `, [
                companyId, invoice_number, due_date, subtotal,
                markup_type, markup_value, markup_amount,
                discount_type, discount_value, discount_amount,
                total_amount, notes, req.user.id
            ]);

            const salesId = salesResult.rows[0].id;

            // Insert items
            for (const item of items) {
                await client.query(`
                    INSERT INTO sales_invoice_items (
                        invoice_id, product_name, quantity, unit_price, line_total, product_id, barcode
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                `, [salesId, item.product_name, item.quantity, item.unit_price, item.line_total, item.product_id, item.barcode]);
            }

            await client.query('COMMIT');
            res.json({ success: true, id: salesId });

        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }

    } catch (error) {
        console.error('Create sales invoice error:', error);
        res.status(500).json({ message: 'Error creating sales invoice' });
    }
});

// Get Transactions
app.get('/api/company/transactions', authenticateToken, async (req, res) => {
    try {
        const query = `
            SELECT ct.*, si.invoice_number
            FROM company_transactions ct
            LEFT JOIN sales_invoices si ON ct.invoice_id = si.id
            WHERE ct.company_id = (SELECT id FROM companies ORDER BY id LIMIT 1)
            ORDER BY ct.created_at DESC
        `;

        const result = await pool.query(query);
        res.json(result.rows);

    } catch (error) {
        console.error('Transactions error:', error);
        res.status(500).json({ message: 'Error loading transactions' });
    }
});

// Quick Sale Processing
app.post('/api/company/quick-sale', authenticateToken, async (req, res) => {
    const client = await pool.connect();
    try {
        const { items, total, subtotal, discount, tax, tax_details, payment_method, customer_name, skip_stock, type, proforma_id, customer_id } = req.body;
        let newId;

        await client.query('BEGIN');

        // Resolve company_id (matching pattern from existing invoice routes)
        const companyResult = await client.query('SELECT id FROM companies ORDER BY id LIMIT 1');
        if (companyResult.rows.length === 0) throw new Error('Company record not found');
        const companyId = companyResult.rows[0].id;

        const invoiceNumber = (type === 'proforma' ? 'PRO-Q-' : 'INV-Q-') + Date.now();

        if (type === 'proforma') {
            const proformaResult = await client.query(`
                INSERT INTO proforma_invoices (
                    company_id, invoice_number, issue_date, expiry_date,
                    subtotal, discount_amount, tax_amount, tax_details, total_amount, notes, created_by, client_name, payment_method, customer_id
                ) VALUES ($1, $2, NOW(), NOW() + INTERVAL '30 days', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                RETURNING id
            `, [companyId, invoiceNumber, subtotal, discount, tax || 0, JSON.stringify(tax_details || []), total, `Quick Document for ${customer_name}`, req.user.id, customer_name, payment_method, customer_id]);

            newId = proformaResult.rows[0].id;

            for (const item of items) {
                await client.query(`
                    INSERT INTO proforma_invoice_items (
                        proforma_id, product_name, quantity, unit_price, line_total, product_id, barcode
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                `, [newId, item.product_name, item.quantity, item.final_price || item.unit_price, item.total, item.product_id, item.barcode]);
            }
        } else {
            // Create sales invoice
            const status = payment_method === 'credit' ? 'Unpaid' : 'Paid';
            const salesResult = await client.query(`
                INSERT INTO sales_invoices (
                    company_id, invoice_number, issue_date, due_date,
                    subtotal, discount_amount, tax_amount, tax_details, total_amount, notes, created_by, client_name, payment_method, status, customer_id
                ) VALUES ($1, $2, NOW(), NOW() + INTERVAL '30 days', $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                RETURNING id
            `, [companyId, invoiceNumber, subtotal, discount, tax || 0, JSON.stringify(tax_details || []), total, `Quick Document for ${customer_name}`, req.user.id, customer_name, payment_method, status, customer_id]);

            newId = salesResult.rows[0].id;

            // If credit payment, update customer balance
            if (payment_method === 'credit' && customer_id) {
                // Verify limit again on server side for safety
                const custRes = await client.query('SELECT credit_limit, current_balance FROM customers WHERE id = $1', [customer_id]);
                if (custRes.rows.length > 0) {
                    const cust = custRes.rows[0];
                    const newBalance = parseFloat(cust.current_balance) + parseFloat(total);
                    if (newBalance > parseFloat(cust.credit_limit)) {
                        throw new Error('Credit limit exceeded');
                    }
                    await client.query('UPDATE customers SET current_balance = current_balance + $1 WHERE id = $2', [total, customer_id]);
                }
            }

            for (const item of items) {
                await client.query(`
                    INSERT INTO sales_invoice_items (
                        invoice_id, product_name, quantity, unit_price, line_total, product_id, barcode
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                `, [newId, item.product_name, item.quantity, item.final_price || item.unit_price, item.total, item.product_id, item.barcode]);
            }

            // Record in transaction history for Sales Invoices/Quick Sales
            await client.query(`
                INSERT INTO company_transactions (company_id, invoice_id, transaction_type, amount, created_by)
                VALUES ($1, $2, 'Sale', $3, $4)
            `, [companyId, newId, total, req.user.id]);

            // If this sale came from a proforma, mark it as completed
            if (proforma_id) {
                await client.query(`UPDATE proforma_invoices SET status = 'Completed' WHERE id = $1`, [proforma_id]);
            }

            // --- INVENTORY DEDUCTION LOGIC (Company Portal Sales) ---
            if (!skip_stock) {
                // Resolve branch for deduction (prioritizing NOVELTY then DZORWULU)
                const branchSearch = ['%NOVELTY%', '%Novelty%', '%DZORWULU%', '%Dzorwulu%'];
                const dzorBranchRes = await client.query(`
                    SELECT name FROM branches 
                    WHERE name ILIKE ANY($1) OR location ILIKE ANY($1)
                    ORDER BY CASE 
                        WHEN name ILIKE '%NOVELTY%' THEN 1 
                        WHEN name ILIKE '%DZORWULU%' THEN 2 
                        ELSE 3 END
                    LIMIT 1
                `, [branchSearch]);

                let targetBranch = dzorBranchRes.rows.length > 0 ? dzorBranchRes.rows[0].name : 'NOVELTY';

                for (const item of items) {
                    const pid = item.id || item.product_id;
                    const barcode = item.barcode;

                    if (pid || (barcode && barcode !== 'N/A')) {
                        // Resolve which key actually exists in stock_levels (handle casing issues)
                        const prodRes = await client.query(`
                            SELECT stock_levels FROM products WHERE ${pid ? 'id = $1' : 'barcode = $1'}
                        `, [pid || barcode]);

                        let actualKey = null; // null = key not found in this product's stock_levels
                        if (prodRes.rows.length > 0) {
                            const levels = prodRes.rows[0].stock_levels || {};
                            const keys = Object.keys(levels);
                            const match = keys.find(k => k.toUpperCase() === targetBranch.toUpperCase());
                            if (match) actualKey = match;
                        }

                        // 1. Update stock_levels JSONB and total stock column
                        const productIdentifier = pid ? 'id = $3' : 'barcode = $3';
                        const identifierValue = pid || barcode;

                        if (actualKey) {
                            // Key exists in stock_levels — decrement it safely
                            await client.query(`
                                UPDATE products 
                                SET 
                                    stock_levels = jsonb_set(
                                        COALESCE(stock_levels, '{}'::jsonb), 
                                        ARRAY[$1], 
                                        (COALESCE((stock_levels->>$1)::int, 0) - $2)::text::jsonb
                                    ),
                                    stock = COALESCE(stock, 0) - $2
                                WHERE ${productIdentifier}
                            `, [actualKey, item.quantity, identifierValue]);
                        } else {
                            // Key does NOT exist for this branch — only decrement the total stock column.
                            // Do NOT create a new negative key in stock_levels (that was the bug).
                            console.warn(`[COMPANY SALE] Branch key "${targetBranch}" not found in stock_levels for product ${pid || barcode}. Skipping jsonb_set to prevent phantom negatives. Decrementing total stock only.`);
                            await client.query(`
                                UPDATE products 
                                SET stock = COALESCE(stock, 0) - $1
                                WHERE ${pid ? 'id = $2' : 'barcode = $2'}
                            `, [item.quantity, identifierValue]);
                        }

                        // 2. Log inventory movement for audit
                        // Note: Using $6 for the product identification in the WHERE clause to avoid indexing conflicts
                        await client.query(`
                            INSERT INTO inventory_audit_log (
                                action_type, product_barcode, quantity_before, quantity_after, 
                                reference_id, reference_type, user_id, branch_id, notes
                            )
                            SELECT 
                                'Stock Out', $1::text, (COALESCE((stock_levels->>$2::text)::int, 0) + $3::int), 
                                (COALESCE((stock_levels->>$2::text)::int, 0)), $4::int, 'Quick Sale', $5::int, 
                                (SELECT id FROM branches WHERE name = $2::text LIMIT 1), 'Company Portal Quick Sale (' || $2 || ')'
                            FROM products WHERE ${pid ? 'id = $6' : 'barcode = $6'}
                        `, [barcode || '', actualKey || targetBranch, item.quantity, newId, req.user.id, identifierValue]);
                    }
                }
            }
        }

        const activityType = skip_stock ? `COMPANY_${payment_method.toUpperCase()}_GENERATED` : 'COMPANY_QUICK_SALE';
        await logActivity(req, activityType, { invoiceNumber, total, customer_name, items_count: items.length });

        await client.query('COMMIT');
        res.json({ success: true, invoice_number: invoiceNumber, id: newId });
    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Quick sale error:', error);
        res.status(500).json({ message: 'Error processing sale' });
    } finally {
        client.release();
    }
});

// Convert Proforma to Sales Invoice
app.post('/api/company/proforma-invoices/:id/convert', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        const proRes = await client.query('SELECT * FROM proforma_invoices WHERE id = $1', [id]);
        if (proRes.rows.length === 0) throw new Error('Proforma not found');
        const pro = proRes.rows[0];

        const itemsRes = await client.query('SELECT * FROM proforma_invoice_items WHERE proforma_id = $1', [id]);

        const invNum = 'INV-C-' + Date.now();
        const salesRes = await client.query(`
            INSERT INTO sales_invoices (
                company_id, invoice_number, proforma_id, issue_date, due_date,
                subtotal, discount_amount, tax_amount, tax_details, total_amount, notes, created_by, status, client_name
            ) VALUES ($1, $2, $3, NOW(), NOW() + INTERVAL '30 days', $4, $5, $6, $7, $8, $9, $10, 'Unpaid', $11)
            RETURNING id
        `, [pro.company_id, invNum, id, pro.subtotal, pro.discount_amount, pro.tax_amount, pro.tax_details, pro.total_amount, pro.notes, req.user.id, pro.client_name]);

        const salesId = salesRes.rows[0].id;

        for (const item of itemsRes.rows) {
            await client.query(`
                INSERT INTO sales_invoice_items (
                    invoice_id, product_name, quantity, unit_price, line_total, product_id, barcode
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            `, [salesId, item.product_name, item.quantity, item.unit_price, item.line_total, item.product_id, item.barcode]);
        }

        await client.query("UPDATE proforma_invoices SET status = 'Converted' WHERE id = $1", [id]);

        await logActivity(req, 'COMPANY_PROFORMA_CONVERTED', { proforma_id: id, invoice_number: invNum, total: pro.total_amount });

        await client.query('COMMIT');
        res.json({ success: true, invoice_number: invNum });
    } catch (error) {
        await client.query('ROLLBACK');
        res.status(500).json({ message: error.message });
    } finally { client.release(); }
});

// Update Proforma Status to Sent
app.post('/api/company/proforma-invoices/:id/send', authenticateToken, async (req, res) => {
    try {
        await pool.query("UPDATE proforma_invoices SET status = 'Sent' WHERE id = $1", [req.params.id]);
        await logActivity(req, 'COMPANY_PROFORMA_SENT', { proforma_id: req.params.id });
        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ message: 'Error sending proforma' });
    }
});

// Record Payment for Sales Invoice
app.post('/api/company/sales-invoices/:id/payment', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const { amount } = req.body;
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const invRes = await client.query('SELECT total_amount, paid_amount FROM sales_invoices WHERE id = $1', [id]);
        if (invRes.rows.length === 0) throw new Error('Invoice not found');
        const inv = invRes.rows[0];

        const newPaid = parseFloat(inv.paid_amount || 0) + parseFloat(amount);
        const status = newPaid >= parseFloat(inv.total_amount) ? 'Paid' : 'Partially Paid';

        await client.query(`
            UPDATE sales_invoices SET paid_amount = $1, status = $2 WHERE id = $3
        `, [newPaid, status, id]);

        await logActivity(req, 'COMPANY_PAYMENT_RECORDED', { invoice_id: id, amount: amount, new_paid_total: newPaid, status: status });

        await client.query('COMMIT');
        res.json({ success: true });
    } catch (error) {
        await client.query('ROLLBACK');
        res.status(500).json({ message: error.message });
    } finally { client.release(); }
});

// Get Single Proforma Detail
app.get('/api/company/proforma-invoices/:id', authenticateToken, async (req, res) => {
    try {
        const id = req.params.id;
        const mainRes = await pool.query('SELECT * FROM proforma_invoices WHERE id = $1', [id]);
        if (mainRes.rows.length === 0) return res.status(404).json({ message: 'Proforma not found' });

        const itemsRes = await pool.query('SELECT * FROM proforma_invoice_items WHERE proforma_id = $1', [id]);
        const data = mainRes.rows[0];
        data.items = itemsRes.rows;
        res.json(data);
    } catch (e) {
        res.status(500).json({ message: 'Error fetching proforma detail' });
    }
});

// Get Single Sales Invoice Detail
app.get('/api/company/sales-invoices/:id', authenticateToken, async (req, res) => {
    try {
        const id = req.params.id;
        const mainRes = await pool.query('SELECT * FROM sales_invoices WHERE id = $1', [id]);
        if (mainRes.rows.length === 0) return res.status(404).json({ message: 'Invoice not found' });

        const itemsRes = await pool.query('SELECT * FROM sales_invoice_items WHERE invoice_id = $1', [id]);
        const data = mainRes.rows[0];
        data.items = itemsRes.rows;
        res.json(data);
    } catch (e) {
        res.status(500).json({ message: 'Error fetching invoice detail' });
    }
});

// Company Tax Management Endpoints
app.get('/api/company/taxes', authenticateToken, async (req, res) => {
    try {
        // Strict isolation: Only return taxes belonging to THIS company account
        const companyBranchId = req.user.store_id;
        if (!companyBranchId) {
            return res.json([]); // No store context = no taxes
        }
        const result = await pool.query(
            'SELECT * FROM tax_rules WHERE branch_id = $1 ORDER BY created_at DESC',
            [companyBranchId]
        );
        res.json(result.rows);
    } catch (error) {
        console.error('Company taxes error:', error);
        res.status(500).json({ message: 'Error loading tax rules' });
    }
});

app.post('/api/company/taxes', authenticateToken, async (req, res) => {
    try {
        const { name, rate } = req.body;

        if (!name || rate === undefined || rate === null) {
            return res.status(400).json({ message: 'Tax name and rate are required' });
        }

        // Assign this tax to the company's unique store_id
        const companyBranchId = req.user.store_id;
        if (!companyBranchId) {
            return res.status(400).json({ message: 'No company context found. Please log out and log back in.' });
        }

        const result = await pool.query(
            'INSERT INTO tax_rules (name, rate, branch_id, status, created_at) VALUES ($1, $2, $3, $4, NOW()) RETURNING *',
            [name, rate, companyBranchId, 'Active']
        );

        await logActivity(req, 'COMPANY_TAX_CREATED', { name, rate });

        res.json(result.rows[0]);
    } catch (error) {
        console.error('Add tax error:', error);
        res.status(500).json({ message: 'Error adding tax rule' });
    }
});

app.put('/api/company/taxes/:id/toggle', authenticateToken, async (req, res) => {
    try {
        const taxId = req.params.id;
        const companyBranchId = req.user.store_id;

        // Only allow toggling taxes that belong to THIS company
        const currentResult = await pool.query(
            'SELECT status FROM tax_rules WHERE id = $1 AND branch_id = $2',
            [taxId, companyBranchId]
        );
        if (currentResult.rows.length === 0) {
            return res.status(404).json({ message: 'Tax rule not found' });
        }

        const currentStatus = currentResult.rows[0].status;
        const newStatus = currentStatus === 'Active' ? 'Inactive' : 'Active';

        const result = await pool.query(
            'UPDATE tax_rules SET status = $1 WHERE id = $2 AND branch_id = $3 RETURNING *',
            [newStatus, taxId, companyBranchId]
        );

        await logActivity(req, 'COMPANY_TAX_TOGGLED', { tax_id: taxId, new_status: newStatus });

        res.json(result.rows[0]);
    } catch (error) {
        console.error('Toggle tax error:', error);
        res.status(500).json({ message: 'Error updating tax rule' });
    }
});

app.delete('/api/company/taxes/:id', authenticateToken, async (req, res) => {
    try {
        const taxId = req.params.id;
        const companyBranchId = req.user.store_id;

        // Only allow deleting taxes that belong to THIS company
        const result = await pool.query(
            'DELETE FROM tax_rules WHERE id = $1 AND branch_id = $2 RETURNING *',
            [taxId, companyBranchId]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ message: 'Tax rule not found' });
        }

        res.json({ message: 'Tax rule deleted successfully' });
    } catch (error) {
        console.error('Delete tax error:', error);
        res.status(500).json({ message: 'Error deleting tax rule' });
    }
});

// Company Tax Report from Transactions
app.get('/api/company/tax-report', authenticateToken, async (req, res) => {
    try {
        const { startDate, endDate } = req.query;

        let whereClause = 'ct.company_id = (SELECT id FROM companies ORDER BY id LIMIT 1)';
        let params = [];

        if (startDate && endDate) {
            whereClause += ` AND ct.created_at >= $1 AND ct.created_at <= $2::date + INTERVAL '1 day' - INTERVAL '1 second'`;
            params.push(startDate, endDate);
        }

        const query = `
            SELECT 
                SUM(ct.amount) as gross_sales,
                SUM(COALESCE(si.tax_amount, 0)) as total_tax,
                SUM(ct.amount - COALESCE(si.tax_amount, 0)) as net_sales
            FROM company_transactions ct
            LEFT JOIN sales_invoices si ON ct.invoice_id = si.id
            WHERE ${whereClause}
        `;

        const result = await pool.query(query, params);
        const row = result.rows[0];

        if (!row || !row.gross_sales) {
            return res.json([{
                period: startDate && endDate ? `${startDate} to ${endDate}` : 'All Time',
                gross_sales: 0,
                net_sales: 0,
                total_tax: 0
            }]);
        }

        res.json([{
            period: startDate && endDate ? `${startDate} to ${endDate}` : 'All Time',
            gross_sales: parseFloat(row.gross_sales) || 0,
            net_sales: parseFloat(row.net_sales) || 0,
            total_tax: parseFloat(row.total_tax) || 0
        }]);

    } catch (error) {
        console.error('Company tax report error:', error);
        res.status(500).json({ message: 'Error generating report' });
    }
});

// Serve static files (HTML, CSS, JS) - Place after API routes
// Security: Prevent access to sensitive source files if serving from root
app.use((req, res, next) => {
    const sensitiveFiles = ['.env', 'server.js', 'package.json', 'vercel.json', 'security.js'];
    if (sensitiveFiles.some(file => req.path.endsWith(file))) {
        return res.status(403).send('Access Denied');
    }
    next();
});
app.use(express.static(path.join(__dirname), { extensions: ['html'], redirect: false }));

// Handle 404 for API routes (prevents HTML response for API errors)
app.use('/api', (req, res) => {
    res.status(404).json({ message: 'API endpoint not found' });
});

// Error handling middleware - Must be last
app.use((err, req, res, next) => {
    console.error(err.stack);

    // Handle other errors
    res.status(500).json({
        success: false,
        message: process.env.NODE_ENV === 'development' ? err.message : 'Something went wrong!'
    });
});

// Trust proxy for local development (handles HTTPS from reverse proxies)
app.set('trust proxy', 1);

// Add middleware to handle protocol inconsistencies
app.use((req, res, next) => {
    // Allow both HTTP and HTTPS for local development
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', '*');
    res.setHeader('Access-Control-Allow-Headers', '*');
    next();
});

// In Vercel serverless environment, we don't call app.listen().
// We export the app and Vercel handles the HTTP layer.
// Locally, we start the server normally.
if (process.env.VERCEL) {
    // Vercel serverless — just export
    module.exports = app;
} else {
    const server = app.listen(port, () => {
        console.log(`Server running on port ${port} (HTTP)`);
        console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
        console.log(`Server should stay running - press Ctrl+C to stop`);
    });

    // Keep the event loop alive with a heartbeat
    const heartbeat = setInterval(() => {}, 5000);

    server.on('close', () => {
        console.log('Server closed');
        clearInterval(heartbeat);
    });

    server.on('error', (err) => {
        console.error('Server error:', err);
    });

    module.exports = { app, pool };
}