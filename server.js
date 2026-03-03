require('dotenv').config({ path: require('path').join(__dirname, '.env') });
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

const faviconPath = path.join(__dirname, 'logo.png');

const app = express();
// Override port 5000 to 5001 to avoid macOS AirPlay conflict
const port = (process.env.PORT && process.env.PORT !== '5000') ? process.env.PORT : 5001;

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
        try {
            console.log('Using DATABASE_URL (masked):', connStr.replace(/:(.*)@/, ':*****@'));
        } catch (e) { console.log('Using DATABASE_URL (masked)'); }
        // Remove sslmode=require from connection string to ensure our ssl config takes precedence
        const connectionString = connStr.replace('sslmode=require', '');
        pool = new Pool({ connectionString, ssl });
    } else {
        throw new Error('No database configuration found in environment. Set DATABASE_URL or PGHOST/PGUSER/PGPASSWORD/PGDATABASE.');
    }
} catch (err) {
    console.error('Postgres pool init error:', err && err.message ? err.message : err);
    throw err;
}

// Audit Logging Helper
async function logActivity(req, action, details = {}) {
    try {
        const userId = req.user?.id || req.session?.user?.id || null;
        const ip = req.headers?.['x-forwarded-for'] || req.socket?.remoteAddress || '0.0.0.0';
        await pool.query(
            'INSERT INTO activity_logs (user_id, action, details, ip_address) VALUES ($1, $2, $3, $4)',
            [userId, action, JSON.stringify(details), ip]
        );
    } catch (err) {
        console.error('Audit Log Error:', err.message);
    }
}

// Initialize/Migrate Database Schema
async function initDb() {
    try {
        await pool.query(`
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
            INSERT INTO branches (id, name, location) VALUES (1, 'Main Warehouse', 'Headquarters') ON CONFLICT (id) DO NOTHING;
            INSERT INTO branches (id, name, location) VALUES (2, 'Accra Branch', 'Accra Central') ON CONFLICT (id) DO NOTHING;
            INSERT INTO branches (id, name, location) VALUES (3, 'Kumasi Branch', 'Kumasi') ON CONFLICT (id) DO NOTHING;

            -- FIX: Sync branches_id_seq with the actual max id to prevent duplicate key errors
            SELECT setval(pg_get_serial_sequence('branches', 'id'), COALESCE((SELECT MAX(id) FROM branches), 1));

            -- Create Products Table if not exists
            CREATE TABLE IF NOT EXISTS products (
                barcode VARCHAR(50) PRIMARY KEY,
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

            -- Add ID column to products for internal referencing (fixes barcode generation)
            ALTER TABLE products ADD COLUMN IF NOT EXISTS id SERIAL;

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
                payment_method VARCHAR(50),
                receipt_number VARCHAR(100),
                items JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS tax_breakdown JSONB;

           -- Add branch_id to other inventory tables for isolation
           ALTER TABLE categories ADD COLUMN IF NOT EXISTS branch_id INT;
           ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS branch_id INT;
           ALTER TABLE purchase_orders ADD COLUMN IF NOT EXISTS branch_id INT;

            -- Ensure transactions table has status column
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'completed';
            UPDATE transactions SET status = 'completed' WHERE status IS NULL;
            
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
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Add branch_id to system_settings if not exists
            ALTER TABLE system_settings ADD COLUMN IF NOT EXISTS branch_id INT DEFAULT 1;
            
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
                    FROM transactions WHERE payment_method = 'credit' AND customer_id IS NOT NULL;
                    
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

            -- Ensure stock_transfers has all columns
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS branch_id INT;
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS confirmed_by INT;
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMP;
            ALTER TABLE stock_transfers ADD COLUMN IF NOT EXISTS notes TEXT;

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
                status VARCHAR(20) DEFAULT 'Active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);

        // Create Default CEO User if not exists
        const ceoCheck = await pool.query("SELECT id FROM users WHERE email = 'ceo@footprint.com'");
        if (ceoCheck.rows.length === 0) {
            const defaultPass = process.env.DEFAULT_ADMIN_PASS || 'ceo123';
            const salt = await bcrypt.genSalt(10);
            const hash = await bcrypt.hash(defaultPass, salt);
            await pool.query(
                "INSERT INTO users (name, email, password, role, store_location, status) VALUES ($1, $2, $3, $4, $5, 'Active')",
                ['Chief Executive Officer', 'ceo@footprint.com', hash, 'ceo', 'Headquarters']
            );
            console.log('Default CEO user created: ceo@footprint.com');
        }

        console.log('Database schema checked/updated');
    } catch (err) {
        console.error('DB Init Error:', err);
    }
}
initDb();

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
        sameSite: 'lax',
        maxAge: 24 * 60 * 60 * 1000, // 24 hours
        domain: process.env.COOKIE_DOMAIN || 'localhost'
    },
    name: 'pos.sid' // Custom session cookie name
};

// Initialize session middleware
app.use(session(sessionConfig));

// CORS configuration
const corsOptions = {
    origin: process.env.CORS_ORIGIN || 'http://localhost:3000',
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

    // Fix: Handle string "null" or "undefined" sent by client when storage is empty
    if (token === 'null' || token === 'undefined') token = null;

    if (token) {
        jwt.verify(token, JWT_SECRET, (err, user) => {
            if (err) return res.status(403).json({ message: 'Invalid or expired token' });
            req.user = user;
            next();
        });
    } else if (req.session && req.session.user) {
        req.user = req.session.user; // Fallback to session cookie
        next();
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
        // Find user by email with branch/store information (Case Insensitive)
        const userResult = await pool.query(
            'SELECT id, name, email, password, role, store_id, store_location, status FROM users WHERE LOWER(email) = LOWER($1)', 
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

        // Set session
        req.session.user = {
            id: user.id,
            email: user.email,
            name: user.name,
            role: user.role,
            store_id: user.store_id,
            store_location: user.store_location || (user.role === 'admin' ? 'Accra Central' : null) // Ensure Admin has a default view context
        };

        // Log Login Activity
        await logActivity(req, 'LOGIN', { email: user.email, role: user.role });

        // Generate JWT Token
        const token = jwt.sign(req.session.user, JWT_SECRET, { expiresIn: '24h' });

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
        await logActivity(req, 'VIEW_USER_LIST');
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
        let locationFilter = "";
        let queryParams = [];
        
        if (storeLocation) {
            locationFilter = "AND store_location = $1";
            queryParams.push(storeLocation);
        }

        // Total Sales (Sum of transactions today)
        const salesRes = await pool.query(
            `SELECT COALESCE(SUM(total_amount), 0) as total FROM transactions WHERE status = 'completed' AND created_at >= CURRENT_DATE ${locationFilter}`,
            queryParams
        );
        
        // Total Transactions
        const txnsRes = await pool.query(
            `SELECT COUNT(*) as count FROM transactions WHERE status = 'completed' AND created_at >= CURRENT_DATE ${locationFilter}`,
            queryParams
        );

        // Low Stock
        let stockCount = 0;
        if (storeLocation) {
            // Check specific branch stock in JSONB
            const stockRes = await pool.query(
                `SELECT COUNT(*) as count FROM products WHERE (COALESCE(stock_levels->>$1, '0'))::int <= COALESCE(reorder_level, 10)`,
                [storeLocation]
            );
            stockCount = parseInt(stockRes.rows[0].count);
        } else {
            // Global stock check
            const stockRes = await pool.query(
                "SELECT COUNT(*) as count FROM products WHERE COALESCE(stock, 0) <= COALESCE(reorder_level, 10)"
            );
            stockCount = parseInt(stockRes.rows[0].count);
        }

        // Recent Transactions
        let recentWhere = "WHERE t.status = 'completed'";
        let recentParams = [];
        
        if (storeLocation) {
            recentWhere += " AND t.store_location = $1";
            recentParams.push(storeLocation);
        }

        const recentRes = await pool.query(
            `SELECT t.*, u.name as cashier_name 
             FROM transactions t 
             LEFT JOIN users u ON t.user_id = u.id 
             ${recentWhere}
             ORDER BY t.created_at DESC LIMIT 5`,
             recentParams
        );

        // Cashier Performance Stats
        let cashierWhere = storeLocation ? "AND t.store_location = $1" : "";
        const cashierRes = await pool.query(
            `SELECT u.name as cashier, COUNT(t.id) as transaction_count, SUM(t.total_amount) as total_sales
             FROM transactions t
             LEFT JOIN users u ON t.user_id = u.id
             WHERE t.status = 'completed' AND t.created_at >= CURRENT_DATE ${cashierWhere}
             GROUP BY u.name
             ORDER BY total_sales DESC LIMIT 5`,
             storeLocation ? [storeLocation] : []
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
                WHERE p.branch_id = $1 OR p.branch_id IS NULL
                ORDER BY p.created_at DESC`;
            params.push(req.user.store_id);
        } else {
            // For Admin/CEO, show global total_discounted
            query = `
                SELECT p.*, COALESCE(b.name, 'Global') as branch_name 
                FROM promotions p
                LEFT JOIN branches b ON p.branch_id = b.id
                ORDER BY p.created_at DESC`;
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
            'INSERT INTO promotions (code, discount_percentage, branch_id) VALUES ($1, $2, $3)',
            [code, discount, branchId]
        );
        await logActivity(req, 'CREATE_PROMOTION', { code, discount, branchId });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error creating promotion' });
    }
});

app.delete('/api/promotions/:code', authenticateToken, async (req, res) => {
    if (!req.user) return res.status(401).json({ message: 'Not authenticated' });
    try {
        let query = 'DELETE FROM promotions WHERE code = $1';
        let params = [req.params.code];

        // Ensure users can only delete promotions from their own branch (unless CEO/Admin)
        // Ensure users can only delete promotions from their own branch (Strict Ownership)
        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' AND (branch_id = $2 OR branch_id IS NULL)';
            query += ' AND branch_id = $2'; // Managers cannot delete Global promotions
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
        let query = 'SELECT * FROM promotions WHERE code = $1';
        let params = [req.params.code];

        // Validate branch applicability
        if (req.user && req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' AND (branch_id = $2 OR branch_id IS NULL)';
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
        
        // Get products and ensure total stock matches sum of location stocks
        const result = await pool.query(`
            SELECT 
                *,
                COALESCE(
                    (SELECT SUM(CAST(value AS INTEGER)) 
                     FROM jsonb_each_text(COALESCE(stock_levels, '{}'))),
                    0
                ) as calculated_total
            FROM products 
            ORDER BY name
        `);
        
        // Process rows - recalculate stock from stock_levels to keep in sync
        const rows = result.rows.map(p => {
            if (isRestricted && userBranch) {
                // Branch View: Show only stock for this branch
                const levels = p.stock_levels || {};
                const levelsObj = typeof levels === 'string' ? JSON.parse(levels) : levels;
                p.stock = parseInt(levelsObj[userBranch] || 0);
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

        const result = await pool.query(`
            SELECT 
                *,
                COALESCE(
                    (SELECT SUM(CAST(value AS INTEGER)) 
                     FROM jsonb_each_text(COALESCE(stock_levels, '{}'))),
                    0
                ) as calculated_total
            FROM products 
            ORDER BY name
        `);

        const rows = result.rows.map(p => {
            if (isRestricted && userBranch) {
                const levels = p.stock_levels || {};
                const levelsObj = typeof levels === 'string' ? JSON.parse(levels) : levels;
                p.stock = parseInt(levelsObj[userBranch] || 0);
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

        await logActivity(req, 'VIEW_INVENTORY_LIST');
        res.json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

app.post('/api/products', authenticateToken, async (req, res) => {
    const { barcode, name, category, price, stock, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry, batch_number, expiry_date } = req.body;
    try {
        const userBranch = req.user.store_location || 'Main Warehouse';
        const stockValue = parseInt(stock) || 0;
        const stockLevels = JSON.stringify({ [userBranch]: stockValue });
        
        await pool.query(
            'INSERT INTO products (barcode, name, category, price, stock, stock_levels, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)',
            [barcode, name, category, price, stockValue, stockLevels, cost_price || 0, selling_unit || 'Unit', packaging_unit || 'Box', conversion_rate || 1, reorder_level || 10, track_batch, track_expiry]
        );

        // Insert Batch if provided and stock > 0
        if (stockValue > 0 && (batch_number || expiry_date)) {
             const branchId = req.user.store_id || 1;
             // Ensure batch number exists if expiry is provided
             const finalBatchNum = batch_number || `BATCH-${Date.now()}`;
             
             await pool.query(
                `INSERT INTO product_batches (product_barcode, batch_number, expiry_date, quantity, quantity_available, quantity_received, branch_id, status)
                 VALUES ($1, $2, $3, $4, $4, $4, $5, 'Active')`,
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

        // Parse Excel File
        const workbook = xlsx.read(req.file.buffer, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0];
        const data = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
        
        const results = { success: 0, failed: 0, errors: [] };
        const userBranch = req.user.store_location || 'Main Warehouse';

        // storage for stock‑take items (barcode + physical count)
        const stockTakeItems = [];
        for (const row of data) {
            try {
                // Map Excel Columns to Database Fields
                const name = row['Product Name'] || row['name'];
                if (!name) continue; // Skip empty rows

                const category = row['Category'] || 'General';
                // ensure category exists in categories table
                if (category) {
                    const branchId = req.user.store_id || 1;
                    await pool.query(
                        `INSERT INTO categories (name, branch_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
                        [category, branchId]
                    );
                }

                const cost = parseFloat(row['Cost (₵)'] || row['Cost'] || 0);
                let price = parseFloat(row['Price (₵)'] || row['Price'] || 0);
                const markup = parseFloat(row['Markup'] || 0);
                
                // Auto-calculate price if missing but Cost & Markup exist
                if (price === 0 && cost > 0 && markup > 0) {
                    price = cost * (1 + (markup / 100));
                }

                const stock = parseInt(row['Current Stock'] || row['Stock'] || 0);
                const sellingUnit = row['Selling Unit'] || 'Unit';
                const packagingUnit = row['Packaging Unit'] || 'Box';
                const conversionRate = parseFloat(row['Items per Package'] || 1);
                const reorderLevel = parseInt(row['Reorder Level'] || 10);
                
                // Extract Batch Information
                const batchNumber = row['Batch Number'] || '';
                let expiryDate = row['Expiry Date'] || '';
                
                // Parse expiry date using flexible parser (handles any format/separator)
                if (expiryDate) {
                    const parsedDate = parseFlexibleDate(expiryDate);
                    expiryDate = parsedDate || '';
                }
                
                // Generate Barcode if not provided
                let barcode = row['Barcode'];
                if (!barcode) {
                    const prefix = name.substring(0, 3).toUpperCase().replace(/[^A-Z]/g, 'X');
                    barcode = `${prefix}-${Date.now().toString().slice(-6)}-${Math.floor(Math.random() * 1000)}`;
                }

                // Optional physical count for stock take
                const physicalCount = parseInt(row['Physical Count'] || row['Stock Take'] || row['Count'] || 0);
                if (!isNaN(physicalCount) && physicalCount > 0) {
                    stockTakeItems.push({ barcode, name, physicalCount });
                }

                const stockLevels = JSON.stringify({ [userBranch]: stock });

                await pool.query(
                    `INSERT INTO products (barcode, name, category, price, cost_price, stock, stock_levels, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, true, true)
                     ON CONFLICT (barcode) DO NOTHING`,
                    [barcode, name, category, price, cost, stock, stockLevels, sellingUnit, packagingUnit, conversionRate, reorderLevel]
                );
                
                // If batch number is provided, create batch record
                if (batchNumber) {
                    const branchId = req.user.store_id || 1;
                    await pool.query(
                        `INSERT INTO product_batches (product_barcode, batch_number, expiry_date, quantity, quantity_available, quantity_received, branch_id, status)
                         VALUES ($1, $2, $3, $4, $4, $4, $5, 'Active')
                         ON CONFLICT DO NOTHING`,
                        [barcode, batchNumber, expiryDate || null, stock, branchId]
                    );
                }
                
                // record barcode for stock take entry after product exists
                if (stockTakeItems.length && stockTakeItems[stockTakeItems.length-1].name === name && !stockTakeItems[stockTakeItems.length-1].barcode) {
                    stockTakeItems[stockTakeItems.length-1].barcode = barcode;
                }

                results.success++;
            } catch (e) {
                results.failed++;
                results.errors.push(`Error adding ${row['Product Name']}: ${e.message}`);
            }
        }

        // if physical counts were provided, create a stock take record
        if (stockTakeItems.length) {
            try {
                const branchId = req.user.store_id || 1;
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

app.put('/products/:barcode', authenticateToken, async (req, res) => {
    const { name, category, price, stock, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry, batch_number, expiry_date } = req.body;
    const { barcode } = req.params;
    try {
        const userBranch = req.user.store_location || 'Main Warehouse';
        const branchId = req.user.store_id || 1;
        const stockValue = parseInt(stock) || 0;
        
        // Get current product to preserve stock_levels and update it
        const currentRes = await pool.query('SELECT stock_levels, track_batch, track_expiry FROM products WHERE barcode = $1', [barcode]);
        if (currentRes.rows.length === 0) {
            return res.status(404).json({ message: 'Product not found' });
        }
        const current = currentRes.rows[0];
        let stockLevels = current.stock_levels || {};
        
        if (typeof stockLevels === 'string') {
            try {
                stockLevels = JSON.parse(stockLevels);
            } catch (e) {
                stockLevels = {};
            }
        }
        
        // Update the specific branch's stock level from the form
        stockLevels[userBranch] = stockValue;

        // Recalculate total stock from all branches
        const totalStock = Object.values(stockLevels).reduce((sum, val) => sum + (parseInt(val) || 0), 0);
        
        await pool.query(
            'UPDATE products SET name = $1, category = $2, price = $3, stock = $4, stock_levels = $5, cost_price = $6, selling_unit = $7, packaging_unit = $8, conversion_rate = $9, reorder_level = $10, track_batch = $11, track_expiry = $12 WHERE barcode = $13',
            [name, category, price, totalStock, JSON.stringify(stockLevels), cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch ?? current.track_batch, track_expiry ?? current.track_expiry, barcode]
        );

        // Handle Batch Update/Insert
        if (batch_number) {
            // Check if this batch already exists
            const batchCheck = await pool.query(
                'SELECT id FROM product_batches WHERE product_barcode = $1 AND batch_number = $2 AND branch_id = $3',
                [barcode, batch_number, branchId]
            );

            if (batchCheck.rows.length > 0) {
                // Update existing batch expiry
                if (expiry_date) {
                    await pool.query(
                        'UPDATE product_batches SET expiry_date = $1 WHERE id = $2',
                        [expiry_date, batchCheck.rows[0].id]
                    );
                }
            } else {
                // Insert new batch
                // Logic: If creating a new batch during edit, assume the current branch stock belongs to this batch 
                // (or at least the quantity provided in the form)
                await pool.query(
                    `INSERT INTO product_batches (product_barcode, batch_number, expiry_date, quantity, quantity_available, quantity_received, branch_id, status)
                     VALUES ($1, $2, $3, $4, $4, $4, $5, 'Active')`,
                    [barcode, batch_number, expiry_date || null, stockValue, branchId]
                );
            }
        }

        await logActivity(req, 'UPDATE_PRODUCT', { barcode, name, stock: totalStock });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error updating product' });
    }
});

app.delete('/products/:barcode', authenticateToken, async (req, res) => {
    try {
        const productRes = await pool.query('SELECT name FROM products WHERE barcode = $1', [req.params.barcode]);
        const productName = productRes.rows[0]?.name || 'Unknown';
        
        await pool.query('DELETE FROM products WHERE barcode = $1', [req.params.barcode]);
        await logActivity(req, 'DELETE_PRODUCT', { barcode: req.params.barcode, name: productName });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error deleting product' });
    }
});

// new API prefix delete route for front-end compatibility
app.delete('/api/products/:barcode', authenticateToken, async (req, res) => {
    try {
        const productRes = await pool.query('SELECT name FROM products WHERE barcode = $1', [req.params.barcode]);
        const productName = productRes.rows[0]?.name || 'Unknown';
        
        await pool.query('DELETE FROM products WHERE barcode = $1', [req.params.barcode]);
        await logActivity(req, 'DELETE_PRODUCT', { barcode: req.params.barcode, name: productName });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error deleting product' });
    }
});

// Update product by ID (Used for barcode generation)
app.put('/api/products/:id', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const { barcode, name, category, price, stock, stock_levels, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry } = req.body;
    
    try {
        const levels = typeof stock_levels === 'object' ? JSON.stringify(stock_levels) : stock_levels;
        await pool.query(
            'UPDATE products SET barcode = $1, name = $2, category = $3, price = $4, stock = $5, stock_levels = $6, cost_price = $7, selling_unit = $8, packaging_unit = $9, conversion_rate = $10, reorder_level = $11, track_batch = $12, track_expiry = $13 WHERE id = $14',
            [barcode, name, category, price, stock, levels, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry, id]
        );
        await logActivity(req, 'UPDATE_PRODUCT_BARCODE', { id, barcode, name });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error updating product' });
    }
});

// update product by barcode for front‑end
app.put('/api/products/:barcode', authenticateToken, async (req, res) => {
    const { barcode } = req.params;
    const { name, category, price, stock, stock_levels, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry } = req.body;
    try {
        const levels = typeof stock_levels === 'object' ? JSON.stringify(stock_levels) : stock_levels;
        await pool.query(
            'UPDATE products SET name = $1, category = $2, price = $3, stock = $4, stock_levels = $5, cost_price = $6, selling_unit = $7, packaging_unit = $8, conversion_rate = $9, reorder_level = $10, track_batch = $11, track_expiry = $12 WHERE barcode = $13',
            [name, category, price, stock, levels, cost_price, selling_unit, packaging_unit, conversion_rate, reorder_level, track_batch, track_expiry, barcode]
        );
        await logActivity(req, 'UPDATE_PRODUCT', { barcode, name });
        res.json({ success: true });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error updating product' });
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
        let query = 'SELECT * FROM categories';
        let params = [];

        // Filter by branch for non-CEO/Admin users
        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' WHERE branch_id = $1 OR branch_id IS NULL';
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
        await pool.query('INSERT INTO categories (name, description, branch_id) VALUES ($1, $2, $3)', [name, description, branchId]);
        await logActivity(req, 'CREATE_CATEGORY', { name, branchId });
        res.json({ success: true });
    } catch (err) { console.error(err); res.status(500).json({ message: 'Error creating category' }); }
});

app.delete('/api/categories/:id', authenticateToken, async (req, res) => {
    try {
        let query = 'DELETE FROM categories WHERE id = $1';
        let params = [req.params.id];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' AND (branch_id = $2 OR branch_id IS NULL)';
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
        let query = `
            SELECT s.*, b.name as branch_name, b.location as branch_location 
            FROM suppliers s
            LEFT JOIN branches b ON s.branch_id = b.id
        `;
        let params = [];

        if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id) {
            query += ' WHERE s.branch_id = $1 OR s.branch_id IS NULL';
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
            'INSERT INTO suppliers (name, contact_person, phone, email, address, branch_id) VALUES ($1, $2, $3, $4, $5, $6)',
            [name, contact, phone, email, address, branchId]
        );
        await logActivity(req, 'CREATE_SUPPLIER', { name, branchId });
        res.json({ success: true });
    } catch (err) { res.status(500).json({ message: 'Error adding supplier' }); }
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
        const dateStr = today.toISOString().slice(2,10).replace(/-/g, ''); // YYMMDD
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
            const expStr = finalExpiry.toISOString().slice(2,10).replace(/-/g, '');
            if (!finalBatchNum) finalBatchNum = `${supplierCode}-${dateStr}-${itemCode}-${expStr}`;

            // Check for existing batch with same number but different expiry
            const existingBatchRes = await client.query(
                'SELECT expiry_date FROM product_batches WHERE product_barcode = $1 AND batch_number = $2 AND branch_id = $3',
                [item.product_barcode, finalBatchNum, branchId]
            );

            if (existingBatchRes.rows.length > 0) {
                const existingExpiry = new Date(existingBatchRes.rows[0].expiry_date);
                // Compare dates (ignoring time)
                if (existingExpiry.toISOString().slice(0,10) !== finalExpiry.toISOString().slice(0,10)) {
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
            query += ' WHERE from_location = $1 OR to_location = $1';
            params.push(req.user.store_location);
        }

        query += ' ORDER BY created_at DESC';
        const result = await pool.query(query, params);
        res.json(result.rows);
    } catch (err) { res.status(500).json({ message: 'Server error' }); }
});

app.post('/api/transfers', authenticateToken, async (req, res) => {
    const { from, to, items } = req.body;
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        
        console.log(`\n📦 TRANSFER REQUEST: ${from} → ${to}`);
        console.log(`Items:`, JSON.stringify(items, null, 2));
        
        // Deduct Stock FIRST (validate and update)
        for (const item of items) {
            // Get current product stock data
            const checkRes = await client.query(`
                SELECT stock, stock_levels, barcode FROM products WHERE barcode = $1
            `, [item.barcode]);
            
            if (checkRes.rows.length === 0) {
                throw new Error(`Product ${item.barcode} not found`);
            }
            
            const product = checkRes.rows[0];
            console.log(`\n📍 Product: ${item.barcode}`);
            console.log(`   Total stock in DB: ${product.stock}`);
            console.log(`   stock_levels in DB:`, product.stock_levels);
            
            // Determine location stock - initialize stock_levels if needed
            let stockLevels = product.stock_levels || {};
            if (typeof stockLevels === 'string') {
                try {
                    stockLevels = JSON.parse(stockLevels);
                } catch (e) {
                    stockLevels = {};
                }
            }
            
            // If location stock doesn't exist, use total stock as fallback for Main Warehouse
            if (!(from in stockLevels)) {
                stockLevels[from] = product.stock || 0;
            }
            
            const locStock = stockLevels[from] || 0;
            console.log(`   Available at ${from}: ${locStock}`);
            console.log(`   Requesting: ${item.qty}`);
            
            if (locStock < item.qty) {
                throw new Error(`Insufficient stock at ${from} for product ${item.barcode}. Available: ${locStock}, Requested: ${item.qty}`);
            }

            // Deduct from Total Stock
            await client.query('UPDATE products SET stock = stock - $1 WHERE barcode = $2', [item.qty, item.barcode]);
            
            // Deduct from Source Location
            await client.query(`
                UPDATE products 
                SET stock_levels = jsonb_set(
                    COALESCE(stock_levels, '{}'::jsonb), 
                    ARRAY[$1::text], 
                    to_jsonb(COALESCE((stock_levels->>$1::text)::int, 0) - $2)
                )
                WHERE barcode = $3
            `, [from, item.qty, item.barcode]);
            
            console.log(`   ✅ Deducted ${item.qty} from ${from}`);
        }

        // Insert Transfer AFTER validating and deducting stock
        await client.query(
            'INSERT INTO stock_transfers (from_location, to_location, items, status) VALUES ($1, $2, $3, $4)',
            [from, to, JSON.stringify(items), 'In Transit']
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
        
        const items = typeof transfer.items === 'string' ? JSON.parse(transfer.items) : transfer.items;
        
        // Add Stock to Destination Location ONLY (NOT total stock - it was already deducted when transfer was created)
        for (const item of items) {
            // Update destination location stock only
            await client.query(`
                UPDATE products 
                SET stock_levels = jsonb_set(
                    COALESCE(stock_levels, '{}'::jsonb), 
                    ARRAY[$1::text], 
                    to_jsonb(COALESCE((stock_levels->>$1::text)::int, 0) + $2)
                )
                WHERE barcode = $3
            `, [transfer.to_location, item.qty, item.barcode]);
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
            SELECT b.*, p.name 
            FROM product_batches b 
            JOIN products p ON b.product_barcode = p.barcode 
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
            const resProd = await client.query('SELECT stock, stock_levels FROM products WHERE barcode = $1', [adj.barcode]);
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

            items.forEach(item => {
                const qty = parseInt(item.qty);
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

            items.forEach(item => {
                if (item.barcode === barcode) {
                    const qty = parseInt(item.qty);
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
        const result = await pool.query(
            "SELECT * FROM transactions WHERE user_id = $1 AND status = 'completed' ORDER BY created_at DESC",
            [req.user.id]
        );
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
    
    const { id } = req.params;
    
    try {
        const result = await pool.query(`
            SELECT t.*, u.name AS cashier_name, u.store_id, c.name AS customer_name, c.current_balance, c.credit_limit
            FROM transactions t
            LEFT JOIN users u ON t.user_id = u.id
            LEFT JOIN customers c ON t.customer_id = c.id
            WHERE t.id = $1
        `, [id]);
        
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
                const txnRes = await client.query('SELECT total_amount, receipt_number FROM transactions WHERE id = $1', [id]);
                const total = parseFloat(txnRes.rows[0].total_amount);
                const receiptNum = receiptNumber || txnRes.rows[0].receipt_number || ('RCP' + Date.now());
                
                // Get customer details
                const custDetailRes = await client.query('SELECT name, current_balance, credit_limit FROM customers WHERE id = $1', [parsedCustomerId]);
                if (custDetailRes.rows.length === 0) throw new Error('Customer not found');
                
                customerName = custDetailRes.rows[0].name;
                
                const custRes = await client.query(
                    'UPDATE customers SET current_balance = current_balance + $1 WHERE id = $2 RETURNING current_balance',
                    [total, parsedCustomerId]
                );
                const newBalance = custRes.rows[0].current_balance;

                // Add to Ledger (Part 2)
                await client.query(
                    `INSERT INTO customer_ledger (customer_id, date, description, type, debit, balance, transaction_id) 
                     VALUES ($1, NOW(), $2, 'SALE', $3, $4, $5)`,
                    [parsedCustomerId, `Credit Sale - ${receiptNum}`, total, newBalance, id]
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

        const token = jwt.sign(req.session.user, JWT_SECRET, { expiresIn: '24h' });
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
        let txnWhere = "status = 'completed'";
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
                SUM((item->>'qty')::int) as units_sold,
                SUM((item->>'qty')::int * (item->>'price')::numeric) as revenue
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
                SUM((item->>'qty')::int) as units_sold,
                SUM((item->>'qty')::int * (item->>'price')::numeric) as revenue
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
        let whereClause = "status = 'completed'";
        let params = [];

        if (startDate && endDate) {
            whereClause += " AND created_at >= $1 AND created_at <= $2::date + INTERVAL '1 day' - INTERVAL '1 second'";
            params.push(startDate, endDate);
        }

        const result = await pool.query(`
            SELECT 
                COALESCE(store_location, 'Main Branch') as branch,
                COUNT(*) as txn_count,
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
            WHERE status = 'completed' AND created_at >= NOW() - INTERVAL '7 days'
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
            SELECT u.name, COUNT(t.id) as transactions, SUM(t.total_amount) as sales
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
            SELECT item->>'name' as name, SUM((item->>'qty')::int) as qty, SUM((item->>'qty')::int * (item->>'price')::numeric) as revenue
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
            SELECT COALESCE(store_location, 'Main Branch') as branch, COUNT(*) as txn_count, SUM(total_amount) as revenue
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
    
    const { items, total, paymentMethod, promoCode, discount, customerId, taxBreakdown, status } = req.body;
    
    try {
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            // Convert customerId to integer if provided
            const parsedCustomerId = customerId ? parseInt(customerId) : null;
            console.log('📝 Creating transaction with customerId:', parsedCustomerId, '(type:', typeof parsedCustomerId, ')');
            
            // Fetch customer name if ID is provided (Ensures data consistency)
            let customerName = null;
            if (parsedCustomerId) {
                const cRes = await client.query('SELECT name FROM customers WHERE id = $1', [parsedCustomerId]);
                if (cRes.rows.length > 0) customerName = cRes.rows[0].name;
            }

            // 1. Check Stock Levels BEFORE creating transaction
            for (const item of items) {
                const res = await client.query('SELECT COALESCE(stock, 0) as stock, name FROM products WHERE barcode = $1', [item.barcode]);
                if (res.rows.length === 0) throw new Error(`Product ${item.barcode} not found`);
                const product = res.rows[0];
                if (product.stock < item.qty) {
                    throw new Error(`Insufficient stock for ${product.name}. Available: ${product.stock}`);
                }
            }
            
            // Create transaction
            const receiptNumber = 'RCP' + Date.now();
            const txnRes = await client.query(
                `INSERT INTO transactions
                (user_id, store_location, total_amount, payment_method, receipt_number, items, created_at, customer_id, customer_name, status, tax_breakdown)
                VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $8, $9, $10) RETURNING id`,
                [req.user.id, req.user.store_location, total, paymentMethod, receiptNumber, JSON.stringify(items), parsedCustomerId, customerName, status || 'pending', JSON.stringify(taxBreakdown || [])]
            );
            
            // Update Promotion Usage
            if (promoCode && discount > 0) {
                // Update global total
                await client.query(
                    'UPDATE promotions SET total_discounted = COALESCE(total_discounted, 0) + $1 WHERE code = $2',
                    [discount, promoCode]
                );
                
                // Update branch specific usage
                const branchId = req.user.store_id || 1;
                await client.query(`
                    INSERT INTO promotion_usage (promotion_code, branch_id, total_discounted)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (promotion_code, branch_id)
                    DO UPDATE SET total_discounted = promotion_usage.total_discounted + $3
                `, [promoCode, branchId, discount]);
            }
            
            // Update stock and batches
            for (const item of items) {
                // Deduct from total stock and specific location
                const storeLoc = req.user.store_location || 'Main Warehouse';
                
                await client.query(`
                    UPDATE products 
                    SET stock = COALESCE(stock, 0) - $1,
                        stock_levels = jsonb_set(
                            COALESCE(stock_levels, '{}'::jsonb), 
                            ARRAY[$3::text], 
                            to_jsonb(COALESCE((stock_levels->>$3::text)::int, 0) - $1)
                        )
                    WHERE barcode = $2
                `, [item.qty, item.barcode, storeLoc]);

                // FIFO/FEFO Batch Deduction
                // Get batches ordered by expiry (FEFO)
                const batches = await client.query(
                    'SELECT * FROM product_batches WHERE product_barcode = $1 AND quantity > 0 ORDER BY expiry_date ASC',
                    [item.barcode]
                );

                let remainingQty = item.qty;
                for (const batch of batches.rows) {
                    if (remainingQty <= 0) break;
                    const deduct = Math.min(remainingQty, batch.quantity);
                    await client.query(
                        'UPDATE product_batches SET quantity = quantity - $1 WHERE id = $2', // Assuming 'id' is PK
                        [deduct, batch.id]
                    );
                    remainingQty -= deduct;
                }

                // Log audit for each item sold
                const branchId = req.user.store_id || 1;
                await client.query(`
                    INSERT INTO inventory_audit_log (action_type, product_barcode, quantity_before, quantity_after, reference_id, reference_type, user_id, branch_id)
                    SELECT 'Sale', $1, stock + $2, stock, $3, 'Transaction', $4, $5 FROM products WHERE barcode = $6
                `, [item.barcode, item.qty, txnRes.rows[0].id, req.user.id, branchId, item.barcode]);
            }
            
            await client.query('COMMIT');
            await logActivity(req, 'POS_SALE', { total, receiptNumber, itemCount: items.length });
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

// ============ ENHANCED INVENTORY MANAGEMENT ENDPOINTS ============

// 1. GET product with full details
app.get('/api/products/full', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT p.*,
                (SELECT COUNT(*) FROM product_batches pb WHERE pb.product_barcode = p.barcode AND pb.status = 'Active') as batch_count,
                (SELECT MIN(expiry_date) FROM product_batches pb WHERE pb.product_barcode = p.barcode AND pb.status = 'Active') as next_expiry
            FROM products p
            ORDER BY p.name
        `);
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error fetching products' });
    }
});

// 2. Create pricelist
app.post('/api/pricelists', authenticateToken, async (req, res) => {
    const { name, list_type, effective_date, branch_id } = req.body;
    try {
        const result = await pool.query(
            'INSERT INTO price_lists (name, list_type, effective_date, branch_id, status) VALUES ($1, $2, $3, $4, $5) RETURNING id',
            [name, list_type, effective_date, branch_id, 'Active']
        );
        await logActivity(req, 'CREATE_PRICELIST', { name, branch_id });
        res.json({ success: true, id: result.rows[0].id });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error creating pricelist' });
    }
});

// 3. Get all pricelists
app.get('/api/pricelists', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM price_lists ORDER BY created_at DESC');
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ message: 'Error fetching pricelists' });
    }
});

// 4. Add item to pricelist with auto-markup
app.post('/api/pricelists/:id/items', authenticateToken, async (req, res) => {
    const { id } = req.params;
    const { product_barcode, markup_percentage } = req.body;
    try {
        const productRes = await pool.query('SELECT cost_price FROM products WHERE barcode = $1', [product_barcode]);
        if (productRes.rows.length === 0) {
            return res.status(404).json({ message: 'Product not found' });
        }
        const costPrice = parseFloat(productRes.rows[0].cost_price) || 0;
        const sellingPrice = costPrice * (1 + (markup_percentage / 100));
        await pool.query(
            'INSERT INTO price_list_items (price_list_id, product_barcode, markup_percentage, selling_price) VALUES ($1, $2, $3, $4)',
            [id, product_barcode, markup_percentage, sellingPrice]
        );
        await logActivity(req, 'ADD_PRICELIST_ITEM', { pricelistId: id, product_barcode, sellingPrice });
        res.json({ success: true, sellingPrice });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error adding pricelist item' });
    }
});

// 5. Receive goods with batch tracking
app.post('/api/goods-received', authenticateToken, async (req, res) => {
    const { po_id, items, received_by, branch_id } = req.body;
    try {
        for (const item of items) {
            const { product_barcode, quantity_received, quantity_packaging_units, unit_cost, batch_number, expiry_date, invoice_number } = item;
            
            await pool.query(`
                INSERT INTO goods_received (po_id, product_barcode, quantity_received, quantity_packaging_units, unit_cost, batch_number, expiry_date, received_by, invoice_number, branch_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            `, [po_id, product_barcode, quantity_received, quantity_packaging_units, unit_cost, batch_number, expiry_date, received_by, invoice_number, branch_id]);
            
            const trackBatch = await pool.query('SELECT track_batch FROM products WHERE barcode = $1', [product_barcode]);
            if (trackBatch.rows[0]?.track_batch) {
                await pool.query(`
                    INSERT INTO product_batches (product_barcode, batch_number, expiry_date, quantity_received, quantity_available, unit_cost, branch_id, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'Active')
                    ON CONFLICT (product_barcode, batch_number, branch_id) DO UPDATE SET
                        quantity_received = quantity_received + $4,
                        quantity_available = quantity_available + $4
                `, [product_barcode, batch_number, expiry_date, quantity_received, quantity_received, unit_cost, branch_id]);
            }
            
            const product = await pool.query('SELECT conversion_rate FROM products WHERE barcode = $1', [product_barcode]);
            const sellableUnits = quantity_packaging_units * parseFloat(product.rows[0].conversion_rate);
            
            await pool.query('UPDATE products SET stock = COALESCE(stock, 0) + $1 WHERE barcode = $2', [sellableUnits, product_barcode]);
            
            await pool.query(`
                INSERT INTO inventory_audit_log (action_type, product_barcode, quantity_after, reference_id, reference_type, user_id, branch_id)
                SELECT 'Stock In', $1::varchar, stock - $5, stock, $2, 'Purchase Order', $3, $4 FROM products WHERE barcode = $1
            `, [product_barcode, po_id, received_by, branch_id, sellableUnits]);
        }
        
        await pool.query('UPDATE purchase_orders SET status = $1 WHERE id = $2', ['Received', po_id]);
        res.json({ success: true, message: 'Goods received successfully' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error receiving goods', error: err.message });
    }
});

// 6. Move items to shelf
app.post('/api/shelf/move', authenticateToken, async (req, res) => {
    const { product_barcode, quantity, staff_id, from_location, to_location, branch_id, notes, batch_number, expiry_date } = req.body;
    try {
        const product = await pool.query('SELECT stock FROM products WHERE barcode = $1', [product_barcode]);
        if (product.rows[0].stock < quantity) {
            return res.status(400).json({ message: 'Insufficient stock' });
        }
        
        // If batch info is provided, ensure it's recorded (User Requirement: Record Expiry when assigned to shelf)
        if (batch_number && expiry_date) {
            await pool.query(`
                INSERT INTO product_batches (product_barcode, batch_number, expiry_date, quantity_received, quantity_available, branch_id, status)
                VALUES ($1, $2, $3, 0, 0, $4, 'Active')
                ON CONFLICT (product_barcode, batch_number, branch_id) 
                DO UPDATE SET expiry_date = $3
            `, [product_barcode, batch_number, expiry_date, branch_id]);
        }

        const movementNotes = notes ? `${notes} [Batch: ${batch_number || 'N/A'}, Exp: ${expiry_date || 'N/A'}]` : `Batch: ${batch_number || 'N/A'}, Exp: ${expiry_date || 'N/A'}`;

        await pool.query(`
            INSERT INTO shelf_inventory (product_barcode, quantity_on_shelf, store_quantity, staff_id, branch_id, notes, last_verified)
            VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
            ON CONFLICT (product_barcode) DO UPDATE SET
                quantity_on_shelf = shelf_inventory.quantity_on_shelf + $2
        `, [product_barcode, quantity, 0, staff_id, branch_id, movementNotes]);
        
        await pool.query(`
            INSERT INTO shelf_movements (product_barcode, movement_type, quantity, staff_id, from_location, to_location, branch_id, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `, [product_barcode, 'Store to Shelf', quantity, staff_id, from_location, to_location, branch_id, movementNotes]);
        
        await logActivity(req, 'MOVE_TO_SHELF', { product_barcode, quantity, to_location });
        res.json({ success: true, message: 'Item moved to shelf' });
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Error moving item to shelf' });
    }
});

// 7. Get shelf inventory
app.get('/api/shelf/inventory/:branch_id', authenticateToken, async (req, res) => {
    const { branch_id } = req.params;
    
    // Security check: Ensure user can only view their own branch (unless Admin/CEO)
    if (req.user.role !== 'admin' && req.user.role !== 'ceo' && req.user.store_id != branch_id) {
        return res.status(403).json({ message: 'Unauthorized access to other branch inventory' });
    }

    try {
        const result = await pool.query(`
            SELECT pb.batch_number, pb.expiry_date, 
                   pb.quantity_available as quantity,
                   p.name, p.selling_unit,
                   si.quantity_on_shelf,
                   si.last_verified
            FROM product_batches pb
            LEFT JOIN shelf_inventory si ON pb.product_barcode = si.product_barcode 
                AND si.branch_id = pb.branch_id
            JOIN products p ON pb.product_barcode = p.barcode
            WHERE (pb.branch_id = $1 OR pb.branch_id = 1) AND pb.quantity_available > 0
            ORDER BY pb.expiry_date ASC
        `, [branch_id]);
        res.json(result.rows);
    } catch (err) {
        console.error('Shelf inventory error:', err);
        res.status(500).json({ message: 'Error fetching shelf inventory' });
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

// Get all batches for a user
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
            FROM products
        `);
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
        // Fetch fresh store_id from DB to ensure accuracy even if token is stale
        const userRes = await pool.query('SELECT store_id FROM users WHERE id = $1', [req.user.id]);
        const dbStoreId = userRes.rows[0]?.store_id;
        
        const branchId = dbStoreId || req.user.store_id || 1;
        let result = await pool.query('SELECT * FROM system_settings WHERE branch_id = $1', [branchId]);
        
        if (result.rows.length === 0) {
            // Fallback to default (branch 1) if specific branch settings don't exist
            result = await pool.query('SELECT * FROM system_settings WHERE branch_id = 1');
        }
        
        res.json(result.rows[0] || {});
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

app.post('/api/settings', authenticateToken, async (req, res) => {
    // Allow admin and ceo roles only (removed manager)
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') {
        return res.status(403).json({ message: 'Unauthorized. Only CEO or Admin can change system settings.' });
    }
    
    const { storeName, currencySymbol, vatRate, receiptFooter } = req.body;
    const branchId = req.user.store_id || 1;
    
    try {
        // Use branchId as id for simplicity (assuming 1-to-1 mapping) or let DB handle it if we used SERIAL
        await pool.query(`
            INSERT INTO system_settings (id, branch_id, store_name, currency_symbol, vat_rate, receipt_footer, updated_at)
            VALUES ($1, $1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO UPDATE SET 
                store_name = $2, 
                currency_symbol = $3, 
                vat_rate = $4, 
                receipt_footer = $5, 
                updated_at = CURRENT_TIMESTAMP
        `, [branchId, storeName, currencySymbol, vatRate, receiptFooter]);
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
    const branchId = req.user.store_id || 1;
    
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
            SELECT a.*, u.name as user_name, u.role as user_role 
            FROM activity_logs a 
            LEFT JOIN users u ON a.user_id = u.id 
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

// Get active tax rules (Public for POS)
app.get('/api/taxes/active', authenticateToken, async (req, res) => {
    try {
        const branchId = req.user.store_id || 1;
        
        // 1. Fetch Additional Taxes
        const rulesRes = await pool.query("SELECT * FROM tax_rules WHERE status = 'Active' ORDER BY created_at DESC");
        const rules = rulesRes.rows;

        // 2. Fetch System VAT
        let settingsRes = await pool.query("SELECT vat_rate FROM system_settings WHERE branch_id = $1", [branchId]);
        if (settingsRes.rows.length === 0) {
            settingsRes = await pool.query("SELECT vat_rate FROM system_settings WHERE branch_id = 1");
        }
        const vatRate = settingsRes.rows.length > 0 ? parseFloat(settingsRes.rows[0].vat_rate) : 0;

        // 3. Combine (VAT first)
        if (vatRate > 0) {
            rules.unshift({ id: 'vat', name: 'VAT', rate: vatRate, status: 'Active' });
        }

        res.json(rules);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Get all tax rules
app.get('/api/taxes', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    try {
        const result = await pool.query('SELECT * FROM tax_rules ORDER BY created_at DESC');
        res.json(result.rows);
    } catch (err) {
        console.error(err);
        res.status(500).json({ message: 'Server error' });
    }
});

// Add tax rule
app.post('/api/taxes', authenticateToken, async (req, res) => {
    if (req.user.role !== 'admin' && req.user.role !== 'ceo') return res.status(403).json({ message: 'Unauthorized' });
    const { name, rate } = req.body;
    try {
        await pool.query('INSERT INTO tax_rules (name, rate) VALUES ($1, $2)', [name, rate]);
        await logActivity(req, 'CREATE_TAX_RULE', { name, rate });
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
        // 1. Get Active Tax Rules (for Columns)
        const rulesRes = await pool.query("SELECT * FROM tax_rules WHERE status = 'Active' ORDER BY created_at DESC");
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

// Start server
const server = app.listen(port, () => {
    console.log(`Server running on port ${port} (HTTP)`);
    console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (err) => {
    console.error('Unhandled Rejection:', err);
    // Close server and exit process
    server.close(() => process.exit(1));
});

// Handle uncaught exceptions
process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err);
    // Close server and exit process
    server.close(() => process.exit(1));
});

module.exports = app; // For testing 