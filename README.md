# Footprint POS System

A comprehensive Point of Sale (POS) and Inventory Management system built with Node.js, Express, and PostgreSQL.

## Features

- **Point of Sale**: Process sales, handle receipts, and manage transactions.
- **Inventory Management**: Track stock, manage suppliers, purchase orders, and goods receiving.
- **Dashboard**: Real-time analytics, sales charts, and low stock alerts.
- **User Management**: Role-based access control (Admin, Manager, Cashier, CEO).
- **Reporting**: Generate PDF statements and export data.

## Prerequisites

- Node.js (v14 or higher)
- PostgreSQL Database

## Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   npm install
   ```

## Configuration

Create a `.env` file in the root directory with the following variables:

```env
# Database (Example for local development)
DATABASE_URL=postgresql://user:password@host:port/database

# Security
JWT_SECRET=your_secure_jwt_secret
SESSION_SECRET=your_secure_session_secret

# Default Passwords (for seeding)
DEFAULT_ADMIN_PASS=secure_admin_password
DEFAULT_CASHIER_PASS=secure_cashier_password
DEFAULT_CEO_PASS=secure_ceo_password

# Email (for password reset & statements)
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_SECURE=false
SMTP_USER=your_email@example.com
SMTP_PASS=your_email_password
ADMIN_EMAIL=admin@footprint.com
```

## Deployment

This project is configured for deployment on **Vercel**.
1. Push to GitHub.
2. Import project in Vercel.
3. Add the Environment Variables in Vercel Project Settings.

For cloud providers like Supabase or Neon, use the pooled connection string they provide for the `DATABASE_URL` variable. It will look something like this:
`DATABASE_URL=postgres://user:password@host.pooler.supabase.com:6543/postgres?sslmode=require`

## Database Seeding

To populate the database with default users (Admin, CEO, Cashier), run:
```bash
node seed.js
```# FootprintRetailSystem
