# Credit Statement Feature - Complete Implementation

## Overview
The credit statement feature generates professional, password-protected PDF statements for credit customers with detailed transaction history, item breakdowns, and credit information.

## Features Implemented ✅

### 1. **Statement Generation**
- Generates month-specific statements for any credit customer
- PDF format with password protection (unique 8-digit code per statement)
- Professional formatting with company branding (Footprint Retail Systems)
- Responsive to page breaks (auto-adds new pages as needed)

### 2. **Transaction Details**
- **Opening Balance**: Calculated from all previous months' transactions and payments
- **Transaction Rows**: 
  - Date, Description, Debit (Sales), Credit (Payments), Running Balance
  - Alternating row colors for readability (white/light gray)
  - Color-coded running balance:
    - 🟡 **Yellow** (#d4a500): Amount owed (positive balance)
    - 🟢 **Green** (#2e7d32): Zero or credit balance (paid in full)

### 3. **Item-Level Details**
- Each transaction displays associated items:
  - Product name
  - Quantity purchased
  - Unit price
  - Item total
- Formatted with tree symbol (└─) for visual hierarchy
- Support for transactions with 1-N items
- Handles transactions with >3 items gracefully ("...and X more items")

### 4. **Credit Summary Box**
- Credit Limit (GHS)
- Amount Used (Current Balance in red)
- Available Credit (calculated as limit - used, in green)
- Unique access code (PDF password) for security

### 5. **Email Delivery**
- Sends statement as password-protected PDF attachment
- Professional HTML email template with company branding
- Includes credit information summary in email body
- Shows unique PDF access code in email for reference

### 6. **Data Validation & Error Handling**
- Month validation (1-12)
- Year validation (2020-current year + 1)
- Customer existence check
- Email address validation
- SMTP configuration error detection
- Specific error codes for debugging:
  - `SMTP_CONFIG_ERROR`: SMTP server configuration issue
  - `SMTP_AUTH_ERROR`: Authentication failed
  - `CUSTOMER_NOT_FOUND`: Customer not found
  - `NO_CUSTOMER_EMAIL`: No email on file
  - `STATEMENT_ERROR`: General error

## API Endpoint

### POST `/api/customers/:id/email-statement`

**Authentication Required**: Yes (JWT Token)

**Request Parameters**:
```json
{
  "month": 1,          // 1-12 (optional, defaults to current month)
  "year": 2026,        // YYYY format (optional, defaults to current year)
  "id": "14"          // Customer ID in URL path
}
```

**Success Response** (200):
```json
{
  "success": true,
  "message": "Statement emailed successfully to kelvin@example.com",
  "details": {
    "recipient": "kelvin@example.com",
    "month": "January",
    "year": 2026,
    "messageId": "..."
  }
}
```

**Error Response** (400/500):
```json
{
  "success": false,
  "message": "Specific error message",
  "code": "ERROR_CODE",
  "details": "Development-only error details"
}
```

## Database Dependencies

### Required Tables
1. **customers**
   - `id` (INT) - Primary key
   - `name` (VARCHAR) - Customer name
   - `email` (VARCHAR) - Email address
   - `credit_limit` (NUMERIC) - Credit limit amount
   - `current_balance` (NUMERIC) - Current outstanding balance

2. **transactions**
   - `id` (INT) - Primary key
   - `customer_name` (VARCHAR) - Credit customer name (NULL for cash)
   - `created_at` (TIMESTAMP) - Transaction date/time
   - `receipt_number` (VARCHAR) - Receipt identifier
   - `total_amount` (NUMERIC) - Transaction total
   - `payment_method` (VARCHAR) - 'credit', 'cash', 'momo', 'card'
   - `items` (JSONB) - Array of purchased items with quantity/price
   - `status` (VARCHAR) - 'completed', 'pending'

3. **customer_payments**
   - `id` (INT) - Primary key
   - `customer_id` (INT) - Link to customers table
   - `amount` (NUMERIC) - Payment amount
   - `payment_date` (TIMESTAMP) - When payment was made

## Console Logging

The endpoint provides detailed logging with emoji prefixes for easy debugging:

```
📧 Starting email-statement request for customer ID: 14
📅 Statement Request - Customer ID: 14 | Month: 1 | Year: 2026
📊 STATEMENT GENERATION DEBUG
👤 Customer: Kelvin Van-Dyck | ID: 14
🔍 Opening Balance Data: { total_sales: 5000, total_paid: 1500 }
🔍 Opening Balance Calculated: 3500
📋 ALL CREDIT TRANSACTIONS FOR CUSTOMER: 3
💾 [1] Transaction ID: 45
    Date: 2026-01-15T10:30:00.000Z
    Receipt: CREDIT-1769455341273
    ...
📧 TRANSACTIONS FOR STATEMENT (Month: January 2026): 2 found
📨 Preparing email for: kelvin@example.com
📎 PDF Password: 12345678
📄 PDF Size: 45.32 KB
📤 Sending email...
✅ Email sent successfully!
📧 Message ID: <...@gmail.com>
```

## How It Works - Transaction Flow

### 1. **Statement Request**
```
User clicks "Generate Statement" in credit-customers.html
↓
Selects Month & Year
↓
POST /api/customers/:id/email-statement
```

### 2. **Data Gathering**
```
Validate month/year
↓
Fetch customer record (get name, email, credit_limit, current_balance)
↓
Calculate opening balance (previous month's transactions - payments)
↓
Fetch all transactions for the month WHERE customer_name = 'Kelvin Van-Dyck'
↓
Fetch all customer payments for the month
```

### 3. **PDF Generation**
```
Create PDFDocument with password protection
↓
Build Header (Statement title, month/year)
↓
Add Account Details Box (name, account #, phone, date)
↓
Add Credit Summary Box (limit, used, available)
↓
Add Transaction Table:
  - Opening balance row
  - Transaction rows with items
  - Closing balance row (highlighted)
↓
Add Footer (thank you message)
↓
Return as Buffer
```

### 4. **Email Delivery**
```
Configure SMTP transporter
↓
Build HTML email (professional template)
↓
Attach PDF (filename: Statement_January_2026_Kelvin_Van-Dyck.pdf)
↓
Send via SMTP
↓
Log activity & return success
```

## PDF Structure

```
┌─────────────────────────────────────────┐
│  CUSTOMER STATEMENT                      │
│  January 2026                           │
│─────────────────────────────────────────│
│                                         │
│ ACCOUNT DETAILS                         │
│ Customer Name: Kelvin Van-Dyck         │
│ Account Number: #14                    │
│ Phone Number: +233 XXX XXX XXX         │
│ Statement Date: 26/01/2026             │
│                                         │
├─────────────────────────────────────────┤
│ CREDIT & STATEMENT SUMMARY              │
│ Credit Limit: GHS 10,000.00            │
│ Amount Used: GHS 7,500.00 (RED)        │
│ Available Credit: GHS 2,500.00 (GREEN) │
│ Access Code: 12345678                   │
├─────────────────────────────────────────┤
│ TRANSACTION DETAILS                     │
│─────────────────────────────────────────│
│ Date | Description | Debit | Credit | Balance
│─────────────────────────────────────────│
│ 01/01│ Opening Balance| | | GHS 5,000
│ 15/01│ Sale - CREDIT-123|GHS 2,000| |GHS 7,000
│  └─ Product A      |1x @ GHS 1,000|GHS 1,000
│  └─ Product B      |1x @ GHS 1,000|GHS 1,000
│ 20/01│ Payment Received| |GHS 1,000|GHS 6,000
│─────────────────────────────────────────│
│ CLOSING BALANCE: GHS 6,000 (YELLOW)    │
│                                         │
│ Thank you for your business.           │
│ © 2026 Footprint Retail Systems        │
└─────────────────────────────────────────┘
```

## Configuration Required

### Environment Variables (.env)
```
# SMTP Configuration (required for email delivery)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_SECURE=false
SMTP_USER=your-email@gmail.com
SMTP_PASS=your-app-password
ADMIN_EMAIL=noreply@footprint.com

# Optional
NODE_ENV=production
```

## Testing the Feature

### Option 1: From UI (Recommended)
1. Navigate to Credit Customers page
2. Find a credit customer (e.g., Kelvin Van-Dyck)
3. Click "Generate Statement"
4. Select month and year
5. Click "Send Statement"
6. Check console for logs and email inbox for PDF

### Option 2: From API (cURL)
```bash
curl -X POST http://localhost:5000/api/customers/14/email-statement \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{"month": 1, "year": 2026}'
```

### Option 3: Direct JavaScript
```javascript
const response = await fetch('/api/customers/14/email-statement', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${localStorage.getItem('authToken')}`
  },
  body: JSON.stringify({ month: 1, year: 2026 })
});

const data = await response.json();
console.log(data);
```

## Key Implementation Details

### 1. **Customer_name Field Strategy**
Instead of relying on numeric `customer_id` foreign keys, transactions include a `customer_name` field:
- **NULL** for cash/momo/card payments
- **Customer name string** for credit transactions
- Query: `WHERE customer_name = 'Kelvin Van-Dyck'`
- No JOINs needed, simpler and more reliable

### 2. **Balance Calculations**
```javascript
// Opening Balance = All previous transactions - All previous payments
const openingBalance = totalSalesBeforeMonth - totalPaymentsBeforeMonth;

// Running Balance (updated per transaction)
runningBalance += debit - credit;

// Closing Balance = Final running balance
const closingBalance = runningBalance;
```

### 3. **Password Protection**
- 8-digit random code generated per statement
- Unique to each customer-month combination
- Prevents unauthorized PDF access
- Displayed in email for customer convenience

### 4. **Item Rendering**
```javascript
// Handles various item data structures:
item.name / item.product_name       // Product identifier
item.quantity / item.qty            // Quantity
item.price / item.unit_price        // Unit price

// Calculates item total automatically
const itemTotal = qty * price;
```

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| "Customer not found" | Invalid customer ID | Verify customer ID exists in database |
| "No email on file" | Customer missing email | Add email to customer record |
| "SMTP CONFIG ERROR" | SMTP settings incorrect | Check .env SMTP_HOST, SMTP_PORT |
| "SMTP AUTH ERROR" | Wrong credentials | Verify SMTP_USER and SMTP_PASS |
| Transactions not showing | Wrong customer_name in DB | Ensure transactions have customer_name field populated |
| PDF not attached | PDF generation failed | Check browser console for errors |
| "Invalid month" | Month not 1-12 | Provide valid month (1-12) |

## Future Enhancements

1. **Scheduled Statements**: Auto-generate and email monthly statements
2. **Multiple Recipients**: Email statement to multiple recipients
3. **Custom Date Range**: Allow arbitrary date ranges (not just months)
4. **Statement Archive**: Store generated statements for audit trail
5. **Payment Plans**: Show payment schedule and due dates
6. **SMS Notification**: Notify customer when statement is ready
7. **Statement Customization**: Custom header/footer with business info

## Files Modified

- [server.js](server.js#L2886) - Email statement endpoint (lines 2886-3300)
  - Validation
  - Data gathering
  - PDF generation
  - Email delivery
  - Error handling

## Status

✅ **PRODUCTION READY**
- All features implemented
- Error handling comprehensive
- Console logging detailed
- Code validated and tested
- Database schema updated

---
**Last Updated**: January 26, 2026
**Version**: 1.0 - Initial Implementation
**Author**: Footprint POS Development Team
