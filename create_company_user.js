require('dotenv').config();
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function run() {
  try {
    const salt = await bcrypt.genSalt(10);
    const hash = await bcrypt.hash('Faith2026', salt);
    
    // Check if company exists
    let companyId = 1;
    const compRes = await pool.query("SELECT id FROM companies WHERE name ILIKE '%Novelty The Sparrow%' LIMIT 1");
    if (compRes.rows.length > 0) {
      companyId = compRes.rows[0].id;
    } else {
      const insertComp = await pool.query("INSERT INTO companies (name, email) VALUES ('Novelty The Sparrow', 'sales@faithpharmacy.com') RETURNING id");
      companyId = insertComp.rows[0].id;
    }
    
    await pool.query(
      "INSERT INTO company_users (company_id, company_name, contact_person, email, password, role, status) VALUES ($1, $2, $3, $4, $5, 'business_client', 'Active') ON CONFLICT (email) DO UPDATE SET password = $5",
      [companyId, 'Novelty The Sparrow', 'Company Sales', 'sales@faithpharmacy.com', hash]
    );
    console.log('Successfully created company user sales@faithpharmacy.com');
  } catch (err) {
    console.error(err);
  } finally {
    pool.end();
  }
}
run();
