require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function run() {
  try {
    const res = await pool.query('SELECT current_user;');
    console.log("DB connection:", res.rows[0]);
  } catch(e) {
    console.error(e);
  } finally { pool.end(); }
}
run();
