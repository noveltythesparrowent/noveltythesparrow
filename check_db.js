require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function checkDb() {
    try {
        const res = await pool.query("SELECT * FROM users");
        console.log("USERS IN DB:", res.rows);
    } catch (err) {
        console.error("ERROR QUERYING DB:", err);
    } finally {
        pool.end();
    }
}
checkDb();
