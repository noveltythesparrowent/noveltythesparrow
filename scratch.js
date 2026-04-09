const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
    connectionString: process.env.DATABASE_URL
});

async function main() {
    try {
        const catRes = await pool.query('SELECT * FROM categories LIMIT 5');
        console.log("Categories:", catRes.rows);
    } catch(e) {
        console.error(e);
    } finally {
        pool.end();
    }
}

main();
