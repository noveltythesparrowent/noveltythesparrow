const fs = require('fs');
const path = require('path');

function replaceInDirectory(dir) {
    const files = fs.readdirSync(dir);
    for (const file of files) {
        if (file === 'node_modules' || file === '.git' || file === 'package.json' || file === 'package-lock.json' || file === 'server.js' || file.endsWith('.png') || file.endsWith('.jpg')) continue;
        
        const fullPath = path.join(dir, file);
        const stat = fs.statSync(fullPath);
        
        if (stat.isDirectory()) {
            replaceInDirectory(fullPath);
        } else if (file.endsWith('.html') || file.endsWith('.js') || file.endsWith('.md')) {
            let content = fs.readFileSync(fullPath, 'utf8');
            if (content.includes('localStorage')) {
                const newContent = content.replace(/localStorage/g, 'localStorage');
                fs.writeFileSync(fullPath, newContent, 'utf8');
                console.log(`Updated: ${fullPath}`);
            }
        }
    }
}

replaceInDirectory('/Users/vandijk/Downloads/Telegram Desktop/2025 POS 4');
console.log('Migration complete!');
