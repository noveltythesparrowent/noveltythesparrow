import os

def migrate_storage(directory):
    for root, dirs, files in os.walk(directory):
        if 'node_modules' in root or '.git' in root:
            continue
        for file in files:
            if file.endswith('.html') or file.endswith('.js') or file.endswith('.md'):
                filepath = os.path.join(root, file)
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if 'sessionStorage' in content:
                    new_content = content.replace('sessionStorage', 'localStorage')
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    print(f"Updated: {filepath}")

migrate_storage('/Users/vandijk/Downloads/Telegram Desktop/2025 POS 4')
print("Migration complete!")
