(function() {
    // Prevent multiple instances
    if (document.getElementById('pos-emulator-overlay')) return;

    const styles = `
        .emulator-overlay {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: rgba(26, 42, 108, 0.95);
            padding: 12px;
            border-radius: 12px;
            z-index: 10000;
            display: flex;
            gap: 10px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.3);
            backdrop-filter: blur(8px);
            border: 1px solid rgba(255,255,255,0.1);
            font-family: 'Segoe UI', sans-serif;
            transition: transform 0.3s cubic-bezier(0.4, 0.0, 0.2, 1);
        }
        .emulator-btn {
            background: rgba(255,255,255,0.1);
            border: none;
            color: white;
            padding: 8px 16px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 13px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 6px;
            transition: all 0.2s;
        }
        .emulator-btn:hover {
            background: rgba(255,255,255,0.2);
            transform: translateY(-1px);
        }
        .emulator-btn.active {
            background: #fdbb2d;
            box-shadow: 0 2px 8px rgba(76, 175, 80, 0.4);
        }
        .emulator-label {
            color: rgba(255,255,255,0.7);
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 4px;
            text-align: center;
            width: 100%;
            position: absolute;
            top: -20px;
            right: 0;
        }
    `;

    const styleSheet = document.createElement("style");
    styleSheet.innerText = styles;
    document.head.appendChild(styleSheet);

    const container = document.createElement('div');
    container.id = 'pos-emulator-overlay';
    container.className = 'emulator-overlay';
    
    const label = document.createElement('div');
    label.className = 'emulator-label';
    label.innerText = '2025 POS Emulator';
    container.appendChild(label);

    const roles = [
        { name: 'Teller', role: 'cashier', icon: '🛒' },
        { name: 'Manager', role: 'manager', icon: '📊' },
        { name: 'CEO', role: 'ceo', icon: '💼' }
    ];

    roles.forEach(r => {
        const btn = document.createElement('button');
        btn.className = 'emulator-btn';
        btn.innerHTML = `<span>${r.icon}</span> ${r.name}`;
        
        btn.onclick = async () => {
            const originalText = btn.innerHTML;
            btn.innerHTML = '⌛ Switching...';
            try {
                const token = localStorage.getItem('token');
                const res = await fetch('/api/debug/switch-role', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
                    body: JSON.stringify({ role: r.role })
                });
                const data = await res.json();
                if (data.success) {
                    localStorage.setItem('token', data.token);
                    localStorage.setItem('user', JSON.stringify(data.user));
                    window.location.href = '/' + data.redirectTo;
                } else alert(data.message);
            } catch (e) { console.error(e); }
            btn.innerHTML = originalText;
        };
        container.appendChild(btn);
    });
    document.body.appendChild(container);
})();