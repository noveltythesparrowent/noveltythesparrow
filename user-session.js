/**
 * Shared User Session Management
 * Handles displaying username and logout functionality across all pages.
 * 
 * Usage:
 * 1. Include this script in your HTML: <script src="user-session.js"></script>
 * 2. Add a container in your header: <div id="user-display"></div>
 */

document.addEventListener('DOMContentLoaded', async () => {
    // 1. Find or Create the Container
    let userContainer = document.getElementById('user-display');
    
    // If container doesn't exist, try to find the nav bar and append it
    if (!userContainer) {
        const nav = document.querySelector('nav') || document.querySelector('.header') || document.querySelector('.top-bar');
        if (nav) {
            userContainer = document.createElement('div');
            userContainer.id = 'user-display';
            userContainer.style.marginLeft = 'auto'; // Push to the right
            userContainer.style.marginRight = '20px';
            nav.appendChild(userContainer);
        }
    }

    if (!userContainer) {
        console.warn('User display container not found. Please add <div id="user-display"></div> to your HTML header.');
        return;
    }

    // 2. Check Authentication Token
    const token = localStorage.getItem('authToken');
    
    if (!token) {
        // If not logged in and not on login page, redirect
        if (!window.location.pathname.includes('login')) {
            window.location.href = '/login';
        }
        return;
    }

    // 3. Fetch Session Data from Server
    try {
        const response = await fetch('/api/session', {
            headers: {
                'Authorization': `Bearer ${token}`
            }
        });

        if (response.ok) {
            const data = await response.json();
            const user = data.user;
            
            // 4. Render User Info & Logout Button
            // Using inline styles to ensure it looks good without external CSS
            userContainer.innerHTML = `
                <div style="display: flex; align-items: center; gap: 15px; color: #333; font-family: 'Poppins', sans-serif;">
                    <div style="text-align: right; line-height: 1.2;">
                        <div style="font-weight: 600; font-size: 14px;">${escapeHtml(user.name)}</div>
                        <div style="font-size: 11px; opacity: 0.7; text-transform: uppercase; letter-spacing: 0.5px;">${escapeHtml(user.role)}</div>
                    </div>
                    <button id="logout-btn" style="
                        background: #b21f1f; 
                        border: none; 
                        color: white; 
                        padding: 8px 16px; 
                        border-radius: 6px; 
                        cursor: pointer; 
                        font-size: 13px; 
                        font-weight: 500;
                        transition: all 0.2s;
                        display: flex;
                        align-items: center;
                        gap: 6px;
                    ">
                        <span>Logout</span>
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"></path><polyline points="16 17 21 12 16 7"></polyline><line x1="21" y1="12" x2="9" y2="12"></line></svg>
                    </button>
                </div>
            `;
            
            // 5. Attach Logout Handler
            document.getElementById('logout-btn').addEventListener('click', handleLogout);
            
        } else {
            // Token invalid or expired
            handleLogout();
        }
    } catch (error) {
        console.error('Session check error:', error);
    }
});

async function handleLogout() {
    try {
        const token = localStorage.getItem('authToken');
        if (token) {
            await fetch('/api/logout', { 
                method: 'POST',
                headers: { 'Authorization': `Bearer ${token}` }
            });
        }
    } catch (e) {
        console.error('Logout API error', e);
    } finally {
        localStorage.clear();
        window.location.href = '/login';
    }
}

function escapeHtml(text) {
    if (!text) return '';
    return text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}