// Auto-refresh the page every 60 seconds
(function() {
    let lastRefresh = Date.now();
    let countdown = 60;
    
    setInterval(function() {
        const now = Date.now();
        const elapsed = Math.floor((now - lastRefresh) / 1000);
        countdown = 60 - elapsed;
        
        if (elapsed >= 60) {
            lastRefresh = now;
            countdown = 60;
            // Reload the page
            window.location.reload();
        }
    }, 1000);
})();
