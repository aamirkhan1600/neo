// Browser bridge: socket.io for live ticks + queue stats poller.
(function () {
  const socket = window.io ? io({ withCredentials: true }) : null;
  const ticksEl = document.getElementById('ticks');
  const symInput = document.getElementById('symInput');
  const subBtn = document.getElementById('subBtn');
  const queueStats = document.getElementById('queueStats');
  const logoutLink = document.getElementById('logoutLink');

  if (socket) {
    socket.on('connect_error', (e) => console.warn('ws auth failed', e.message));
    socket.on('tick', (t) => {
      if (!ticksEl) return;
      let row = document.getElementById('tick-' + t.symbol);
      if (!row) {
        row = document.createElement('tr');
        row.id = 'tick-' + t.symbol;
        row.innerHTML = `<td>${t.symbol}</td><td class="ltp"></td><td class="ts"></td>`;
        ticksEl.appendChild(row);
      }
      row.querySelector('.ltp').textContent = t.ltp.toFixed(2);
      row.querySelector('.ts').textContent = new Date(t.ts).toLocaleTimeString();
    });

    socket.on('order_update', (o) => {
      const row = document.getElementById('order-' + o.id);
      if (row) row.cells[5].textContent = o.status;
    });
  }

  if (subBtn && symInput) {
    subBtn.addEventListener('click', () => {
      const v = symInput.value.trim();
      if (v && socket) socket.emit('subscribe', [v]);
    });
  }

  if (queueStats) {
    async function refresh() {
      try {
        const r = await fetch('/api/reports/queue', { credentials: 'include' });
        if (r.ok) queueStats.textContent = JSON.stringify(await r.json(), null, 2);
      } catch {}
    }
    refresh();
    setInterval(refresh, 5000);
  }

  if (logoutLink) {
    logoutLink.addEventListener('click', async (e) => {
      e.preventDefault();
      await fetch('/api/auth/logout', { method: 'POST', credentials: 'include' });
      location.href = '/login';
    });
  }
})();
