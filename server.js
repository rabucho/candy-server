'use strict';
const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const cors       = require('cors');
const cron       = require('node-cron');
const { v4: uuidv4 } = require('uuid');
const db         = require('./db'); // the SQLite3 database instance

// ─── Helper: promisify db methods for cleaner async/await ──
function dbRun(sql, params) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function(err) {
      if (err) reject(err);
      else resolve({ lastID: this.lastID, changes: this.changes });
    });
  });
}
function dbGet(sql, params) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) reject(err);
      else resolve(row);
    });
  });
}
function dbAll(sql, params) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

// ─── App & HTTP server ────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

app.use(cors({ origin: '*' }));
app.use(express.json());

app.get('/', (req, res) => res.json({ status: 'Candy Relay running' }));
app.get('/ping', (_req, res) => res.json({ ok: true }));

// ─── Socket.io ────────────────────────────────────────────────
const io = new Server(server, {
  cors: { origin: '*' },
  logger: false,
});

let sellerSocket = null;
const SELLER_PIN = process.env.SELLER_PIN || '112233';

io.on('connection', async (socket) => {
  const authTimeout = setTimeout(() => socket.disconnect(true), 5000);

  // Send current shop status immediately on connection
  try {
    const shopSetting = await dbGet(`SELECT value FROM settings WHERE key = 'shop_open'`, []);
    const isOpen = shopSetting ? shopSetting.value === 'true' : true;
    socket.emit('shop:status', isOpen);
  } catch(e) {}

  socket.on('auth', async ({ role, sessionId, pin }) => {
    clearTimeout(authTimeout);

    try {
      if (role === 'seller') {
        if (pin !== SELLER_PIN) {
          socket.emit('auth:error', 'Invalid PIN');
          socket.disconnect(true);
          return;
        }
        sellerSocket = socket;
        socket.data.role = 'seller';
        socket.data.sessionId = 'seller';

        // Fetch pending queue
        const queue = await dbAll(
          `SELECT * FROM orders WHERE status IN ('PENDING','CONFIRMED') ORDER BY created_at ASC`,
          []
        );
        socket.emit('queue:snapshot', queue);
        socket.emit('auth:ok', { role: 'seller' });
        return;
      }

      // Buyer
      const sid = sessionId || uuidv4();
      await dbRun(
        `INSERT OR IGNORE INTO sessions(id, role) VALUES (?, 'buyer')`,
        [sid]
      );
      socket.data.role = 'buyer';
      socket.data.sessionId = sid;
      socket.emit('auth:ok', { role: 'buyer', sessionId: sid });

      // Send chat history
      const history = await dbAll(
        `SELECT * FROM messages WHERE thread_id = ? ORDER BY created_at ASC LIMIT 50`,
        [sid]
      );
      if (history.length) socket.emit('chat:history', history);
    } catch (err) {
      console.error('Auth error:', err.message);
      socket.emit('auth:error', 'Internal error');
      socket.disconnect(true);
    }
  });

  // ── Shop Toggle (Seller only) ───────────────────────────────
  socket.on('shop:toggle', async ({ isOpen }) => {
    if (socket.data.role !== 'seller') return;
    try {
      const val = isOpen ? 'true' : 'false';
      await dbRun(`UPDATE settings SET value = ? WHERE key = 'shop_open'`, [val]);
      io.emit('shop:status', isOpen);
    } catch (err) {
      console.error('Shop toggle error:', err.message);
    }
  });

  // ── Order creation ──────────────────────────────────────────
  socket.on('order:create', async ({ items, total, notes, buyerName, buyerPhone }) => {
    if (socket.data.role !== 'buyer') return;
    const id    = uuidv4();
    const sid   = socket.data.sessionId;
    const now   = Math.floor(Date.now() / 1000);
    try {
      await dbRun(
        `INSERT INTO orders(id, session_id, buyer_name, buyer_phone, items, total, status, notes, created_at)
         VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)`,
        [id, sid, buyerName || null, buyerPhone || null, JSON.stringify(items), total, notes || null, now]
      );
      const order = { 
        id, session_id: sid, buyer_name: buyerName || null, buyer_phone: buyerPhone || null, 
        items: JSON.stringify(items), total, status: 'PENDING', notes: notes || null, created_at: now 
      };
      socket.emit('order:created', order);
      if (sellerSocket?.connected) sellerSocket.emit('order:new', order);
    } catch (err) {
      console.error('Order create error:', err.message);
      socket.emit('order:error', { message: 'Failed to create order' });
    }
  });

  // ── Order update ────────────────────────────────────────────
  socket.on('order:update', async ({ orderId, status, smsRef }) => {
    if (socket.data.role !== 'seller') return;
    try {
      await dbRun(
        `UPDATE orders SET status = ?, sms_ref = ? WHERE id = ?`,
        [status, smsRef || null, orderId]
      );
      const updated = await dbGet(`SELECT * FROM orders WHERE id = ?`, [orderId]);
      if (updated) {
        // Notify the buyer with matching session_id
        io.sockets.sockets.forEach((s) => {
          if (s.data.sessionId === updated.session_id) {
            s.emit('order:statusChange', { orderId, status, smsRef });
          }
        });
        socket.emit('order:updateAck', { orderId, status });
      }
    } catch (err) {
      console.error('Order update error:', err.message);
      socket.emit('order:error', { message: 'Update failed' });
    }
  });

  // ── Chat (buyer sends) ──────────────────────────────────────
  socket.on('chat:send', async ({ content }) => {
    const role      = socket.data.role;
    const sessionId = socket.data.sessionId;
    if (!role || !sessionId || !content?.trim()) return;
    if (role === 'seller') return; // seller uses chat:reply

    const msg = {
      id: uuidv4(),
      thread_id: sessionId,
      sender_role: role,
      content: content.trim(),
      created_at: Math.floor(Date.now() / 1000),
    };
    try {
      await dbRun(
        `INSERT INTO messages(id, thread_id, sender_role, content, created_at)
         VALUES (?, ?, ?, ?, ?)`,
        [msg.id, msg.thread_id, msg.sender_role, msg.content, msg.created_at]
      );
      // Echo to buyer
      io.sockets.sockets.forEach((s) => {
        if (s.data.sessionId === sessionId) s.emit('chat:message', msg);
      });
      if (sellerSocket?.connected) sellerSocket.emit('chat:message', msg);
    } catch (err) {
      console.error('Chat send error:', err.message);
      socket.emit('chat:error', { message: 'Failed to send message' });
    }
  });

  // ── Seller replies ──────────────────────────────────────────
  socket.on('chat:reply', async ({ threadId, content }) => {
    if (socket.data.role !== 'seller') return;
    if (!threadId || !content?.trim()) return;
    const msg = {
      id: uuidv4(),
      thread_id: threadId,
      sender_role: 'seller',
      content: content.trim(),
      created_at: Math.floor(Date.now() / 1000),
    };
    try {
      await dbRun(
        `INSERT INTO messages(id, thread_id, sender_role, content, created_at)
         VALUES (?, ?, ?, ?, ?)`,
        [msg.id, msg.thread_id, msg.sender_role, msg.content, msg.created_at]
      );
      io.sockets.sockets.forEach((s) => {
        if (s.data.sessionId === threadId) s.emit('chat:message', msg);
      });
      socket.emit('chat:message', msg);
    } catch (err) {
      console.error('Reply error:', err.message);
      socket.emit('chat:error', { message: 'Reply failed' });
    }
  });

  // ── Seller opens a thread ──────────────────────────────────
  socket.on('chat:thread', async ({ threadId }) => {
    if (socket.data.role !== 'seller') return;
    try {
      const history = await dbAll(
        `SELECT * FROM messages WHERE thread_id = ? ORDER BY created_at ASC LIMIT 100`,
        [threadId]
      );
      socket.emit('chat:history', history);
    } catch (err) {
      console.error('Thread history error:', err.message);
      socket.emit('chat:error', { message: 'History fetch failed' });
    }
  });

  // ── Seller requests all threads ─────────────────────────────
  socket.on('chat:threads', async () => {
    if (socket.data.role !== 'seller') return;
    try {
      // Get the latest message for each thread
      const threads = await dbAll(`
        SELECT thread_id, content, sender_role, created_at 
        FROM messages 
        WHERE id IN (
          SELECT id FROM messages 
          GROUP BY thread_id 
          HAVING created_at = MAX(created_at)
        )
        ORDER BY created_at DESC
        LIMIT 50
      `, []);
      socket.emit('chat:threads', threads);
    } catch (err) {
      console.error('Threads fetch error:', err.message);
    }
  });

  socket.on('disconnect', () => {
    if (socket.data.role === 'seller') sellerSocket = null;
  });
});

// ─── Global error handler to prevent crashes ────────────────
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err.message);
  // Do not exit; keep server running
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection:', reason);
});

// ─── Start ────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  process.stdout.write(`Relay listening on :${PORT}\n`);
});