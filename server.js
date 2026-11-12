// Halay Aktarıcı (bulut sunucusu)
// Oyun, yayıncının yazdığı TikTok kullanıcı adıyla bu sunucuya sorar; sunucu o yayına bağlanır
// ve olayları (giriş, yorum, beğeni, hediye) oyuna HTTP üzerinden verir. Bilgisayarında hiçbir şey çalışmaz.
//
// Ortam değişkenleri:
//   RELAY_KEY      : oyunla aynı gizli anahtar (zorunlu, başkaları kullanamasın diye)
//   SIGN_API_KEY   : (isteğe bağlı) Euler Stream anahtarı, TikTok bağlantısı sık reddedilirse
//   PORT           : barındırma servisi kendisi verir
//
// Uçlar:
//   GET /            -> "ok" (sağlık kontrolü)
//   GET /events?u=kullaniciadi&after=0[&first=1]   (başlık: x-relay-key)
//        -> { status, error, events: [{ s, t, u, n, ... }] }
//   GET /stop?u=kullaniciadi             -> bağlantıyı kapatır

const http = require('http');

const RELAY_KEY = process.env.RELAY_KEY || '';
const SIGN_API_KEY = process.env.SIGN_API_KEY || '';
const PORT = Number(process.env.PORT) || 3000;
const IDLE_CLOSE_MS = 120_000;   // oyun 2 dk sormazsa bağlantı kapanır
const MAX_BUFFER = 600;

let lib = null;
function loadLib() {
  if (!lib) lib = require('tiktok-live-connector');
  return lib;
}

const rooms = new Map();

function cut(s, n) {
  s = String(s ?? '').normalize('NFC').replace(/\s+/g, ' ').trim();
  const chars = Array.from(s);
  return chars.length > n ? chars.slice(0, n).join('') : s;
}

function userOf(d) {
  const u = d.user || d;
  const uid = u.uniqueId || d.uniqueId || u.userId || d.userId;
  const name = u.nickname || d.nickname || uid;
  return { uid: String(uid || ''), name: String(name || '') };
}

function makeRoom(username) {
  const room = {
    username,
    status: 'baglaniyor',
    error: '',
    seq: 0,
    events: [],
    pendingLikes: 0,
    lastPoll: Date.now(),
    lastSent: new Map(),
    conn: null,
    retryTimer: null,
    closed: false,
  };

  const push = (ev) => {
    room.seq += 1;
    ev.s = room.seq;
    room.events.push(ev);
    if (room.events.length > MAX_BUFFER) room.events.splice(0, room.events.length - MAX_BUFFER);
  };
  const throttled = (key, ms) => {
    const now = Date.now();
    if (room.lastSent.has(key) && now - room.lastSent.get(key) < ms) return true;
    room.lastSent.set(key, now);
    return false;
  };
  room.push = push;

  const connect = () => {
    if (room.closed) return;
    const { TikTokLiveConnection, WebcastPushConnection } = loadLib();
    const Connection = TikTokLiveConnection || WebcastPushConnection;
    const options = SIGN_API_KEY
      ? { signApiKey: SIGN_API_KEY, signProviderOptions: { params: { apiKey: SIGN_API_KEY } } }
      : {};
    const conn = new Connection(username, options);
    room.conn = conn;
    room.status = 'baglaniyor';

    conn.on('member', (d) => {
      const { uid, name } = userOf(d);
      if (uid && !throttled('j' + uid, 120_000)) push({ t: 'j', u: cut(uid, 30), n: cut(name, 24) });
    });
    conn.on('chat', (d) => {
      const { uid, name } = userOf(d);
      if (uid && !throttled('c' + uid, 2_500)) push({ t: 'c', u: cut(uid, 30), n: cut(name, 24), m: cut(d.comment, 60) });
    });
    conn.on('like', (d) => {
      room.pendingLikes += Number(d.likeCount) || 1;
      const { uid, name } = userOf(d);
      if (uid && !throttled('k' + uid, 20_000)) push({ t: 'k', u: cut(uid, 30), n: cut(name, 24) });
    });
    for (const evName of ['follow', 'share']) {
      conn.on(evName, (d) => {
        const { uid, name } = userOf(d);
        if (uid && !throttled('k' + uid, 20_000)) push({ t: 'k', u: cut(uid, 30), n: cut(name, 24) });
      });
    }
    conn.on('gift', (d) => {
      const { uid, name } = userOf(d);
      const details = d.giftDetails || d.gift || {};
      const giftName = d.giftName || details.giftName || details.name || 'Hediye';
      const diamonds = d.diamondCount ?? details.diamondCount ?? 1;
      const giftType = d.giftType ?? details.giftType;
      if (giftType === 1 && !d.repeatEnd) return; // seri hediye: bitince toplamla gönder
      push({ t: 'g', u: cut(uid, 30), n: cut(name, 24), g: cut(giftName, 24), d: diamonds | 0, c: (d.repeatCount || 1) | 0 });
    });
    conn.on('streamEnd', () => {
      room.status = 'bitti';
      room.error = 'Yayın sona erdi';
    });
    conn.on('disconnected', () => {
      if (room.closed) return;
      if (room.status === 'bagli') room.status = 'koptu';
      scheduleRetry();
    });

    conn.connect()
      .then(() => {
        room.status = 'bagli';
        room.error = '';
        console.log(`✅ @${username} yayınına bağlandı`);
      })
      .catch((err) => {
        const msg = String((err && (err.message || err.info)) || err);
        room.status = /offline|not.*live|LIVE has ended|isn't online/i.test(msg) ? 'yayin_yok' : 'hata';
        room.error = cut(msg, 140);
        console.log(`❌ @${username}: ${room.error}`);
        scheduleRetry();
      });
  };

  const scheduleRetry = () => {
    if (room.closed || room.retryTimer) return;
    room.retryTimer = setTimeout(() => {
      room.retryTimer = null;
      try { room.conn && room.conn.disconnect(); } catch (_) {}
      connect();
    }, 10_000);
  };

  room.close = () => {
    room.closed = true;
    clearTimeout(room.retryTimer);
    try { room.conn && room.conn.disconnect(); } catch (_) {}
  };

  connect();
  return room;
}

function getRoom(username) {
  let room = rooms.get(username);
  if (!room) {
    room = makeRoom(username);
    rooms.set(username, room);
  }
  return room;
}

// beğenileri saniyede bir toplu olay yap, boşta kalan odaları kapat
setInterval(() => {
  const now = Date.now();
  for (const [name, room] of rooms) {
    if (room.pendingLikes > 0) {
      room.push({ t: 'l', c: room.pendingLikes });
      room.pendingLikes = 0;
    }
    if (now - room.lastPoll > IDLE_CLOSE_MS) {
      room.close();
      rooms.delete(name);
      console.log(`💤 @${name} kapatıldı (oyun sormuyor)`);
    }
  }
}, 1000);

function send(res, code, obj) {
  const body = typeof obj === 'string' ? obj : JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': typeof obj === 'string' ? 'text/plain; charset=utf-8' : 'application/json; charset=utf-8' });
  res.end(body);
}

function cleanUser(u) {
  u = String(u || '').trim().replace(/^@/, '').toLowerCase();
  return /^[a-z0-9_.]{2,24}$/.test(u) ? u : null;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/') return send(res, 200, 'ok');

  if (!RELAY_KEY || req.headers['x-relay-key'] !== RELAY_KEY) {
    return send(res, 401, { status: 'yetkisiz', error: 'Anahtar hatalı', events: [] });
  }
  const user = cleanUser(url.searchParams.get('u'));
  if (!user) return send(res, 400, { status: 'gecersiz', error: 'Kullanıcı adı geçersiz', events: [] });

  if (url.pathname === '/stop') {
    const room = rooms.get(user);
    if (room) { room.close(); rooms.delete(user); }
    return send(res, 200, { status: 'kapali', events: [] });
  }

  if (url.pathname === '/events') {
    const room = getRoom(user);
    room.lastPoll = Date.now();
    const after = Number(url.searchParams.get('after')) || 0;
    // ilk sorguda (first=1) eski olayları tekrar oynatma, sadece sıra numarasını ver
    const first = url.searchParams.get('first') === '1';
    const events = first ? [] : room.events.filter((e) => e.s > after).slice(0, 200);
    return send(res, 200, { status: room.status, error: room.error, seq: room.seq, events });
  }

  send(res, 404, { status: 'yok', events: [] });
});

if (require.main === module) {
  if (!RELAY_KEY) console.warn('UYARI: RELAY_KEY ayarlı değil, istekler reddedilecek.');
  server.listen(PORT, () => console.log(`Halay aktarıcı ${PORT} portunda çalışıyor`));
}

module.exports = { server, rooms, getRoom, cleanUser, _setLib: (l) => { lib = l; } };
