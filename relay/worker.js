// Strait of Georgia Marine Map: AIS relay on Cloudflare
// Keeps one connection to aisstream.io and serves the latest ship positions at /vessels,
// so the map works on phones. Your aisstream key stays here (as a secret), never in the web page.
const BOX = [[[49.09, -124.90], [49.57, -122.84]]];   // Sand Heads to Gambier Island, Baynes Sound to Reed Point
const KEEP_MINUTES = 30;          // drop ships not heard from in this long
const IDLE_MINUTES = 20;          // disconnect when nobody has looked at the map for this long
const ALLOWED = ['https://react20.github.io', 'http://localhost', 'http://127.0.0.1', 'null'];

export default {
  async fetch(req, env) {
    const origin = req.headers.get('Origin') || '';
    const cors = {
      'Access-Control-Allow-Origin': ALLOWED.some(a => origin === a || origin.startsWith(a + ':')) ? origin : ALLOWED[0],
      'Access-Control-Allow-Methods': 'GET, OPTIONS', 'Vary': 'Origin', 'Cache-Control': 'no-store'
    };
    if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });
    const url = new URL(req.url);
    if (url.pathname !== '/vessels') return new Response('Strait AIS relay is running. Ship data is at /vessels', { headers: cors });
    const relay = env.RELAY.get(env.RELAY.idFromName('strait'));
    const r = await relay.fetch(req);
    return new Response(r.body, { status: r.status, headers: { ...cors, 'Content-Type': 'application/json' } });
  }
};

export class AisRelay {
  constructor(state, env) {
    this.state = state; this.env = env;
    this.vessels = new Map(); this.ws = null; this.connecting = false;
    this.connected = false; this.lastMessage = null; this.lastView = 0; this.error = null;
  }
  async fetch() {
    this.lastView = Date.now();
    await this.ensure();
    if (!(await this.state.storage.getAlarm())) await this.state.storage.setAlarm(Date.now() + 30e3);
    this.prune();
    return new Response(JSON.stringify({
      connected: this.connected, lastMessage: this.lastMessage, error: this.error,
      vessels: [...this.vessels.values()].filter(v => v.lat != null)
    }));
  }
  async alarm() {
    // Stay connected (and in memory) while someone is using the map; let go when idle
    if (Date.now() - this.lastView < IDLE_MINUTES * 60e3) {
      await this.ensure(); this.prune();
      await this.state.storage.setAlarm(Date.now() + 30e3);
    } else {
      try { this.ws?.close(); } catch (e) {}
      this.ws = null; this.connected = false;
    }
  }
  async ensure() {
    if (this.ws || this.connecting) return;
    if (!this.env.AIS_KEY) { this.error = 'No aisstream key saved (run the installer again)'; return; }
    this.connecting = true;
    try {
      const r = await fetch('https://stream.aisstream.io/v0/stream', { headers: { Upgrade: 'websocket' } });
      const ws = r.webSocket;
      if (!ws) throw new Error('aisstream.io refused the connection (' + r.status + ')');
      ws.accept();
      this.ws = ws; this.connected = true; this.error = null;
      ws.send(JSON.stringify({
        APIKey: this.env.AIS_KEY, BoundingBoxes: BOX,
        FilterMessageTypes: ['PositionReport', 'StandardClassBPositionReport', 'ExtendedClassBPositionReport', 'ShipStaticData']
      }));
      const dec = new TextDecoder();
      ws.addEventListener('message', e => {
        try {
          const msg = JSON.parse(typeof e.data === 'string' ? e.data : dec.decode(e.data));
          if (msg.error) { this.error = 'aisstream.io: ' + msg.error; return; }
          this.handle(msg);
        } catch (err) {}
      });
      const down = () => { if (this.ws === ws) { this.ws = null; this.connected = false; } };
      ws.addEventListener('close', down); ws.addEventListener('error', down);
    } catch (e) {
      this.error = String(e.message || e); this.ws = null; this.connected = false;
    } finally { this.connecting = false; }
  }
  upsert(mmsi, patch) { const v = this.vessels.get(mmsi) || { mmsi }; Object.assign(v, patch); this.vessels.set(mmsi, v); }
  handle(msg) {
    const meta = msg.MetaData || {}, mmsi = String(meta.MMSI || ''); if (!mmsi) return;
    const name = (meta.ShipName || '').trim(), m = msg.Message || {};
    const pos = m.PositionReport || m.StandardClassBPositionReport || m.ExtendedClassBPositionReport;
    if (pos) this.upsert(mmsi, {
      name: name || this.vessels.get(mmsi)?.name || '',
      lat: pos.Latitude ?? meta.latitude, lon: pos.Longitude ?? meta.longitude,
      sog: pos.Sog, cog: pos.Cog, heading: pos.TrueHeading === 511 ? null : pos.TrueHeading, time: Date.now()
    });
    const st = m.ShipStaticData;
    if (st) this.upsert(mmsi, { name: (st.Name || name || '').trim(), callsign: (st.CallSign || '').trim(), type: st.Type, destination: (st.Destination || '').trim() });
    this.lastMessage = Date.now();
  }
  prune() { const cut = Date.now() - KEEP_MINUTES * 60e3; for (const [k, v] of this.vessels) if (!v.time || v.time < cut) this.vessels.delete(k); }
}
