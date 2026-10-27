'use strict';
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(process.argv[2]);
if (!path.isAbsolute(root)) throw new Error('Usage: main.cjs ABSOLUTE_APP_ROOT PROFILE_ID');
const profile = String(process.argv[3] || 'default');
if (!/^[a-z0-9-]{1,40}$/.test(profile)) throw new Error('Invalid profile');
const runtime = __dirname;
const { createAssetTransport } = require('./android_asset_transport.cjs');
const { createServer, PORT } = require('./danmemore_server.cjs');
const databaseRoot = path.join(root, 'profiles'); fs.mkdirSync(databaseRoot, { recursive: true });
const metadataFile = path.join(root, 'profiles.json');
const metadata = fs.existsSync(metadataFile) ? JSON.parse(fs.readFileSync(metadataFile, 'utf8')) : {};
const eventDate = metadata.eventDates?.[profile] || null;
const contents = path.join(root, 'content', 'active');
if (![1,2,3,4,5,6,7,8].every(phase => fs.existsSync(path.join(contents, String(phase), 'project.manifest'))))
  throw new Error('Import the preserved contents ZIP before starting DanMemoRe');
const log = line => fs.appendFileSync(path.join(root, 'server.log'), `${line}\n`, { encoding: 'utf8', mode: 0o600 });
const assets = createAssetTransport({
  contents,
  work: root,
  voiceCatalog: JSON.parse(fs.readFileSync(path.join(runtime, 'voice-catalog.json'))),
  log,
});

const app = createServer({
  seed: JSON.parse(fs.readFileSync(path.join(runtime, 'seed.json'))),
  master: JSON.parse(fs.readFileSync(path.join(runtime, 'master.json'))),
  gachaArt: JSON.parse(fs.readFileSync(path.join(runtime, 'gacha-art.json'))),
  database: path.join(databaseRoot, `${profile}.sqlite`),
  databaseFactory: require('./sqlite.cjs').open,
  eventDate,
  resourceDirectory: path.join(runtime, 'manifests'),
  log,
});
// The original ForeverEden gameplay handler is untouched. Only GET/HEAD asset
// downloads are consumed here; every other request is delegated unchanged.
const handlers = app.server.listeners('request');
app.server.removeAllListeners('request');
app.server.on('request', (req, res) => {
  if (!assets.serve(req, res)) for (const handler of handlers) handler.call(app.server, req, res);
});
app.server.listen(PORT, '127.0.0.1', () => {
  const target = path.join(root, 'listener-status.json'), temporary = `${target}.tmp-${process.pid}`;
  fs.writeFileSync(temporary, `${JSON.stringify({ version: 1, port: PORT, pid: process.pid, ready: true,
    eventClock: app.store.eventClock.summary() })}\n`, { mode: 0o600 });
  fs.renameSync(temporary, target);
});
