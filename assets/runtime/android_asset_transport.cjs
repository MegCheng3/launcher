'use strict';

// RevivalSide's boundary: verified private payload -> bounded, range-capable
// HTTP streaming. No game saves or gameplay listener contracts belong here.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const v8 = require('node:v8');
const { Readable, pipeline } = require('node:stream');
const hash = bytes => crypto.createHash('md5').update(bytes).digest('hex');

function contentType(name) {
  const extension = path.extname(name).toLowerCase();
  return ({ '.json': 'application/json', '.manifest': 'application/json', '.zip': 'application/zip',
    '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp',
    '.ogg': 'audio/ogg', '.mp3': 'audio/mpeg' })[extension] ?? 'application/octet-stream';
}

function safeFile(root, relative) {
  if (typeof relative !== 'string' || relative.includes('\\') || relative.split('/').some(p => !p || p === '.' || p === '..'))
    throw new Error('Unsafe preserved asset path');
  const result = path.resolve(root, relative);
  if (!result.startsWith(path.resolve(root) + path.sep)) throw new Error('Preserved asset escapes its root');
  return result;
}

function zipParts(contents, episode) {
  const parts = [], central = [];
  let offset = 0;
  for (const entry of episode.files) {
    const name = Buffer.from(`${episode.id}/${entry.path}`);
    const file = safeFile(contents, `voice_root/files/sound/voice_episode/${episode.id}/${entry.path}`);
    const header = Buffer.alloc(30 + name.length);
    header.writeUInt32LE(0x04034b50); header.writeUInt16LE(20, 4); header.writeUInt16LE(33, 12);
    header.writeUInt32LE(entry.crc, 14); header.writeUInt32LE(entry.size, 18); header.writeUInt32LE(entry.size, 22);
    header.writeUInt16LE(name.length, 26); name.copy(header, 30);
    const directory = Buffer.alloc(46 + name.length);
    directory.writeUInt32LE(0x02014b50); directory.writeUInt16LE(788, 4); directory.writeUInt16LE(20, 6);
    directory.writeUInt16LE(33, 14); directory.writeUInt32LE(entry.crc, 16);
    directory.writeUInt32LE(entry.size, 20); directory.writeUInt32LE(entry.size, 24);
    directory.writeUInt16LE(name.length, 28); directory.writeUInt32LE(0x81a40000, 38);
    directory.writeUInt32LE(offset, 42); name.copy(directory, 46);
    parts.push({ bytes: header, size: header.length }, { file, size: entry.size }); central.push(directory);
    offset += header.length + entry.size;
  }
  const directory = Buffer.concat(central), end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50); end.writeUInt16LE(central.length, 8); end.writeUInt16LE(central.length, 10);
  end.writeUInt32LE(directory.length, 12); end.writeUInt32LE(offset, 16);
  parts.push({ bytes: directory, size: directory.length }, { bytes: end, size: end.length });
  if (offset + directory.length + end.length !== episode.size) throw new Error('Voice ZIP recipe size mismatch');
  return parts;
}

async function* readParts(parts, start, end) {
  let offset = 0;
  for (const part of parts) {
    const from = Math.max(0, start - offset), to = Math.min(part.size - 1, end - offset);
    offset += part.size;
    if (from > to) continue;
    if (part.bytes) yield part.bytes.subarray(from, to + 1);
    else yield* fs.createReadStream(part.file, { start: from, end: to, highWaterMark: 256 * 1024 });
    if (offset > end) break;
  }
}

function byteRange(value, size) {
  if (!value) return { start: 0, end: size - 1, partial: false };
  const match = /^bytes=(\d*)-(\d*)$/.exec(value.trim());
  if (!match || !size || (!match[1] && !match[2])) throw new Error('Invalid range');
  const start = match[1] ? Number(match[1]) : Math.max(0, size - Number(match[2]));
  const end = match[1] && match[2] ? Math.min(size - 1, Number(match[2])) : size - 1;
  if (![start,end].every(Number.isSafeInteger) || start < 0 || start >= size || end < start || (!match[1] && Number(match[2]) <= 0))
    throw new Error('Invalid range');
  return { start, end, partial: true };
}

function createAssetTransport({ contents, work, voiceCatalog = null, log = () => {} }) {
  const manifests = Array.from({ length: 8 }, (_, i) => safeFile(contents, `${i + 1}/project.manifest`));
  const manifestDirectory = safeFile(contents, '1/manifests');
  const downloadManifests = fs.readdirSync(manifestDirectory)
    .filter(name => /\.manifest(?:\.[1-8])?$/.test(name)).sort()
    .map(name => safeFile(manifestDirectory, name));
  const auxiliary = ['single_image','area_atlas','voice_episode'].map(name => safeFile(manifestDirectory, `${name}.manifest`));
  const marker = path.join(contents, '.danmemore-content.json');
  const identity = hash(JSON.stringify([3, path.resolve(contents), process.versions.v8,
    [...manifests,...downloadManifests].map(file => { const s = fs.statSync(file); return [s.size,s.mtimeMs,s.ctimeMs,s.ino]; }),
    fs.existsSync(marker) ? fs.readFileSync(marker, 'utf8') : null, voiceCatalog]));
  fs.mkdirSync(work, { recursive: true });
  const cache = path.join(work, 'asset-index-v3.bin');
  let index;
  try {
    const saved = v8.deserialize(fs.readFileSync(cache));
    if (saved.identity === identity && saved.routes instanceof Map && saved.auxiliary instanceof Map &&
        saved.projects?.length === 8 && saved.versions?.length === 8)
      index = saved;
  } catch { /* Interrupted/old derived caches are rebuilt; the verified payload is untouched. */ }
  if (!index) {
    const routes = new Map(), auxiliaryRoutes = new Map(), projects = [], versions = [];
    const sources = manifests.map(file => fs.readFileSync(file));
    const fingerprint = crypto.createHash('md5');
    sources.forEach((source, i) => fingerprint.update(String(i + 1)).update(Buffer.from([0])).update(source));
    let voiceBytes;
    if (voiceCatalog) {
      if (voiceCatalog.version !== 1 || !Array.isArray(voiceCatalog.episodes) || !voiceCatalog.episodes.length)
        throw new Error('Unsupported or empty archived voice catalog');
      const voiceSource = fs.readFileSync(auxiliary[2]);
      if (crypto.createHash('sha256').update(voiceSource).digest('hex') !== voiceCatalog.sourceSha256)
        throw new Error('Imported voice manifest does not match this client/payload contract');
      const manifest = JSON.parse(voiceSource);
      for (const episode of voiceCatalog.episodes) {
        const original = manifest.episodes[episode.id];
        if (!original || !/^\d{10}$/.test(episode.id) || !/^[a-f0-9]{32}$/.test(episode.md5) ||
            !Number.isSafeInteger(episode.size) || episode.size <= 0 || !Array.isArray(episode.files) || !episode.files.length)
          throw new Error('Invalid archived voice episode');
        const route = `resources/voice/packages/${episode.id}/${episode.md5}.zip`;
        manifest.episodes[episode.id] = { ...original, path: route, md5: episode.md5, size: episode.size };
        routes.set(route, { episode, size: episode.size, md5: episode.md5 });
      }
      voiceBytes = Buffer.from(JSON.stringify(manifest));
    }
    for (const file of downloadManifests) {
      const name = path.basename(file);
      const bytes = name === 'voice_episode.manifest' && voiceBytes ? voiceBytes : fs.readFileSync(file);
      auxiliaryRoutes.set(name, { bytes, size: bytes.length, md5: hash(bytes) });
      fingerprint.update(name).update(Buffer.from([0])).update(bytes);
    }
    const version = fingerprint.digest('hex');
    for (let i = 0; i < 8; i++) {
      const phase = String(i + 1), project = JSON.parse(sources[i]);
      if (i === 0 && voiceBytes) project.assets['manifests/voice_episode.manifest'] = {
        path: 'resources/voice/voice_episode.manifest', md5: hash(voiceBytes), size: voiceBytes.length,
      };
      for (const [relative, metadata] of Object.entries(project.assets ?? {})) {
        routes.set(metadata.path, i === 0 && relative === 'manifests/voice_episode.manifest' && voiceBytes
          ? { bytes: voiceBytes, size: voiceBytes.length, md5: hash(voiceBytes) }
          : { relative: `${phase}/${relative}`, size: metadata.size, md5: metadata.md5 });
      }
      Object.assign(project, { version, packageUrl: '{}',
        remoteManifestUrl: `{}/manifests/us/pkm/project.manifest.${phase}/${version}`,
        remoteVersionUrl: `{}/asset/manifest/${version}/us/pkm/version.manifest.${phase}`,
        size: Object.values(project.assets ?? {}).reduce((n, row) => n + Number(row.size ?? 0), 0) });
      projects.push(Buffer.from(JSON.stringify(project)));
      versions.push(Buffer.from(JSON.stringify({ engineVersion: project.engineVersion, packageUrl: '{}', version,
        remoteManifestUrl: project.remoteManifestUrl, remoteVersionUrl: project.remoteVersionUrl })));
    }
    for (const [at, key, root] of [[0,'singleImages','single_image_root'],[1,'areaAtlas','area_atlas_root']]) {
      const document = JSON.parse(fs.readFileSync(auxiliary[at]));
      for (const row of Object.values(document[key] ?? {})) {
        // Separate native cache roots are not included in the eight phase indexes.
        if (!row.path.startsWith('contents/')) throw new Error('Unknown auxiliary asset URL');
        const relative = `${root}/${row.path.slice('contents/'.length, row.path.lastIndexOf('/'))}`;
        const file = safeFile(contents, relative);
        if (fs.existsSync(file) && fs.statSync(file).isFile()) routes.set(row.path, { relative, size: row.size, md5: row.md5 });
      }
    }
    index = { identity, routes, auxiliary: auxiliaryRoutes, projects, versions };
    const temporary = `${cache}.tmp-${process.pid}`;
    fs.writeFileSync(temporary, v8.serialize(index), { mode: 0o600 }); fs.renameSync(temporary, cache);
    log(JSON.stringify({ event: 'asset-index', cached: false, routes: routes.size }));
  } else log(JSON.stringify({ event: 'asset-index', cached: true, routes: index.routes.size }));

  function directResource(target) {
    let file;
    try { file = safeFile(contents, target); } catch { return null; }
    let stat;
    try { stat = fs.statSync(file); } catch { return null; }
    if (!stat.isFile()) return null;
    return { relative: target, size: stat.size,
      etag: `W/"${stat.size.toString(16)}-${Math.trunc(stat.mtimeMs).toString(16)}"` };
  }

  function resolve(url) {
    const pathname = decodeURIComponent(new URL(url, 'http://127.0.0.1').pathname);
    if (pathname === '/storage/eu/status.json') {
      const bytes = Buffer.from('{"code":0}\n');
      return { bytes, size: bytes.length, md5: hash(bytes) };
    }
    const prefix = pathname.match(/^\/(api|content|storage)\/(.+)$/);
    if (!prefix) return null;
    const namespace = prefix[1], target = prefix[2];
    if (target.includes('\\') || target.split('/').some(part => !part || part === '.' || part === '..'))
      throw new Error('Unsafe preserved asset URL');
    const flat = target.match(/^(project|version)\.manifest\.([1-8])$/);
    const project = target.match(/^manifests\/us\/pkm\/project\.manifest\.([1-8])(?:\/.*)?$/);
    const version = target.match(/^asset\/manifest\/.*\/version\.manifest\.([1-8])(?:\/.*)?$/);
    const directManifest = target.match(/^([1-8])\/(project|version)\.manifest$/);
    if (flat || project || version || directManifest) {
      const phase = Number(flat?.[2] ?? project?.[1] ?? version?.[1] ?? directManifest?.[1]);
      const kind = flat?.[1] ?? (version ? 'version' : directManifest?.[2] ?? 'project');
      const bytes = (kind === 'version' ? index.versions : index.projects)[phase - 1];
      return { bytes, size: bytes.length, md5: hash(bytes) };
    }
    const auxiliary = target.match(/^manifests\/us\/pkm\/([^/]+\.manifest(?:\.[1-8])?)(?:\/.*)?$/);
    if (auxiliary) return index.auxiliary.get(auxiliary[1]) ?? null;
    const directAuxiliary = target.match(/^1\/manifests\/([^/]+\.manifest(?:\.[1-8])?)$/);
    if (directAuxiliary) return index.auxiliary.get(directAuxiliary[1]) ?? null;
    const indexed = index.routes.get(target);
    if (indexed) return indexed;
    return namespace === 'content' || namespace === 'storage' ? directResource(target) : null;
  }

  function serve(req, res) {
    if (!['GET','HEAD'].includes(req.method)) return false;
    let resource;
    try { resource = resolve(req.url); }
    catch { res.writeHead(400, { 'Content-Length': '0' }); res.end(); return true; }
    if (!resource) {
      const pathname = new URL(req.url, 'http://127.0.0.1').pathname;
      if (!/^\/(?:content|storage)\//.test(pathname) &&
          !/^\/api\/(?:asset\/manifest|manifests\/us\/pkm|contents\/|resources\/)/.test(pathname)) return false;
      log(JSON.stringify({ event: 'asset-unavailable', path: req.url, message: 'Preserved asset was not found' }));
      res.writeHead(404, { 'Content-Length': '0' }); res.end(); return true;
    }
    let parts;
    try {
      parts = resource.episode ? zipParts(contents, resource.episode) : [resource.bytes
        ? { bytes: resource.bytes, size: resource.size } : { file: safeFile(contents, resource.relative), size: resource.size }];
      for (const part of parts) if (part.file) {
        const stat = fs.statSync(part.file);
        if (!stat.isFile() || stat.size !== part.size) throw new Error('Archived asset is incomplete');
      }
    } catch (error) {
      log(JSON.stringify({ event: 'asset-unavailable', path: req.url, message: error.message }));
      res.writeHead(404, { 'Content-Length': '0' }); res.end(); return true;
    }
    let range;
    try { range = byteRange(req.headers.range, resource.size); }
    catch { res.writeHead(416, { 'Content-Range': `bytes */${resource.size}`, 'Content-Length': '0' }); res.end(); return true; }
    const etag = resource.etag ?? `"${resource.md5}"`;
    const pathname = new URL(req.url, 'http://127.0.0.1').pathname;
    const cacheControl = /manifest|status\.json/i.test(pathname) ? 'no-cache' : 'public, max-age=31536000, immutable';
    const matches = String(req.headers['if-none-match'] ?? '').split(',').map(value => value.trim());
    if (!range.partial && (matches.includes('*') || matches.includes(etag))) {
      res.writeHead(304, { ETag: etag, 'Cache-Control': cacheControl, 'Content-Length': '0' }); res.end(); return true;
    }
    const headers = { 'Content-Type': contentType(resource.relative ?? req.url),
      'Content-Length': String(Math.max(0,range.end - range.start + 1)),
      'Accept-Ranges': 'bytes', ETag: etag, 'Cache-Control': cacheControl };
    if (range.partial) headers['Content-Range'] = `bytes ${range.start}-${range.end}/${resource.size}`;
    res.writeHead(range.partial ? 206 : 200, headers);
    log(JSON.stringify({ event: 'asset', path: req.url, status: range.partial ? 206 : 200, bytes: Number(headers['Content-Length']) }));
    if (req.method === 'HEAD' || !resource.size) res.end();
    else pipeline(Readable.from(readParts(parts, range.start, range.end), { objectMode: false, highWaterMark: 256 * 1024 }), res, error => {
      if (error && !res.destroyed) res.destroy(error);
    });
    return true;
  }
  return { serve, resolve };
}

module.exports = { createAssetTransport, zipParts, readParts, byteRange };
