import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, readdir, rename, rm, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import CryptoJS from 'crypto-js';
import ffmpegPath from 'ffmpeg-static';

const cacheDir = join(tmpdir(), 'eva-music-audio');
const cacheTtlMs = 12 * 60 * 60 * 1000;
const conversions = new Map();

function isSaavnUrl(url) {
  try {
    const parsed = new URL(url);
    const hostname = parsed.hostname.toLowerCase();
    return parsed.protocol === 'https:' && (hostname === 'saavncdn.com' || hostname.endsWith('.saavncdn.com'));
  } catch {
    return false;
  }
}

function decryptSaavnMediaUrl(encryptedUrl) {
  if (!encryptedUrl) return null;
  try {
    const key = CryptoJS.enc.Utf8.parse('38346591');
    const cipherParams = CryptoJS.lib.CipherParams.create({
      ciphertext: CryptoJS.enc.Base64.parse(encryptedUrl),
    });
    const decrypted = CryptoJS.DES.decrypt(cipherParams, key, {
      mode: CryptoJS.mode.ECB,
      padding: CryptoJS.pad.Pkcs7,
    });
    const url = decrypted.toString(CryptoJS.enc.Utf8);
    if (!url.startsWith('http')) return null;
    return url.replace('_96.mp4', '_320.mp4').replace('_160.mp4', '_320.mp4');
  } catch {
    return null;
  }
}

async function resolveSaavnTrackUrl(trackId) {
  const match = /^saavn-([A-Za-z0-9_-]+)$/.exec(trackId);
  if (!match) return null;
  const songId = match[1];
  const endpoint = new URL('https://www.jiosaavn.com/api.php');
  endpoint.search = new URLSearchParams({
    __call: 'song.getDetails',
    _format: 'json',
    cc: 'in',
    _marker: '0',
    pids: songId,
  }).toString();
  const response = await fetch(endpoint, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
      Referer: 'https://www.jiosaavn.com/',
    },
    signal: AbortSignal.timeout(8000),
  });
  if (!response.ok) throw new Error(`JioSaavn track lookup failed with HTTP ${response.status}`);
  const text = await response.text();
  const jsonStart = text.indexOf('{');
  if (jsonStart === -1) return null;
  const data = JSON.parse(text.slice(jsonStart));
  const song = data[songId] || Object.values(data)[0];
  return decryptSaavnMediaUrl(song?.encrypted_media_url);
}

async function getMp3Path(audioUrl) {
  const cachePath = join(cacheDir, `${createHash('sha256').update(audioUrl).digest('hex')}.mp3`);
  try {
    const cached = await stat(cachePath);
    if (Date.now() - cached.mtimeMs < cacheTtlMs) return cachePath;
    await rm(cachePath, { force: true });
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }

  const currentConversion = conversions.get(cachePath);
  if (currentConversion) return currentConversion;

  const conversion = (async () => {
    await mkdir(cacheDir, { recursive: true });
    const files = await readdir(cacheDir);
    await Promise.all(files.filter(file => file.endsWith('.mp3')).map(async file => {
      const path = join(cacheDir, file);
      const cached = await stat(path).catch(() => null);
      if (cached && Date.now() - cached.mtimeMs >= cacheTtlMs) {
        await rm(path, { force: true });
      }
    }));

    const upstream = await fetch(audioUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
        Referer: 'https://www.jiosaavn.com/',
      },
    });
    if (!upstream.ok || !upstream.body) {
      throw new Error(`Saavn audio request failed with HTTP ${upstream.status}`);
    }
    if (!ffmpegPath) throw new Error('FFmpeg is unavailable on this server');

    const tempPath = `${cachePath}.${process.pid}.${Date.now()}.tmp`;
    const ffmpeg = spawn(ffmpegPath, [
      '-hide_banner', '-loglevel', 'error',
      '-i', 'pipe:0',
      '-map', '0:a:0', '-vn',
      '-codec:a', 'libmp3lame', '-b:a', '192k',
      '-f', 'mp3', 'pipe:1',
    ]);
    let stderr = '';
    ffmpeg.stderr.on('data', chunk => {
      stderr = (stderr + chunk.toString()).slice(-4000);
    });
    const ffmpegExit = new Promise((resolve, reject) => {
      ffmpeg.once('error', reject);
      ffmpeg.once('close', code => {
        if (code === 0) resolve();
        else reject(new Error(`FFmpeg exited with code ${code}: ${stderr.trim()}`));
      });
    });

    try {
      await Promise.all([
        pipeline(Readable.fromWeb(upstream.body), ffmpeg.stdin),
        pipeline(ffmpeg.stdout, createWriteStream(tempPath)),
        ffmpegExit,
      ]);
      const converted = await stat(tempPath);
      if (!converted.size) throw new Error('FFmpeg returned an empty audio stream');
      await rename(tempPath, cachePath);
      return cachePath;
    } catch (error) {
      ffmpeg.kill();
      await rm(tempPath, { force: true });
      throw error;
    }
  })();

  conversions.set(cachePath, conversion);
  try {
    return await conversion;
  } finally {
    conversions.delete(cachePath);
  }
}

function sendMp3(req, res, filePath, size) {
  res.setHeader('Content-Type', 'audio/mpeg');
  res.setHeader('Accept-Ranges', 'bytes');
  res.setHeader('Cache-Control', 'private, max-age=3600');

  let start = 0;
  let end = size - 1;
  if (req.headers.range) {
    const match = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range);
    if (!match || (!match[1] && !match[2])) {
      res.setHeader('Content-Range', `bytes */${size}`);
      res.statusCode = 416;
      return res.end();
    }
    const suffixLength = Number(match[2]);
    start = match[1] ? Number(match[1]) : Math.max(0, size - suffixLength);
    end = match[2] && match[1] ? Math.min(Number(match[2]), size - 1) : size - 1;
    if (start >= size || start > end) {
      res.setHeader('Content-Range', `bytes */${size}`);
      res.statusCode = 416;
      return res.end();
    }
    res.setHeader('Content-Range', `bytes ${start}-${end}/${size}`);
    res.statusCode = 206;
  } else {
    res.statusCode = 200;
  }

  res.setHeader('Content-Length', end - start + 1);
  if (req.method === 'HEAD') return res.end();
  return createReadStream(filePath, { start, end }).pipe(res);
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!['GET', 'HEAD'].includes(req.method)) return res.status(405).end();

  let audioUrl = Array.isArray(req.query.url) ? req.query.url[0] : req.query.url;
  const trackId = Array.isArray(req.query.trackId) ? req.query.trackId[0] : req.query.trackId;
  if (!audioUrl && typeof trackId === 'string') {
    try {
      audioUrl = await resolveSaavnTrackUrl(trackId);
    } catch (error) {
      console.error('Saavn track lookup error:', error);
      return res.status(502).send('Track lookup failed');
    }
  }
  if (typeof audioUrl !== 'string' || !isSaavnUrl(audioUrl)) {
    return res.status(404).send('Track audio is unavailable');
  }

  try {
    const filePath = await getMp3Path(audioUrl);
    const file = await stat(filePath);
    return sendMp3(req, res, filePath, file.size);
  } catch (error) {
    console.error('Saavn stream conversion error:', error);
    return res.status(502).send('Audio conversion failed');
  }
}
