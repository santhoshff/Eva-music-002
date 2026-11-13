import { Readable } from 'node:stream';
import CryptoJS from 'crypto-js';

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
    if (!url.startsWith('https://')) return null;
    return url.replace('_96.mp4', '_320.mp4').replace('_160.mp4', '_320.mp4');
  } catch {
    return null;
  }
}

function isSaavnCdnUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' &&
      (url.hostname === 'saavncdn.com' || url.hostname.endsWith('.saavncdn.com'));
  } catch {
    return false;
  }
}

async function resolveTrackUrl(trackId) {
  const match = /^saavn-([A-Za-z0-9_-]+)$/.exec(trackId);
  if (!match) return null;

  const endpoint = new URL('https://www.jiosaavn.com/api.php');
  endpoint.search = new URLSearchParams({
    __call: 'song.getDetails',
    _format: 'json',
    cc: 'in',
    _marker: '0',
    pids: match[1],
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
  const song = data[match[1]] || Object.values(data).find(item => item?.encrypted_media_url);
  return decryptSaavnMediaUrl(song?.encrypted_media_url);
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Expose-Headers', 'Accept-Ranges, Content-Range, Content-Length');
  res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');

  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    return res.status(405).send('Method not allowed');
  }

  const queryValue = value => Array.isArray(value) ? value[0] : value;
  const trackId = queryValue(req.query.trackId);
  let audioUrl = queryValue(req.query.url);
  if (!audioUrl && typeof trackId === 'string') {
    try {
      audioUrl = await resolveTrackUrl(trackId);
    } catch (error) {
      console.error('Saavn track lookup error:', error);
      return res.status(502).send('Track lookup failed');
    }
  }
  if (typeof audioUrl !== 'string' || !isSaavnCdnUrl(audioUrl)) {
    return res.status(404).send('Track audio is unavailable');
  }

  try {
    const upstreamHeaders = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Referer': 'https://www.jiosaavn.com/',
    };
    if (req.headers.range) {
      upstreamHeaders['Range'] = req.headers.range;
    }

    const upstream = await fetch(audioUrl, { headers: upstreamHeaders });

    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Content-Type', upstream.headers.get('content-type') || 'audio/mp4');
    res.setHeader('Cache-Control', 'no-store');

    const contentRange = upstream.headers.get('content-range');
    if (contentRange) res.setHeader('Content-Range', contentRange);

    const contentLength = upstream.headers.get('content-length');
    if (contentLength) res.setHeader('Content-Length', contentLength);

    res.status(upstream.status);

    if (req.method === 'HEAD') {
      return res.end();
    }

    if (upstream.body) {
      const nodeStream = Readable.fromWeb(upstream.body);
      nodeStream.pipe(res);
    } else {
      res.end();
    }
  } catch (err) {
    console.error('Stream proxy error:', err);
    return res.status(502).send('Audio stream unavailable');
  }
}
