import { FastifyInstance } from 'fastify';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, readdir, rename, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import ffmpegPath from 'ffmpeg-static';
import { getStreamUrl } from '../services/youtubeMusic.js';

const audioCacheDir = join(tmpdir(), 'eva-music-audio');
const audioCacheTtlMs = 12 * 60 * 60 * 1000;
const conversions = new Map<string, Promise<string>>();

async function pruneAudioCache(): Promise<void> {
  const files = await readdir(audioCacheDir).catch((err: NodeJS.ErrnoException) => {
    if (err.code === 'ENOENT') return [];
    throw err;
  });
  await Promise.all(files.filter(file => file.endsWith('.mp3')).map(async file => {
    const path = join(audioCacheDir, file);
    const cached = await stat(path).catch(() => null);
    if (cached && Date.now() - cached.mtimeMs >= audioCacheTtlMs) {
      await rm(path, { force: true });
    }
  }));
}

function isSaavnUrl(url: string): boolean {
  try {
    const parsed = new URL(url);
    const hostname = parsed.hostname.toLowerCase();
    return parsed.protocol === 'https:' && (hostname === 'saavncdn.com' || hostname.endsWith('.saavncdn.com'));
  } catch {
    return false;
  }
}

async function createMp3Cache(audioUrl: string): Promise<string> {
  const cachePath = join(audioCacheDir, `${createHash('sha256').update(audioUrl).digest('hex')}.mp3`);
  try {
    const cached = await stat(cachePath);
    if (Date.now() - cached.mtimeMs < audioCacheTtlMs) return cachePath;
    await rm(cachePath, { force: true });
  } catch (err: any) {
    if (err?.code !== 'ENOENT') throw err;
  }

  const currentConversion = conversions.get(cachePath);
  if (currentConversion) return currentConversion;

  const conversion = (async () => {
    await mkdir(audioCacheDir, { recursive: true });
    await pruneAudioCache();
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
    ffmpeg.stderr?.on('data', chunk => {
      stderr = (stderr + chunk.toString()).slice(-4000);
    });
    const ffmpegExit = new Promise<void>((resolve, reject) => {
      ffmpeg.once('error', reject);
      ffmpeg.once('close', code => {
        if (code === 0) resolve();
        else reject(new Error(`FFmpeg exited with code ${code}: ${stderr.trim()}`));
      });
    });

    try {
      await Promise.all([
        pipeline(Readable.fromWeb(upstream.body as any), ffmpeg.stdin),
        pipeline(ffmpeg.stdout, createWriteStream(tempPath)),
        ffmpegExit,
      ]);
      const converted = await stat(tempPath);
      if (converted.size === 0) throw new Error('FFmpeg returned an empty audio stream');
      await rename(tempPath, cachePath);
      return cachePath;
    } catch (err) {
      ffmpeg.kill();
      await rm(tempPath, { force: true });
      throw err;
    }
  })();

  conversions.set(cachePath, conversion);
  try {
    return await conversion;
  } finally {
    conversions.delete(cachePath);
  }
}

async function sendMp3(request: any, reply: any, audioUrl: string) {
  const filePath = await createMp3Cache(audioUrl);
  const file = await stat(filePath);
  const range = request.headers.range as string | undefined;
  reply.header('Content-Type', 'audio/mpeg');
  reply.header('Accept-Ranges', 'bytes');
  reply.header('Cache-Control', 'private, max-age=3600');

  if (!range) {
    reply.header('Content-Length', file.size);
    if (request.method === 'HEAD') return reply.code(200).send();
    return reply.code(200).send(createReadStream(filePath));
  }

  const match = /^bytes=(\d*)-(\d*)$/.exec(range);
  if (!match || (!match[1] && !match[2])) {
    return reply.header('Content-Range', `bytes */${file.size}`).code(416).send();
  }
  const suffixLength = Number(match[2]);
  const start = match[1] ? Number(match[1]) : Math.max(0, file.size - suffixLength);
  const end = match[2] && match[1] ? Math.min(Number(match[2]), file.size - 1) : file.size - 1;
  if (start >= file.size || start > end) {
    return reply.header('Content-Range', `bytes */${file.size}`).code(416).send();
  }

  reply.header('Content-Range', `bytes ${start}-${end}/${file.size}`);
  reply.header('Content-Length', end - start + 1);
  reply.code(206);
  if (request.method === 'HEAD') return reply.send();
  return reply.send(createReadStream(filePath, { start, end }));
}

async function handleStreamReply(request: any, reply: any, trackId?: string, directUrl?: string) {
  const resolvedUrl = directUrl || (trackId ? await getStreamUrl(trackId) : null);

  if (!resolvedUrl) {
    return reply.code(404).send({ error: 'Audio stream unavailable for this track' });
  }
  reply.header('Access-Control-Allow-Origin', '*');
  reply.header('Access-Control-Allow-Headers', '*');
  reply.header('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
  if (isSaavnUrl(resolvedUrl)) {
    try {
      return await sendMp3(request, reply, resolvedUrl);
    } catch (err: any) {
      request.log.error({ err, trackId }, 'Saavn audio conversion failed');
      return reply.code(502).send({ error: 'Audio conversion failed' });
    }
  }

  reply.header('Accept-Ranges', 'bytes');

  if (request.method === 'HEAD') {
    reply.header('Content-Type', 'audio/mp4');
    return reply.code(200).send();
  }

  const range = request.headers.range;
  let streamHostname: string;
  try {
    streamHostname = new URL(resolvedUrl).hostname.toLowerCase();
  } catch {
    return reply.code(502).send({ error: 'Resolved audio URL is invalid' });
  }
  const isSaavnStream = streamHostname === 'saavncdn.com' || streamHostname.endsWith('.saavncdn.com');
  const upstreamHeaders: Record<string, string> = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
  };
  if (isSaavnStream) {
    upstreamHeaders.Referer = 'https://www.jiosaavn.com/';
  }
  if (range) {
    upstreamHeaders['Range'] = range;
  }

  try {
    const upstream = await fetch(resolvedUrl, { headers: upstreamHeaders });

    const contentType = upstream.headers.get('content-type') ?? 'audio/mp4';
    const contentRange = upstream.headers.get('content-range');
    const contentLength = upstream.headers.get('content-length');

    reply.header('Content-Type', contentType);
    if (contentRange) reply.header('Content-Range', contentRange);
    if (contentLength) reply.header('Content-Length', contentLength);

    reply.code(upstream.status);
    if (upstream.body) {
      const stream = Readable.fromWeb(upstream.body as any);
      return reply.send(stream);
    }
    return reply.redirect(resolvedUrl, 302);
  } catch (err: any) {
    console.error(`[stream] proxy fetch error for ${trackId || 'direct-url'}:`, err);
    return reply.redirect(resolvedUrl, 302);
  }
}

export async function streamRoutes(fastify: FastifyInstance) {
  fastify.route({
    method: ['GET', 'HEAD'],
    url: '/api/stream',
    handler: async (request, reply) => {
      const { url, trackId } = request.query as { url?: string; trackId?: string };
      const directUrl = typeof url === 'string' ? url : undefined;

      if (directUrl && isSaavnUrl(directUrl)) {
        return handleStreamReply(request, reply, undefined, directUrl);
      }
      if (directUrl) return reply.code(400).send({ error: 'Only Saavn audio URLs are supported' });

      if (!trackId) {
        return reply.code(400).send({ error: 'trackId or url is required' });
      }

      return handleStreamReply(request, reply, trackId, undefined);
    },
  });

  fastify.route({
    method: ['GET', 'HEAD'],
    url: '/api/stream/:trackId',
    handler: async (request, reply) => {
      const { trackId } = request.params as { trackId: string };

      if (!trackId) {
        return reply.code(400).send({ error: 'trackId is required' });
      }

      return handleStreamReply(request, reply, trackId, undefined);
    },
  });
}
