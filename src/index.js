/**
 * LoopLive streaming worker
 */

import { spawn } from "node:child_process";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createClient } from "@supabase/supabase-js";
import ws from "ws";
import "dotenv/config";

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  USER_ID,
  STORAGE_BUCKET = "videos",
  POLL_INTERVAL_MS = "5000",
} = process.env;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !USER_ID) {
  console.error("Missing SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, or USER_ID");
  process.exit(1);
}

const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
    realtime: {
      transport: ws,
    },
  }
);

const log = (...a) => console.log(new Date().toISOString(), "-", ...a);

let ffmpeg = null;
let currentRunId = 0;
let workdir = null;

async function getStream() {
  const { data, error } = await supabase
    .from("streams")
    .select("*")
    .eq("user_id", USER_ID)
    .maybeSingle();

  if (error) throw error;
  return data;
}

async function setStatus(status, extra = {}) {
  await supabase
    .from("streams")
    .update({ status, ...extra })
    .eq("user_id", USER_ID);
}

async function getPlaylist() {
  const { data, error } = await supabase
    .from("videos")
    .select("storage_path, title, playlist_order")
    .eq("user_id", USER_ID)
    .order("playlist_order", { ascending: true });

  if (error) throw error;
  return data ?? [];
}

async function downloadPlaylist(videos) {
  workdir = await mkdtemp(path.join(tmpdir(), "looplive-"));
  const concatLines = [];

  for (let i = 0; i < videos.length; i++) {
    const v = videos[i];

    log(`Downloading [${i + 1}/${videos.length}] ${v.title}`);

    const { data, error } = await supabase.storage
      .from(STORAGE_BUCKET)
      .download(v.storage_path);

    if (error) {
      throw new Error(
        `Download failed for ${v.storage_path}: ${error.message}`
      );
    }

    const buf = Buffer.from(await data.arrayBuffer());

    const file = path.join(workdir, `clip_${i}.mp4`);

    await writeFile(file, buf);

    concatLines.push(`file '${file.replace(/'/g, "'\\''")}'`);
  }

  const listPath = path.join(workdir, "playlist.txt");

  await writeFile(listPath, concatLines.join("\n"));

  return listPath;
}

async function cleanup() {
  if (workdir) {
    try {
      await rm(workdir, { recursive: true, force: true });
    } catch {}

    workdir = null;
  }
}

function killFfmpeg() {
  if (ffmpeg) {
    log("Stopping FFmpeg...");

    try {
      ffmpeg.kill("SIGTERM");
    } catch {}

    ffmpeg = null;
  }
}

function spawnFfmpeg({ playlistPath, rtmpUrl, streamKey, loop }) {
  const args = [
    "-re",
    ...(loop ? ["-stream_loop", "-1"] : []),
    "-f",
    "concat",
    "-safe",
    "0",
    "-i",
    playlistPath,
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-tune",
    "zerolatency",
    "-pix_fmt",
    "yuv420p",
    "-r",
    "30",
    "-g",
    "60",
    "-b:v",
    "2500k",
    "-maxrate",
    "2500k",
    "-bufsize",
    "5000k",
    "-vf",
    "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    "-ar",
    "44100",
    "-ac",
    "2",
    "-f",
    "flv",
    `${rtmpUrl.replace(/\/$/, "")}/${streamKey}`,
  ];

  log("Spawning FFmpeg...");

  return spawn("ffmpeg", args, {
    stdio: ["ignore", "inherit", "inherit"],
  });
}

async function runStream(stream) {
  const runId = ++currentRunId;

  await setStatus("starting", {
    last_started_at: new Date().toISOString(),
    last_error: null,
  });

  const videos = await getPlaylist();

  if (!videos.length) {
    await setStatus("error", {
      last_error: "No videos in playlist",
    });

    return;
  }

  const playlistPath = await downloadPlaylist(videos);

  await setStatus("live");

  let backoff = 1000;

  while (runId === currentRunId) {
    await new Promise((resolve) => {
      ffmpeg = spawnFfmpeg({
        playlistPath,
        rtmpUrl: stream.rtmp_url,
        streamKey: stream.stream_key,
        loop: stream.loop_playlist,
      });

      ffmpeg.on("exit", (code, signal) => {
        log(`FFmpeg exited code=${code} signal=${signal}`);

        ffmpeg = null;

        resolve();
      });

      ffmpeg.on("error", (err) => {
        log("FFmpeg error:", err.message);
      });
    });

    if (runId !== currentRunId) break;

    if (!stream.auto_reconnect) {
      await setStatus("offline");
      break;
    }

    log(`Reconnecting in ${backoff}ms...`);

    await new Promise((r) => setTimeout(r, backoff));

    backoff = Math.min(backoff * 2, 30000);
  }

  await cleanup();
}

async function controlLoop() {
  let activeRun = null;
  let lastKey = null;

  while (true) {
    try {
      const stream = await getStream();

      if (!stream) {
        log("No stream row found for user", USER_ID);
      } else {
        const wantsLive =
          (stream.status === "starting" || stream.status === "live") &&
          !!stream.stream_key;

        const key = `${stream.stream_key}|${stream.rtmp_url}|${stream.loop_playlist}|${stream.auto_reconnect}`;

        if (wantsLive && (!activeRun || key !== lastKey)) {
          if (activeRun) {
            killFfmpeg();

            await activeRun.catch(() => {});

            await cleanup();
          }

          lastKey = key;

          activeRun = runStream(stream).catch(async (err) => {
            log("Run failed:", err.message);

            await setStatus("error", {
              last_error: err.message,
            });
          });
        } else if (!wantsLive && activeRun) {
          log("Status went offline; stopping...");

          currentRunId++;

          killFfmpeg();

          await activeRun.catch(() => {});

          activeRun = null;
          lastKey = null;

          await cleanup();
        }
      }
    } catch (err) {
      log("Control loop error:", err.message);
    }

    await new Promise((r) =>
      setTimeout(r, Number(POLL_INTERVAL_MS))
    );
  }
}

process.on("SIGTERM", async () => {
  log("SIGTERM received");

  currentRunId++;

  killFfmpeg();

  await cleanup();

  process.exit(0);
});

controlLoop();
