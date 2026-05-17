// Worker process — polls job_queue, claims jobs via an atomic UPDATE token,
// dispatches to per-type handlers. Run as a separate Node.js process
// (PM2: `kotak-neo-worker`). Multiple workers can run concurrently.

const config = require('./config');
const logger = require('./utils/logger');
const jobQueue = require('./services/jobQueue');
const brokerService = require('./services/brokerService');
const handlers = require('./workers/handlers');
const heartbeat = require('./services/workerHeartbeat');
const { close: closeDb } = require('./db/pool');

const WORKER_ID = `${config.worker.id}-${process.pid}`;

let running = true;
let activeJobs = 0;

async function processJob(job) {
  const start = Date.now();
  const handler = handlers[job.job_type];
  if (!handler) {
    await jobQueue.fail(job.id, new Error(`no_handler_for_${job.job_type}`));
    return;
  }
  try {
    const result = await handler(job);
    await jobQueue.complete(job.id, result);
    logger.info('job done', {
      id: job.id, type: job.job_type, ms: Date.now() - start, userId: job.user_id,
    });
  } catch (err) {
    if (err instanceof brokerService.RateLimitedError) {
      const seconds = Math.max(1, Math.ceil(err.waitMs / 1000));
      await jobQueue.reschedule(job.id, seconds);
      logger.warn('job rate-limited, rescheduled', { id: job.id, seconds });
      return;
    }
    if (err.isAuth) {
      // Session expired — mark this attempt failed; brokerService also
      // schedules a RELOGIN job. Other queued jobs will fail similarly until
      // the user re-authenticates.
      await jobQueue.fail(job.id, err, { rescheduleSeconds: 60 });
      return;
    }
    logger.error('job error', {
      id: job.id, type: job.job_type, err: err.message, stack: err.stack,
    });
    await jobQueue.fail(job.id, err);
  }
}

async function loop() {
  logger.info('worker started', { id: WORKER_ID, poll: config.worker.pollMs });

  // Liveness pings + periodic stale-lock reaper.
  heartbeat.start(WORKER_ID, { pid: process.pid, host: require('os').hostname() });
  setInterval(() => {
    jobQueue.reapStaleLocks().catch(err => logger.error('reap failed', { err: err.message }));
  }, 30000).unref();

  while (running) {
    try {
      const jobs = await jobQueue.claim(WORKER_ID);
      if (!jobs.length) {
        await new Promise(r => setTimeout(r, config.worker.pollMs));
        continue;
      }
      // Process claimed jobs in parallel — they're isolated by job id.
      await Promise.all(jobs.map(async (j) => {
        activeJobs++;
        try { await processJob(j); }
        finally { activeJobs--; }
      }));
    } catch (err) {
      logger.error('worker loop error', { err: err.message, stack: err.stack });
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

async function shutdown(signal) {
  logger.info(`worker received ${signal}, draining`, { id: WORKER_ID, activeJobs });
  running = false;
  // Wait up to 15s for in-flight jobs to settle.
  const deadline = Date.now() + 15000;
  while (activeJobs > 0 && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 200));
  }
  try { await closeDb(); } catch {}
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('uncaughtException', (err) => {
  logger.error('uncaughtException', { err: err.message, stack: err.stack });
});
process.on('unhandledRejection', (err) => {
  logger.error('unhandledRejection', { err: err && err.message });
});

loop().catch((err) => {
  logger.error('worker crashed', { err: err.message, stack: err.stack });
  process.exit(1);
});
