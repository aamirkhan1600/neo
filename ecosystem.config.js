// PM2 process manifest. App in cluster mode + dedicated worker process.
module.exports = {
  apps: [
    {
      name: 'kotak-neo-app',
      script: 'src/app.js',
      instances: 'max',
      exec_mode: 'cluster',
      max_memory_restart: '500M',
      env: { NODE_ENV: 'production' },
    },
    {
      name: 'kotak-neo-worker',
      script: 'src/worker.js',
      instances: 2,
      exec_mode: 'fork',
      max_memory_restart: '400M',
      env: { NODE_ENV: 'production' },
      // Each worker reads WORKER_ID from process.env; PM2 instance id appended for uniqueness.
    },
  ],
};
