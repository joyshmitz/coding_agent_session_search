module.exports = {
  extends: 'lighthouse:default',
  settings: {
    throttlingMethod: 'simulate',
    throttling: {
      rttMs: 150,
      throughputKbps: 1600,
      cpuSlowdownMultiplier: 4
    },
    onlyCategories: ['performance'],
    formFactor: 'desktop'
  }
};
