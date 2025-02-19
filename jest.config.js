module.exports = {
  testEnvironment: 'node',
  collectCoverage: true,
  coverageReporters: ['clover', 'json', 'lcov', ['text', {skipFull: true}]],
  coverageThreshold: {
    global: {
      branches: 70,
      functions: 80,
      lines: 90,
      statements: 90,
    },
  },
  roots: ['<rootDir>/test'],
  testMatch: ['**/*.test.js'],
};
