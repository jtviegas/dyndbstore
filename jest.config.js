module.exports = {
  testEnvironment: 'node',
  collectCoverage: true,
  coverageReporters: ['clover', 'json', 'lcov', ['text', {skipFull: true}]],
  coverageThreshold: {
    global: {
      branches: 50,
      functions: 85,
      lines: 90,
      statements: 90,
    },
  },
  roots: ['<rootDir>/test'],
  testMatch: ['**/*.test.js'],
};
