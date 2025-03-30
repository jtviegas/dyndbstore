module.exports = {
  testEnvironment: 'node',
  collectCoverage: true,
  coverageReporters: ['clover', 'json', 'lcov', ['text', {skipFull: true}]],
  coverageThreshold: {
    global: {
      branches: 70,
      functions: 65,
      lines: 75,
      statements: 75,
    },
  },
  roots: ['<rootDir>/test'],
  testMatch: ['**/*.test.js'],
};
