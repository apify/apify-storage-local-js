const path = require('path');

module.exports = {
    verbose: true,
    // Switch to false for debugging
    silent: true,
    // rootDir: path.join(__dirname, './'),
    // testMatch: [path.join(__dirname, '**/test/?(*.)+(spec|test).[tj]s?(x)')],
    testEnvironment: 'node',
    testTimeout: 5000,
    collectCoverage: true,
    collectCoverageFrom: [
        '**/src/**/*.js',
        '!**/node_modules/**',
    ],
    maxWorkers: 3,
    globalSetup: path.join(__dirname, 'test', '_globalSetup.js'),
    globalTeardown: path.join(__dirname, 'test', '_globalTeardown.js'),
};
