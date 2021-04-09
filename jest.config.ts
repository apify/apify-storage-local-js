import { join } from 'path';
import type { Config } from '@jest/types';

export default async (): Promise<Config.InitialOptions> => ({
    verbose: true,
    // Switch to false for debugging
    silent: true,
    // rootDir: join(__dirname, './'),
    // testMatch: [join(__dirname, '**/test/?(*.)+(spec|test).[tj]s?(x)')],
    preset: 'ts-jest',
    testEnvironment: 'node',
    testRunner: 'jest-circus/runner',
    testTimeout: 5000,
    collectCoverage: true,
    collectCoverageFrom: [
        '**/src/**/*.ts',
        '**/src/**/*.js',
        '!**/node_modules/**',
    ],
    maxWorkers: 3,
    globalSetup: join(__dirname, 'test', '_globalSetup.ts'),
    globalTeardown: join(__dirname, 'test', '_globalTeardown.ts'),
    globals: {
        'ts-jest': {
            tsconfig: '<rootDir>/test/tsconfig.json',
        },
    },
});
