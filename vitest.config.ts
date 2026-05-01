import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        globals: true,
        restoreMocks: true,
        silent: true,
        testTimeout: 10_000,
        hookTimeout: 10_000,
        globalSetup: './test/_globalSetup.ts',
        globalTeardown: './test/_globalTeardown.ts',
    },
});
