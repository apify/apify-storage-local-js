import { defineConfig } from '@apify/oxlint-config';

export default defineConfig({
    ignorePatterns: ['**/node_modules', 'dist', 'coverage'],
    rules: {
        'typescript/no-explicit-any': 'off',
        'import/extensions': 'off',
    },
    overrides: [
        {
            files: ['*.config.ts', 'jest.config.ts', '.github/scripts/**'],
            rules: {
                'no-console': 'off',
                'import/no-default-export': 'off',
            },
        },
        {
            files: ['test/**'],
            rules: {
                // Tests use the `try { ... } catch (err) { expect(err)... }` pattern. Migrating
                // to `await expect(...).rejects.toX(...)` is out of scope for the lint migration.
                'jest/no-conditional-expect': 'off',
                'vitest/no-conditional-expect': 'off',
                // Some assertions live in helper functions (e.g. testTaxNumber) and a handful of
                // intentionally-empty `test.skip(...)` placeholders trip the rule. Too noisy to
                // enforce in this repo.
                'jest/expect-expect': 'off',
                'vitest/expect-expect': 'off',
                // Skipped tests are kept intentionally as TODOs for the request-queue v2 work.
                'jest/no-disabled-tests': 'off',
                'vitest/no-disabled-tests': 'off',
                // Test files export shared helper types (e.g. `TestQueue` interface).
                'jest/no-export': 'off',
            },
        },
    ],
});
