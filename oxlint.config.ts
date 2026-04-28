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
                'jest/no-disabled-tests': 'off',
            },
        },
    ],
});
