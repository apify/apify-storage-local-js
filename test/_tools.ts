import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';

export const TEMP_DIR = join(import.meta.dirname, 'tmp');

export function prepareTestDir(): string {
    const name = Math.random().toString(36).substring(2, 10) + Math.random().toString(36).substring(2, 10);
    const dir = join(TEMP_DIR, name);
    mkdirSync(dir, { recursive: true });
    return dir;
}

export function removeTestDir(name: string): void {
    const dir = join(TEMP_DIR, name);
    rmSync(dir, { recursive: true, force: true });
}
