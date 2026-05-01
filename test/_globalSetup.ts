import { mkdirSync, rmSync } from 'node:fs';
import { TEMP_DIR } from './_tools';

export default function globalSetup(): void {
    rmSync(TEMP_DIR, { recursive: true, force: true });
    mkdirSync(TEMP_DIR, { recursive: true });
}
