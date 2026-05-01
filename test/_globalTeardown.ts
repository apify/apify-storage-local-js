import { rmSync } from 'node:fs';
import { TEMP_DIR } from './_tools';

export default function globalTeardown(): void {
    rmSync(TEMP_DIR, { recursive: true, force: true });
}
