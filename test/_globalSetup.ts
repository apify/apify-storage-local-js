import { emptyDirSync, ensureDirSync } from 'fs-extra';
import { TEMP_DIR } from './_tools';

module.exports = () => {
    ensureDirSync(TEMP_DIR);
    emptyDirSync(TEMP_DIR);
};
