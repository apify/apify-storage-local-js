import { removeSync } from 'fs-extra';
import { TEMP_DIR } from './_tools';

module.exports = () => {
    removeSync(TEMP_DIR);
};
