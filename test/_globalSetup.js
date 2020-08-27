const fs = require('fs-extra');
const { TEMP_DIR } = require('./_tools');

module.exports = () => {
    fs.ensureDirSync(TEMP_DIR);
    fs.emptyDirSync(TEMP_DIR);
};
