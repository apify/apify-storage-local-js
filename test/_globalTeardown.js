const fs = require('fs-extra');
const { TEMP_DIR } = require('./_tools');

module.exports = () => {
    fs.removeSync(TEMP_DIR);
};
