const fs = require('fs-extra');
const path = require('path');

exports.TEMP_DIR = path.join(__dirname, 'tmp');

exports.prepareTestDir = () => {
    const name = Math.random().toString(36).substring(2, 10) + Math.random().toString(36).substring(2, 10);
    const dir = path.join(exports.TEMP_DIR, name);
    fs.ensureDirSync(dir);
    fs.emptyDirSync(dir);
    return dir;
};

exports.removeTestDir = (name) => {
    const dir = path.join(exports.TEMP_DIR, name);
    fs.removeSync(dir);
};
