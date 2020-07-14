const Database = require('better-sqlite3');
const fs = require('fs-extra');
const ApifyStorageLocal = require('../src/index');
const { prepareTestDir, removeTestDir } = require('./_tools');

let STORAGE_DIR;
beforeEach(() => {
    STORAGE_DIR = prepareTestDir('index');
});

afterAll(() => {
    removeTestDir('index');
});


