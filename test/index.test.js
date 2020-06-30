const Database = require('better-sqlite3');
const fs = require('fs-extra');
const path = require('path');
const ApifyStorageLocal = require('../src/index');

const TEMP_DIR = path.join(__dirname, 'tmp');

beforeAll(() => {
    fs.ensureDirSync(TEMP_DIR);
});

afterAll(() => {
    fs.removeSync(TEMP_DIR);
});

afterEach(() => {
    fs.emptyDirSync(TEMP_DIR);
});

test('creates database in memory', () => {
    const storage = new ApifyStorageLocal({
        inMemory: true,
    });
    expect(storage.db).toBeInstanceOf(Database);
    expect(fs.readdirSync(TEMP_DIR)).toHaveLength(0);
    storage.closeDatabase();
});

test('creates database in file', () => {
    const storage = new ApifyStorageLocal({
        storageDir: TEMP_DIR,
    });
    const dbFile = 'db.sqlite';
    expect(storage.db).toBeInstanceOf(Database);
    expect(fs.readdirSync(TEMP_DIR)).toEqual([
        dbFile,
        ...ApifyStorageLocal.DATABASE_FILE_SUFFIXES.map(sfx => `${dbFile}${sfx}`),
    ]);
    storage.dropDatabase();
});

test('dropDatabase removes database files', () => {
    const storage = new ApifyStorageLocal({
        storageDir: TEMP_DIR,
    });

    storage.dropDatabase();
    expect(fs.readdirSync(TEMP_DIR)).toHaveLength(0);
});
