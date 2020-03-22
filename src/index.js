const Database = require('better-sqlite3');
const fs = require('fs');
const ow = require('ow');
const path = require('path');
const RequestQueues = require('./request_queues');

/**
 * To enable high performance WAL mode, SQLite creates 2 more
 * files for performance optimizations.
 * @type {string[]}
 */
const DATABASE_FILE_SUFFIXES = ['-shm', '-wal'];

/**
 * @typedef {object} ApifyStorageLocalOptions
 * @property {string} [dbDirectoryPath='./apify_storage']
 *  Path to directory where the database file will be created,
 *  unless either the inMemory option is true or the file
 *  already exists.
 * @property {string} [dbFilename='db.sqlite']
 *  Custom filename for your database. Useful when you want to
 *  keep multiple databases for any reason. Note that 2 other
 *  files are created by the database that enable higher performance.
 * @property {boolean} [debug=false]
 *  Whether all SQL queries made by the database should be logged
 *  to the console.
 * @property {boolean} [inMemory=false]
 *  If true, the database will only exist in memory. This is useful
 *  for testing or for cases where persistence is not necessary,
 *  such as short running tasks where it may improve performance.
 */

/**
 * Represents local emulation of [Apify Storage](https://apify.com/storage).
 * Only Request Queue emulation is currently supported.
 */
class ApifyStorageLocal {
    /**
     * @param {ApifyStorageLocalOptions} [options]
     */
    constructor(options) {
        ow(options, 'ApifyStorageLocalOptions', ow.optional.object.partialShape({
            dbDirectoryPath: ow.optional.string,
            dbFilename: ow.optional.string,
            debug: ow.optional.boolean,
            inMemory: ow.optional.boolean,
        }));

        const {
            dbDirectoryPath = './apify_storage',
            dbFilename = 'db.sqlite',
            debug = false,
            inMemory = false,
        } = options;

        this.dbFilePath = inMemory
            ? ':memory:'
            : path.resolve(dbDirectoryPath, dbFilename);
        this.inMemory = inMemory;
        this.debug = debug;
        this.connectDatabase();
    }

    /**
     * Connects to an existing database, or creates a new one.
     * It's called automatically when {@link ApifyStorageLocal} instance
     * is constructed. Calling manually is useful after you call
     * {@link ApifyStorageLocal#dropDatabase} to get a clean slate.
     */
    connectDatabase() {
        const dbOptions = {};
        if (this.debug) dbOptions.verbose = this._logDebug;
        try {
            this.db = new Database(this.dbFilePath, dbOptions);
        } catch (err) {
            throw new Error(`Connection to local database could not be established at ${this.dbFilePath}\nCause: ${err.message}`);
        }
        // WAL mode should greatly improve performance
        // https://github.com/JoshuaWise/better-sqlite3/blob/master/docs/performance.md
        this.db.pragma('journal_mode = WAL');
        this.db.pragma('foreign_keys = ON');
        this.requestQueues = new RequestQueues(this.db);
    }

    /**
     * Closes database connection and keeps the data when using
     * a file system database. In memory data are lost. Should
     * be called at the end of use to allow the process to exit
     * gracefully. No further database operations will be executed.
     *
     * Call {@link ApifyStorageLocal#connectDatabase} or create a new
     * {@link ApifyStorageLocal} instance to get a new database connection.
     */
    closeDatabase() {
        this.db.close();
    }

    /**
     * Closes the database connection and removes all data.
     * With file system databases, it deletes the database file.
     * No further database operations will be executed.
     *
     * Call {@link ApifyStorageLocal#connectDatabase} or create a new
     * {@link ApifyStorageLocal} instance to create a new database.
     */
    dropDatabase() {
        this.db.close();
        if (this.inMemory) return;
        fs.unlinkSync(this.dbFilePath);

        // It seems that the extra 2 files are automatically deleted
        // when the original file is deleted, but I'm not sure if
        // this applies to all OSs.
        DATABASE_FILE_SUFFIXES.forEach((suffix) => {
            try {
                fs.unlinkSync(`${this.dbFilePath}${suffix}`);
            } catch (err) {
                if (err.code !== 'ENOENT') throw err;
            }
        });
    }

    _logDebug(statement) {
        console.log(statement);
    }
}
ApifyStorageLocal.DATABASE_FILE_SUFFIXES = DATABASE_FILE_SUFFIXES;
module.exports = ApifyStorageLocal;
