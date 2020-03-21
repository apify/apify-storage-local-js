const Database = require('better-sqlite3');
const path = require('path');
const ow = require('ow');
const RequestQueues = require('./request_queues');

/**
 * @typedef {object} ApifyStorageLocalOptions
 * @property {string} [dbDirectoryPath='./apify_storage']
 *  Path to directory where the database file will be created,
 *  unless either the inMemory option is true or the file
 *  already exists.
 * @property {string} [dbFilename='db.sqlite']
 *  Custom filename for your database. Useful when you want to
 *  keep multiple databases for any reason.
 * @property {boolean} [debug=false]
 *  Whether all SQL queries made by the database should be logged
 *  to the console.
 * @property {boolean} [inMemory=false]
 *  If true, the database will only exist in memory. This is useful
 *  for testing or for cases where persistence is not necessary,
 *  such as short running tasks where it may improve performance.
 */

class ApifyStorageLocal {
    /**
     * @param {ApifyStorageLocalOptions} options
     */
    constructor(options) {
        ow(options, ow.object.partialShape({
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

        this.dbFilePath = path.resolve(dbDirectoryPath, dbFilename);
        const dbOptions = { memory: inMemory };
        if (debug) dbOptions.verbose = this._logDebug;

        try {
            this.db = new Database(this.dbFilePath, dbOptions);
            // WAL mode should greatly improve performance
            // https://github.com/JoshuaWise/better-sqlite3/blob/master/docs/performance.md
            this.db.pragma('journal_mode = WAL');
            this.db.pragma('foreign_keys = ON');
        } catch (err) {
            throw new Error(`Connection to local database could not be established at ${this.dbFilePath}\nCause: ${err.message}`);
        }

        this.requestQueues = new RequestQueues(this.db);
    }

    /**
     * Closes all existing database connections.
     * Call close to gracefully exit when using
     * a file system database (memory: false).
     */
    closeDb() {
        this.db.close();
    }

    _logDebug(statement) {
        console.log(statement);
    }
}

module.exports = ApifyStorageLocal;
