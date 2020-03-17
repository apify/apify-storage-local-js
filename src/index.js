const Database = require('better-sqlite3');
const path = require('path');
const ow = require('ow');
const RequestQueues = require('./request_queues');

/**
 * @typedef {object} ApifyClientLocalOptions
 * @property {string} [dbPath='./apify_storage/local.db']
 *  Path to the database file. If it doesn't exist, it will be created
 *  unless the memory option is true.
 * @property {boolean} [debug=false]
 *  Whether all SQL queries made by the database should be logged
 *  to the console.
 * @property {boolean} [memory=false]
 *  If true, the database will only exist in memory. This is useful
 *  for testing or for cases where persistence is not necessary,
 *  such as short running tasks where it may improve performance.
 */
const apifyClientLocalOptions = ow.object.partialShape({
    dbPath: ow.optional.string,
    debug: ow.optional.boolean,
    memory: ow.optional.boolean,
});

class ApifyClientLocal {
    /**
     * @param {ApifyClientLocalOptions} options
     */
    constructor(options) {
        ow(options, apifyClientLocalOptions);
        const {
            dbPath = './apify_storage/local.db',
            debug = false,
            memory = false,
        } = options;

        this.dbFilePath = path.resolve(dbPath);
        const dbOptions = { memory };
        if (debug) dbOptions.verbose = this._logDebug;

        try {
            this.db = new Database(this.dbFilePath, dbOptions);
            // WAL mode should greatly improve performance
            // https://github.com/JoshuaWise/better-sqlite3/blob/master/docs/performance.md
            this.db.pragma('journal_mode = WAL');
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
    close() {
        this.db.close();
    }

    _logDebug(statement) {
        console.log(statement);
    }
}

module.exports = ApifyClientLocal;
