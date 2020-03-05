const path = require('path');
const { promisify } = require('util');
const sqlite3 = require('sqlite3');
const RequestQueues = require('./request_queues');

const resourceClasses = {
    requestQueues: RequestQueues,
};

class ApifyClientLocal {
    constructor(options = {}) {
        const {
            dbPath = './apify_storage/local.db',
            debug = false,
            inMemory = false,
        } = options;

        this.dbFilePath = inMemory
            ? ':memory:'
            : path.resolve(dbPath);

        const sqlite = debug ? sqlite3.verbose() : sqlite3;
        this.db = new sqlite.Database(this.dbFilePath, (err) => {
            if (err) {
                throw new Error(`Connection to local database could not be established at ${this.dbFilePath}\nCause: ${err.message}`);
            }
        });

        // Create instances of individual resources
        Object.entries(resourceClasses).forEach(([name, ResourceClass]) => {
            this[name] = new ResourceClass(this.db);
        });
    }

    async destroy() {
        const close = promisify(this.db.close.bind(this.db));
        await close();
    }
}

module.exports = ApifyClientLocal;
