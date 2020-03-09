const { promisify } = require('util');
const { promisifyDbRun } = require('../src/utils');

const RESOURCE_TABLE_NAME = 'RequestQueues';
const REQUESTS_TABLE_NAME = 'RequestQueueRequests';

class RequestQueues {
    /**
     * @param {Database} db
     */
    constructor(db) {
        this.db = db;
        this._run = promisifyDbRun(this.db);
        this._get = promisify(this.db.get.bind(this.db));
        this.initPromise = this._initialize();
    }

    async getQueue({ queueId }) {
        await this.initPromise;
        const queue = await this._get(`
            SELECT * FROM ${RESOURCE_TABLE_NAME}
            WHERE id = ${queueId}
        `);
        return queue || null;
    }

    async getOrCreateQueue({ queueName }) {
        await this.initPromise;
        const query = typeof queueName === 'number'
            ? `id=${queueName}`
            : `name='${queueName}'`;
        const queue = await this._get(`
            SELECT * FROM ${RESOURCE_TABLE_NAME}
            WHERE ${query}
        `);
        if (queue) return queue;
        const queueId = await new Promise((resolve, reject) => {
            this.db.run(`
                INSERT INTO ${RESOURCE_TABLE_NAME}(name)
                VALUES('${queueName}')
            `, function (err) { // eslint-disable-line prefer-arrow-callback
                if (err) return reject(err);
                resolve(this.lastID);
            });
        });
        return this.getQueue({ queueId });
    }

    async deleteQueue() {

    }

    async getRequest() {

    }

    async updateRequest() {

    }

    async getHead() {

    }

    async _initialize() {
        const queues = this._run(`
            CREATE TABLE IF NOT EXISTS ${RESOURCE_TABLE_NAME}(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                createdAt TEXT DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
                modifiedAt TEXT,
                accessedAt TEXT,
                totalRequestCount INTEGER DEFAULT 0,
                handledRequestCount INTEGER DEFAULT 0,
                pendingRequestCount INTEGER DEFAULT 0
            )
        `);
        const requests = this._run(`
            CREATE TABLE IF NOT EXISTS ${REQUESTS_TABLE_NAME}(
                requestId TEXT NOT NULL,
                queueId INTEGER NOT NULL,
                url TEXT NOT NULL,
                uniqueKey TEXT UNIQUE NOT NULL,
                json TEXT NOT NULL,
                PRIMARY KEY (queueId, requestId)
            )
        `);
        await Promise.all([queues, requests]);
    }
}
RequestQueues.RESOURCE_TABLE_NAME = RESOURCE_TABLE_NAME;
RequestQueues.REQUESTS_TABLE_NAME = REQUESTS_TABLE_NAME;
module.exports = RequestQueues;
