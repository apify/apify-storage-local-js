/**
 * Length of id property of a Request instance in characters.
 * @type {number}
 */
exports.REQUEST_ID_LENGTH = 15;

/**
 * SQL that produces a timestamp in the correct format.
 * @type {string}
 */
exports.TIMESTAMP_SQL = "STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'NOW')";

/**
 * Names of all emulated storages.
 * @type {{REQUEST_QUEUES: string, KEY_VALUE_STORES: string, REQUEST_QUEUE_REQUESTS: string, DATASETS: string}}
 */
exports.STORAGE_NAMES = {
    REQUEST_QUEUES: 'request_queues',
    KEY_VALUE_STORES: 'key_value_stores',
    DATASETS: 'datasets',
};

/**
 * Name of the request queue master database file.
 * @type {string}
 */
exports.DATABASE_FILE_NAME = 'db.sqlite';

/**
 * To enable high performance WAL mode, SQLite creates 2 more
 * files for performance optimizations.
 * @type {string[]}
 */
exports.DATABASE_FILE_SUFFIXES = ['-shm', '-wal'];

/**
 * Except in dataset items, the default limit for API results is 1000.
 * @type {number}
 */
exports.DEFAULT_API_PARAM_LIMIT = 1000;
