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
 * Names of all tables used in the database.
 * @type {Object}
 */
exports.TABLE_NAMES = {
    REQUEST_QUEUES: 'request_queues',
    REQUEST_QUEUE_REQUESTS: 'request_queue_requests',
};
