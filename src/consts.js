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
