const crypto = require('crypto');
const ow = require('ow');
const { REQUEST_ID_LENGTH } = require('./consts');
/**
 * Removes all properties with a null value
 * from the provided object.
 *
 * @param {object} [object]
 * @return {object}
 */
exports.purgeNullsFromObject = (object) => {
    if (object && typeof object === 'object' && !Array.isArray(object)) {
        Object.keys(object).forEach((k) => {
            if (object[k] === null) {
                delete object[k];
            }
        });
    }
    return object;
};

/**
 * Creates a standard request ID (same as Platform).
 * @param {string} uniqueKey
 * @return {string}
 */
exports.uniqueKeyToRequestId = (uniqueKey) => {
    const str = crypto
        .createHash('sha256')
        .update(uniqueKey)
        .digest('base64')
        .replace(/(\+|\/|=)/g, '');

    return str.length > REQUEST_ID_LENGTH ? str.substr(0, REQUEST_ID_LENGTH) : str;
};

/**
 * @param {*} value
 * @return {boolean}
 */
exports.isBuffer = (value) => ow.isValid(value, ow.any(ow.buffer, ow.arrayBuffer, ow.typedArray));

/**
 * @param {*} value
 * @return {boolean}
 */
exports.isStream = (value) => ow.isValid(value, ow.object.hasKeys('on', 'pipe'));
