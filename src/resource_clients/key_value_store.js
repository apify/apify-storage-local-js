const ow = require('ow');

/**
 * Key-value Store client.
 *
 * @property {KeyValueStoreEmulator} emulator
 */
class KeyValueStoreClient {
    /**
     * @param {object} options
     * @param {RequestQueueEmulator} options.emulator
     * @param {string} options.id
     */
    constructor(options) {
        const {
            emulator,
            id,
        } = options;

        this.id = id;
        this.emulator = emulator;
    }

    async listKeys(options = {}) {
        ow(options, ow.object.exactShape({
            limit: ow.optional.number,
            exclusiveStartKey: ow.optional.string,
            desc: ow.optional.boolean,
        }));

        return {
            count: 2,
            limit: 2,
            exclusiveStartKey: null,
            isTruncated: true,
            nextExclusiveStartKey: 'ee5834b388b46c79df0f3e4252906bbc',
            items: [
                {
                    key: 'ece38627826b419d88ccdc934b061d56',
                    size: 12204,
                },
            ],

        };
    }

    async getValue(key, options = {}) {
        ow(key, ow.string);
        ow(options, ow.object.exactShape({
            buffer: ow.optional.boolean,
            stream: ow.optional.boolean,
            // This option is ignored, but kept here
            // for validation consistency with API client.
            disableRedirect: ow.optional.boolean,
        }));
    }

    async setValue(key, value, options = {}) {
        ow(key, ow.string);
        // value can be anything
        ow(options, ow.object.exactShape({
            contentType: ow.optional.string,
        }));
    }

    async deleteValue(key) {
        ow(key, ow.string);
    }
}

module.exports = KeyValueStoreClient;
