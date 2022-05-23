import { createHash } from 'crypto';
import ow from 'ow';
import { REQUEST_ID_LENGTH } from './consts';
import { RawQueueTableData, RequestQueueInfo } from './emulators/request_queue_emulator';

/**
 * Removes all properties with a null value
 * from the provided object.
 */
export function purgeNullsFromObject<T>(object: T): T {
    if (object && typeof object === 'object' && !Array.isArray(object)) {
        for (const [key, value] of Object.entries(object)) {
            if (value === null) Reflect.deleteProperty(object as Record<string, unknown>, key);
        }
    }

    return object;
}

/**
 * Converts date strings to date objects and adds `id` alias for `name`.
 */
export function mapRawDataToRequestQueueInfo(raw?: RawQueueTableData): RequestQueueInfo | undefined {
    if (!raw) {
        return raw;
    }

    const queue: RequestQueueInfo = {
        ...raw,
        id: raw.name,
        createdAt: new Date(raw.createdAt),
        accessedAt: new Date(raw.accessedAt),
        modifiedAt: new Date(raw.modifiedAt),
    };

    return purgeNullsFromObject(queue);
}

/**
 * Creates a standard request ID (same as Platform).
 */
export function uniqueKeyToRequestId(uniqueKey: string): string {
    const str = createHash('sha256')
        .update(uniqueKey)
        .digest('base64')
        .replace(/([+/=])/g, '');

    return str.length > REQUEST_ID_LENGTH ? str.substr(0, REQUEST_ID_LENGTH) : str;
}

export function isBuffer(value: unknown): boolean {
    return ow.isValid(value, ow.any(ow.buffer, ow.arrayBuffer, ow.typedArray));
}

export function isStream(value: unknown): boolean {
    return ow.isValid(value, ow.object.hasKeys('on', 'pipe'));
}
