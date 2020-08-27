class QueueOperationInfo {
    /**
     * @param {string} requestId
     * @param {number|null} [requestOrderNo]
     */
    constructor(requestId, requestOrderNo) {
        this.requestId = requestId;
        this.wasAlreadyPresent = requestOrderNo !== undefined;
        this.wasAlreadyHandled = requestOrderNo === null;
    }
}

module.exports = QueueOperationInfo;
