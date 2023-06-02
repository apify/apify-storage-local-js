import { AllowedHttpMethods } from '../../resource_clients/request_queue';

export class UnprocessedRequest {
    constructor(public uniqueKey: string, public url: string, public method?: AllowedHttpMethods) {}
}
