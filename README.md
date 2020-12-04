# Local Emulation of [Apify Storage](https://apify.com/storage)
This package helps with local development of Apify Actors, by providing an emulation layer
for Apify cloud Storage. Interface of this package replicates the [Apify API client
for JavaScript](https://github.com/apify/apify-client-js) and can be used as its local
replacement.

[Apify SDK](https://sdk.apify.com) is the main consumer of this package. It allows the SDK
to be used without access to the Apify Platform.
