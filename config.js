const url = require('url')
const localInfraHostName = process.env.DOCKER_HOST ? url.parse(process.env.DOCKER_HOST).hostname : 'localhost'
const port = process.env.PORT || 3000

module.exports = {

    localInfraHostName,
    appHostPort: port,
    appHostUrl: `http://localhost:${port}`,
    mongodbUrl: process.env.MONGODB_URL ||process.env.MONGOLAB_URI ||  `mongodb://${localInfraHostName}/test`,
    amqpUrl: process.env.AMQP_URL || process.env.CLOUDAMQP_URL || `amqp://${localInfraHostName}:5672`,
    esHostUrl: process.env.ES_URL || process.env.SEARCHBOX_URL || `http://${localInfraHostName}:9200`,
    bufferTimeout: process.env.BUFFER_TIMEOUT || 1000,
    bufferCount: process.env.BUFFER_COUNT || 20,
    pipelineFailRetry: process.env.PIPELINE_FAIL_RETRY || 10,
    esSyncQueuePrefetch: process.env.ES_SYNC_QUEUE_PREFETCH || 40,
    retryInitial: process.env.RETRY_INITIAL || 100,
    retryMultiply: process.env.RETRY_MULTIPLY || 2,
    retryMax: process.env.RETRY_MAX || 10000

}