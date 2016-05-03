const url = require('url')
const localInfraHostName = process.env.DOCKER_HOST ? url.parse(process.env.DOCKER_HOST).hostname : 'localhost'
const port = process.env.PORT || 3000

module.exports = {
    
    appHostPort: port,
    appHostUrl: `http://localhost:${port}`,
    openIDMHostUrl: `https://openidm.agcocorp.com`,
    mongodbUrl: process.env.MONGODB_URL ||process.env.MONGOLAB_URI ||  `mongodb://${localInfraHostName}/test`,
    amqpUrl: process.env.AMQP_URL || process.env.CLOUDAMQP_URL || `amqp://${localInfraHostName}:5672`,
    esSyncQueuePrefetch: process.env.ES_SYNC_QUEUE_PREFETCH || 100,
    retryInitial: process.env.RETRY_INITIAL || 100,
    retryMultiply: process.env.RETRY_MULTIPLY || 2,
    retryMax: process.env.RETRY_MAX || 10000,
    pipelineOnDisposeRetry: process.env.PIPELINE_ON_DISPOSE_RETRY || 10
    

}