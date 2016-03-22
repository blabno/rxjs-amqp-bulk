
const url = require('url')

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname

module.exports = {

    dockerHostName,
    appHostPort: 3000,
    appHostUrl: 'http://localhost:3000',
    mongodbUrl: `mongodb://${dockerHostName}/test`,
    mongodbOplogUrl: `mongodb://${dockerHostName}/local`,
    amqpUrl : [`amqp://${dockerHostName}:5672`],
    esHostUrl: `http://${dockerHostName}:9200`,
    bufferTimeout: 3000000,
    bufferCount: 20

}