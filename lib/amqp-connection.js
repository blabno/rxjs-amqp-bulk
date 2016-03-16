const amqp = require('amqp-connection-manager')
const url = require('url')

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname
module.exports.connect = ()=> amqp.connect([`amqp://${dockerHostName}:5672`])