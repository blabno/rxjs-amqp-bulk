const amqp = require('amqp-connection-manager')

module.exports.connect = (config)=> {

    const connection = amqp.connect(config.amqpUrl);
    connection.on('disconnect', (params) => {
        console.error(params)
    })
    return connection
}
