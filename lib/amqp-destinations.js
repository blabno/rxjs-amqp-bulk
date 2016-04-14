const amqp = require('amqplib')

module.exports.setup = (config) => {

    return amqp.connect(config.amqpUrl)
        .then((conn) => conn.createChannel())
        .then((ch)=> {
            return Promise.resolve()
                .then(()=> ch.assertExchange('change.events.exchange', 'topic'))
                .then(()=> ch.assertQueue('es.sync.q'))
                .then(()=> ch.bindQueue('es.sync.q', 'change.events.exchange', 'trackingData.insert'))
                .then(()=> ch.assertQueue('es.sync.dlq'))
                .then(()=> ch.close())
        })

}