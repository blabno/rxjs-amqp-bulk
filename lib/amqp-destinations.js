const amqp = require('amqplib')

module.exports.setup = (config) => {

    return amqp.connect(config.amqpUrl)
        .then((conn) => conn.createChannel())
        .then((ch)=> {
            return Promise.resolve()
                // assert and bind the dlq exchange / queue
                .then(()=> ch.assertExchange('es.sync.dlq.exchange', 'direct'))
                .then(()=> ch.assertQueue('es.sync.dlq'))
                .then(()=> ch.bindQueue('es.sync.dlq', 'es.sync.dlq.exchange'))
                // assert and bind the change events exchange / es sync queue
                .then(()=> ch.assertExchange('change.events.exchange', 'topic'))
                .then(()=> ch.assertQueue('es.sync.q', {arguments: {"x-dead-letter-exchange": "es.sync.dlq.exchange"}}))
                .then(()=> ch.bindQueue('es.sync.q', 'change.events.exchange', 'trackingData.insert'))
                // close the channel
                .then(()=> ch.close())
        })

}