const amqp = require('amqplib')

module.exports.setup = (config) => {

    return amqp.connect(config.amqpUrl)
        .then((conn) => conn.createChannel())
        .then((ch)=> {
            return Promise.resolve()
                .then(()=> ch.assertQueue('es-sync-error-queue'))

                .then(()=> ch.assertExchange('es-sync-loop-exchange', 'topic')) // make this a topic so we can listen in on tests
                .then(()=> ch.assertExchange('es-sync-retry-exchange', 'direct'))
                .then(()=> ch.assertQueue('es-sync-retry-queue', {deadLetterExchange: 'es-sync-loop-exchange'}))
                .then(()=> ch.bindQueue('es-sync-retry-queue', 'es-sync-retry-exchange', 'trackingData.insert'))

                .then(()=> ch.assertExchange('change-events-exchange', 'topic'))
                .then(()=> ch.assertQueue('es-sync-queue'))
                .then(()=> ch.bindQueue('es-sync-queue', 'change-events-exchange', 'trackingData.insert'))
                .then(()=> ch.bindQueue('es-sync-queue', 'es-sync-loop-exchange', 'trackingData.insert'))

                .then(()=> ch.close())
        })

}