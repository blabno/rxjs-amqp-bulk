const amqp = require('amqplib')
const Promise = require('bluebird')

module.exports.setup = (config) => {
    return amqp.connect(config.amqpUrl)
        .then((conn) => conn.createChannel())
        .then((ch)=> {
            return Promise.resolve()

                .then(()=> ch.assertExchange('change-events', 'topic'))
                .then(()=> ch.assertQueue('users-queue'))
                .then(()=> ch.bindQueue('users-queue', 'change-events', 'users.insert'))
                .then(()=> ch.bindQueue('users-queue', 'change-events', 'users.update'))

                .then(()=> ch.assertExchange('users-retry', 'topic'))
                .then(()=> ch.assertQueue('users-retry-queue', {deadLetterExchange: 'change-events'}))
                .then(()=> ch.bindQueue('users-retry-queue', 'users-retry', 'users.insert'))
                .then(()=> ch.bindQueue('users-retry-queue', 'users-retry', 'users.update'))

                .then(()=> ch.close())
        })


}