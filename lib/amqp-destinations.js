module.exports.setup = (amqpConnection) => {

    const channelWrapper = amqpConnection.createChannel()
    return channelWrapper.addSetup((channel)=> {
        return Promise.all([
            // assert and bind the change events exchange / es sync queue
            channel.assertExchange('change.events.exchange', 'topic'),
            channel.assertQueue('es.sync.q'),
            channel.bindQueue('es.sync.q', 'change.events.exchange', 'trackingData.insert')
        ])
    })

}