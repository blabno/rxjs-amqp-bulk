const Rx = require('rx')

const fromAmqpConnection = module.exports.fromAmqpConnection = (amqpConnection, setup) => {

    return Rx.Observable
        .using(
            () => new DisposableChannel(),
            (disposable) => Rx.Observable.create((observer) => {
                return setup(disposable.channel, observer)
            })
        )

    function DisposableChannel() {
        const channel = amqpConnection.createChannel()
        const d = Rx.Disposable.create(() => {
            console.log(`disposing amqp channel`)
            channel.close()

        })
        d.channel = channel
        return d
    }

}

module.exports.queueObservable = function (amqpConnection, queueName, options, prefetch) {

    return fromAmqpConnection(amqpConnection, setup)

    function setup(channelWrapper, observer) {
        return channelWrapper.addSetup((channel)=> {
            const promises = []
            if (prefetch) {
                promises.push(channel.prefetch(prefetch))
            }
            promises.push(channel.consume(queueName, (msg)=> observer.onNext({source: {msg, channel}}), options || {}))
            return Promise.all(promises)
        })
    }
}
