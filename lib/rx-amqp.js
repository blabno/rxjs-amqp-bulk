const Rx = require('rx')
const _ = require('lodash')
const Promise = require('bluebird')

module.exports.queueObservable = function (amqpConnection, queueName, options, prefetch) {

    if (!_.isObject(options) && !prefetch) {
        prefetch = options
        options = {}
    }

    return Rx.Observable
        .using(
            () => new DisposableChannel(),
            (disposable) => Rx.Observable.create((observer) => {
                return disposable.channel.addSetup((channel)=> {
                    const promises = []
                    if (prefetch) {
                        promises.push(channel.prefetch(prefetch))
                    }
                    promises.push(channel.consume(queueName, (msg)=> observer.onNext({msg, channel}), options || {}))
                    return Promise.all(promises)
                })
            })
        )

    function DisposableChannel() {
        console.log(`initialising amqp channel`)
        const channel = amqpConnection.createChannel()
        const d = Rx.Disposable.create(() => {
            console.log(`disposing amqp channel`)
            channel.close()
        })
        d.channel = channel
        return d
    }


}

module.exports.json = (event)=> _.merge(event, {json: JSON.parse(event.msg.content)})

module.exports.ack = (event) => event.channel.ack(event.msg)

module.exports.calculateExpiration = (event, retryInitial, retryMultiply, retryMax) => {
    const headers = event.msg.properties.headers
    if (headers["x-death"]) {
        const candidateExpiration = (headers["x-death"][0]["original-expiration"] * (retryMultiply ? retryMultiply : 1))
        return (candidateExpiration > retryMax) ? retryMax : candidateExpiration
    } else {
        return retryInitial
    }
}
