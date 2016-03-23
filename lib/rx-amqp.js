const Rx = require('rx')

module.exports.amqpConsumeObservable = (amqpConnection, setupChannel) => {

    return Rx.Observable
        .using(
            () => new DisposableChannel(),
            (disposable) => Rx.Observable.create((observer) => {
                const channel = disposable.channel;
                channel.addSetup(setupChannel(onConsume(channel, observer)))
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

function onConsume (channel, observer) {
    return (msg)=> {
        observer.onNext({
            content: msg.content,
            msg,
            channel,
            ack: ()=> {
                channel.ack(msg)
            },
            nack: ()=> {
                channel.nack(msg)
            }
        });
    }
}
