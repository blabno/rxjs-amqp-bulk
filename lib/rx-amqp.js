const Rx = require('rx')

// create an Observable which initiates a channel and runs the node-amqp-connection-manager ChannelWrapper#addSetup(setup) function
// https://github.com/benbria/node-amqp-connection-manager#channelwrapperaddsetupsetup
// this setup function should in turn hook up the channel consume function to the onConsume function in this module
// to start emitting observer onNext events
module.exports.amqpConsumeObservable = (amqpConnection, setup) => {

    return Rx.Observable
        .using(
            () => new DisposableChannel(),
            (disposable) => Rx.Observable.create((observer) => {
                const channel = disposable.channel;
                channel.addSetup(setup(onConsume(channel, observer)))
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
