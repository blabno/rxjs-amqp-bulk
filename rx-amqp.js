
module.exports.onConsume = (channel, observer) => {
    return (msg)=> observer.onNext({
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

