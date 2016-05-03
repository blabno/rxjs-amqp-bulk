const RxAmqp = require('./rx-amqp')
const _ = require('lodash')
const Promise = require('bluebird')
const $http = require('http-as-promised')

module.exports = (config, amqpConnection)=> {

    const sendChannel = amqpConnection.createChannel({json: true});

    return {

        pipeline() {
            return RxAmqp.queueObservable(amqpConnection, 'users-queue')
                .map((event) => _.merge(event, {json: JSON.parse(event.msg.content)}))
                .flatMap((event)=> this.updateOpenIDM(event))
                .retryWhen((errors) => {
                    return errors.do(console.error).delay(config.pipelineOnDisposeRetry)
                })
        },

        updateOpenIDM(event) {
            return Promise.resolve()
                .then(()=> {
                    return $http(`${config.appHostUrl}/users?filter[id]=${event.json}`)
                })
                .then((userRepresentation)=> {
                    // simplified
                    // omitting the actual OpenIDM lookup, constructing payload and updating status on response
                    return $http({uri: `${config.openIDMHostUrl}/users`, method: 'post', json: {}})
                        .then(()=> event) // when properly implemented this should return the updated representation of the user
                })
                .catch((e)=> {
                    console.error('updateOpenIDM failed', e)
                    return this.sendToRetryQueue(event);
                })
                .then((successEvent)=> {
                    event.channel.ack(event.msg)
                    return successEvent
                })
        },

        sendToRetryQueue(event) {
            console.log(`forwarding user change event msg to the retry queue. routing key : ${event.msg.fields.routingKey} id : ${event.json}`)
            return retryAndLog(event).then(()=> false) // returning false on a caught failure, this facilitates testing
        }

    }

    function retryAndLog(event) {
        const msg = event.msg
        const expiration = calculateExpiration(msg.properties.headers);
        return sendChannel.publish('users-retry', msg.fields.routingKey, event.json, {expiration})
    }

    function calculateExpiration(headers) {
        if (headers["x-death"]) {
            const candidateExpiration = (headers["x-death"][0]["original-expiration"] * config.retryMultiply)
            return (candidateExpiration > config.retryMax ) ? config.retryMax : candidateExpiration
        } else {
            return config.retryInitial
        }
    }

}





