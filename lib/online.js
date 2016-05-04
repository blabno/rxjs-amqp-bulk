const RxAmqp = require('./rx-amqp')
const _ = require('lodash')
const Promise = require('bluebird')
const $http = require('http-as-promised')

module.exports = (config, amqpConnection)=> {

    const sendChannel = amqpConnection.createChannel({json: true});

    return {

        pipeline() {
            return RxAmqp.queueObservable(amqpConnection, 'users-queue')
                .map(RxAmqp.json)
                .flatMap((event)=> this.updateOpenIDM(event))
                .retryWhen((errors) => errors.do(console.error).delay(config.pipelineOnDisposeRetry))
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
                    RxAmqp.ack(event)
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
        const expiration = RxAmqp.calculateExpiration(event,config.retryInitial, config.retryMultiply, config.retryMax)
        return sendChannel.publish('users-retry', msg.fields.routingKey, event.json, {expiration})
    }


}





