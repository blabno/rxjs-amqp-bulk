const RxAmqp = require('./rx-amqp')
const _ = require('lodash')

module.exports = (sync, config, amqpConnection)=> {
    
    return {

        pipeline() {
            return this.esSyncQueueObservable()
                .map((event) => _.merge(event, {json: JSON.parse(event.msg.content)}))
                .bufferWithTimeOrCount(config.bufferTimeout, config.bufferCount)
                .doOnNext((events)=> console.log(`${events.length} trackingData events were emitted from the online pipeline`))
                .filter((events) => events.length > 0)
                .flatMap((events) => sync.enrichBufferAndSync(events))
                .retryWhen((errors) => {
                    return errors.do(console.error).delay(config.pipelineOnDisposeRetry)
                })
        },

        esSyncQueueObservable() {
            return RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', config.esSyncQueuePrefetch)
        }
    }
}





