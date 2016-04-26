const RxAmqp = require('./rx-amqp')
const RxNode = require('rx-node')
const _ = require('lodash')

module.exports = (sync, config, amqpConnection)=> {

    const sendChannel = amqpConnection.createChannel({json: true})
    
    return {
        
        pipeline() {
            return this.esMassReindexQueueObservable()
                .map((event) => _.merge(event, {json: JSON.parse(event.msg.content)}))
                .doOnNext((event)=> console.log(`${event.json.length} trackingData ids were received from the mass reindex pipeline`))
                .filter((event) => event.json.length > 0)
                .flatMap((events) => sync.enrichBufferAndSync([events])) // force array to unify online & batch processing
                .retryWhen((errors) => {
                    return errors.do(console.error).delay(config.pipelineOnDisposeRetry)
                })
        },

        esMassReindexQueueObservable() {
            return RxAmqp.queueObservable(amqpConnection, 'es-mass-reindex-queue', config.esMassReindexQueuePrefetch)
        },

        reindex(batchSize) {

            const stream = adapter.models.trackingData.find({}, '_id').sort({dateOfReception: -1}).lean()
                .batchSize(batchSize).stream()

            return RxNode.fromStream(stream)
                .map((trackingDataIdObject) => trackingDataIdObject._id)
                .bufferWithTimeOrCount(10000, batchSize)
                .doOnNext((trackingDataIds)=> sendChannel.sendToQueue('es-mass-reindex-queue', trackingDataIds))
                .subscribe()
        }

    }

}





