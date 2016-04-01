const RxAmqp = require('./rx-amqp')
const Promise = require('bluebird')
const _ = require('lodash/fp')
const $http = require('http-as-promised')
const url = require('url')

module.exports = (config) => {

    return {

        pipeline(esQueueObservable) {

            return esQueueObservable
                .bufferWithTimeOrCount(config.bufferTimeout, config.bufferCount)
                .flatMap((events) => this.enrichBufferAndSync(events))
                .retryWhen((errors) => {
                    return errors.do(console.error).delay(config.pipelineFailRetry)
                })
        },

        esQueueObservable(amqpConnection) {

            return RxAmqp.amqpObservable(amqpConnection, setup)

            function setup(channel, observer) {
                return Promise.all([
                    channel.prefetch(config.prefetch),
                    channel.assertQueue('es.sync'),
                    channel.bindQueue('es.sync', 'change.events', 'trackingData.insert'),
                    channel.consume('es.sync', (msg)=> observer.onNext({msg, channel}))
                ])
            }
        },

        enrichBufferAndSync(events) {
            return Promise.resolve(events)
                .map(this.fetchTrackingDataComposite)
                .then(this.syncBufferedToEs)
                .then(this.ack)
        },

        fetchTrackingDataComposite(event) {
            const trackingData = JSON.parse(event.msg.content.toString());
            return $http.get({
                    // todo referring to id with _id due to a bug in hapi-harvester
                    uri: `${config.appHostUrl}/trackingData/${trackingData.data._id}?include=equipment`,
                    json: true
                })
                .spread((res, body)=> {
                    return {
                        source: event,
                        result: body
                    }
                })
        },

        syncBufferedToEs(eventsWithSourceAndResult) {

            if (eventsWithSourceAndResult.length > 0) {

                const transformToBulkDocumentItems = _.map((sourceAndResult)=> {
                        const trackingDataComposite = sourceAndResult.result
                        const action_and_meta_data = JSON.stringify({index: {_id: trackingDataComposite.data.id}})
                        const source = JSON.stringify(denormalize(trackingDataComposite))
                        return `${action_and_meta_data}\n${source}`
                    })

                const bulkDocument = transformToBulkDocumentItems(eventsWithSourceAndResult).join('\n').concat('\n');
                return $http({
                    uri: `${config.esHostUrl}/telemetry/trackingData/_bulk`,
                    method: 'post',
                    body: bulkDocument
                }).then(()=> eventsWithSourceAndResult)

            } else {
                return false
            }

            function denormalize(result) {
                const equipment = _.find(_.matches({type: 'equipment'}))(result.included)
                const merged = _.merge(result.data, {attributes: {equipment: equipment.attributes}})
                return _.omit(merged, ['relationships'])
            }

        },

        ack(eventsWithSourceAndResult) {
            const ackEvents = _.map((sourceAndResult)=> {
                const source = sourceAndResult.source;
                return source.channel.ack(source.msg)
            })
            return Promise.all(ackEvents(eventsWithSourceAndResult))
                .then(()=> eventsWithSourceAndResult)
        }
    }
}





