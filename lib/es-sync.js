const Rx = require('rx')
const RxAmqp = require('./rx-amqp')
const Promise = require('bluebird')
const _ = require('lodash')
const $http = require('http-as-promised')
const url = require('url')

module.exports = (config) => {

    return {

        pipeline(esQueueObservable) {

            return esQueueObservable
                .bufferWithTimeOrCount(config.bufferTimeout, config.bufferCount)
                .map((events)=> {
                    return Rx.Observable.defer(()=> this.enrichBulkSyncAck(events))
                })
                .concatAll()
                .retryWhen((errors) => {
                    return errors.do(console.error).delay(config.pipelineFailRetry)
                })
        },

        esQueueObservable(amqpConnection) {

            return RxAmqp.amqpObservable(amqpConnection, setup)

            function setup(channel, observer) {
                return Promise.all([
                    channel.assertQueue('es.sync'),
                    channel.bindQueue('es.sync', 'change.events', 'trackingData.insert'),
                    channel.consume('es.sync', (msg)=> observer.onNext({msg, channel}))
                ])
            }
        },

        enrichBulkSyncAck(events) {
            return Promise
                .map(events, (event)=> {
                    return this.fetchTrackingDataComposite(event)
                        .then((trackingDataComposite)=> {
                            return {
                                source: event,
                                result: trackingDataComposite
                            }
                        })
                })
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
                .spread((res, body)=>body)
        },

        syncBufferedToEs(eventsWithSourceAndResult) {

            if (eventsWithSourceAndResult.length > 0) {

                const bulkDocumentItems = _.map(eventsWithSourceAndResult, (sourceAndResult)=> {
                    const trackingDataComposite = sourceAndResult.result
                    const action_and_meta_data = JSON.stringify({index: {_id: trackingDataComposite.data.id}})
                    const source = JSON.stringify(denormalize(trackingDataComposite))
                    return `${action_and_meta_data}\n${source}`
                });

                return $http({
                    uri: `${config.esHostUrl}/telemetry/trackingData/_bulk`,
                    method: 'post',
                    body: bulkDocumentItems.join('\n').concat('\n')
                }).then(()=> eventsWithSourceAndResult)

            } else {
                return false
            }

            function denormalize(result) {
                const equipment = _.find(result.included, _.matches({type: 'equipment'}))
                const merged = _.merge({}, result.data, {attributes: {equipment: equipment.attributes}})
                return _.omit(merged, ['relationships'])
            }

        },

        ack(eventsWithSourceAndResult) {
            return Promise.all(_.map(eventsWithSourceAndResult, (sourceAndResult)=> {
                const source = sourceAndResult.source;
                return source.channel.ack(source.msg)
            })).then(()=> eventsWithSourceAndResult)
        }
    }
}





