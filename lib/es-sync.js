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

            function setup(channelWrapper, observer) {
                return channelWrapper.addSetup((channel)=> {
                    return Promise.all([
                        channel.prefetch(config.prefetch),
                        channel.consume('es.sync.q', (msg)=> observer.onNext({source : {msg, channel: channelWrapper}}))
                    ])
                })
            }
        },

        enrichBufferAndSync(events) {
            return Promise.resolve(events)
                .map(this.fetchTrackingDataComposite)
                .then(this.syncBufferedToEs)
                .catch((e)=> {
                    console.log(e)
                    const sendAllToWaitQueue = _.map((event)=> {
                        return event.source.channel.sendToQueue('es.sync.wait.q', event.source.msg.content)
                            .then(()=> {
                                return event
                            })
                    })
                    return Promise.all(sendAllToWaitQueue(events))
                })
                .then(()=> this.ack(events))
        },

        fetchTrackingDataComposite(event) {
            const trackingData = JSON.parse(event.source.msg.content.toString());

            return $http.get({
                    // todo referring to id with _id due to a bug in hapi-harvester
                    uri: `${config.appHostUrl}/trackingData/${trackingData.data._id}?include=equipment,equipment.dealer`,
                    json: true
                })
                .spread((res, body)=> {
                    // check for functional error
                    if (!_.find({type: 'dealers'}, body.included)) {
                        // send the contents to the dlq
                        return event.source.channel.sendToQueue('es.sync.dlq', event.source.msg.content)
                            .then(()=> {
                                return event
                            })
                    } else {
                        return _.merge(event, {result: body})
                    }
                })
        },

        syncBufferedToEs(eventsWithSourceAndResult) {

            return Promise.resolve().then(()=> {

                if (eventsWithSourceAndResult.length > 0) {

                    const transformToBulkDocumentItems = _.map((sourceAndResult)=> {
                        if (sourceAndResult.result) {
                            const trackingDataComposite = sourceAndResult.result
                            const action_and_meta_data = JSON.stringify({index: {_id: trackingDataComposite.data.id}})
                            const source = JSON.stringify(denormalize(trackingDataComposite))
                            return `${action_and_meta_data}\n${source}`
                        }
                    })

                    const bulkDocumentItems = _.compact(transformToBulkDocumentItems(eventsWithSourceAndResult));

                    if (!_.isEmpty(bulkDocumentItems)) {

                        const bulkDocument = bulkDocumentItems.join('\n').concat('\n');
                        return $http({
                            uri: `${config.esHostUrl}/telemetry/trackingData/_bulk`,
                            method: 'post',
                            body: bulkDocument
                        }).then(()=> eventsWithSourceAndResult)

                    } else {
                        return eventsWithSourceAndResult
                    }

                } else {
                    return []
                }

            })

            function denormalize(result) {
                const equipment = _.find(_.matches({type: 'equipment'}))(result.included)
                const dealer = _.find(_.matches({type: 'dealers'}))(result.included)
                const merged = _.merge(result.data, {
                    attributes: {
                        equipment: equipment.attributes,
                        dealer: dealer.attributes
                    }
                })
                return _.omit(['relationships'])(merged)
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

    function returnSourceAndResult(event, result) {
        return {
            source: event,
            result: result
        }
    }
}





