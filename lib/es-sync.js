const Rx = require('rx')
const RxAmqp = require('./rx-amqp')
const Promise = require('bluebird')
const _ = require('lodash')
const $http = require('http-as-promised')
const url = require('url')


function start(amqpConnection, config) {
    return esBulkSyncPipeline(config, queueToObserver(amqpConnection), fetchTrackingDataComposite(config), syncBufferedToEs(config))
}

function esBulkSyncPipeline(config, queueToObserver, fetchTrackingDataComposite, syncBufferedToEs) {
    return queueToObserver
        .bufferWithTimeOrCount(config.bufferTimeout, config.bufferCount)
        .map((events)=> {

            return Rx.Observable.defer(()=> {

                return Promise
                    .map(events, (event)=> {
                        return fetchTrackingDataComposite(event).reflect()
                            .then((trackingDataCompositeInspection)=> {
                                return {
                                    source: event,
                                    result: trackingDataCompositeInspection
                                }
                            })
                    })
                    .then((eventsWithSourceAndResults)=> {
                        return settleResults(eventsWithSourceAndResults)
                    })
                    .then((settled)=> {
                        return syncBufferedToEs(settled)
                    })
            })
        })
        .concatAll()
        .retry(5)
        .map((settled)=> {
            _.each(settled.resolved, (sourceAndResult)=> sourceAndResult.source.ack())
            _.each(settled.rejected, (sourceAndResult)=> sourceAndResult.source.nack())
            return settled
        });
}

function queueToObserver(amqpConnection) {

    return Rx.Observable.create((observer) => {
        amqpConnection.createChannel({
            setup: function (channel) {
                return Promise.all([
                    channel.assertQueue('es.sync'),
                    channel.bindQueue('es.sync', 'change.events', 'trackingData.insert'),
                    channel.consume('es.sync', RxAmqp.onConsume(channel, observer))
                ])
            }
        })
    });
}

function fetchTrackingDataComposite(config) {

    return (event) => {

        const trackingData = JSON.parse(event.content.toString());
        return $http.get({
                uri: `${config.appHostUrl}/trackingData/${trackingData.data.id}?include=equipment`,
                json: true
            })
            .spread((res, body)=>body)
    }
}

function settleResults(eventsWithResult) {

    const resolved = []
    const rejected = []

    return Promise.all(eventsWithResult)
        .map((eventWithResult, index) => {
            if (eventWithResult.result.isFulfilled()) {
                resolved.push(eventsWithResult[index])
            } else {
                rejected.push(eventsWithResult[index])
            }
        })
        .then(()=> {
            return {
                resolved,
                rejected
            }
        })
}


function syncBufferedToEs(config) {

    return (settled)=> {

        if (settled.resolved.length > 0) {

            const bulkDocumentItems = _.map(settled.resolved, (sourceAndResult)=> {
                const trackingDataComposite = sourceAndResult.result.value();
                const action_and_meta_data = JSON.stringify({index: {_id: trackingDataComposite.data.id}})
                const source = JSON.stringify(denormalize(trackingDataComposite))
                return `${action_and_meta_data}\n${source}`
            });

            return $http({
                uri: `${config.esHostUrl}/telemetry/trackingData/_bulk`,
                method: 'post',
                body: bulkDocumentItems.join('\n').concat('\n')
            }).then(()=> settled)

        } else {
            return false
        }

    }

    function denormalize(result) {
        const equipment = _.find(result.included, _.matches({type: 'equipment'}))
        const merged = _.merge({}, result.data, {attributes: {equipment: equipment.attributes}})
        return _.omit(merged, ['relationships'])
    }

}


module.exports = {start, esBulkSyncPipeline, queueToObserver, fetchTrackingDataComposite, syncBufferedToEs}

