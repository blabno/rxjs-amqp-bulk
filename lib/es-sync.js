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

    return constructPipeline(0)

    function constructPipeline(delay) {

        const base = queueToObserver
            .delay(delay)
            .bufferWithTimeOrCount(config.bufferTimeout, config.bufferCount)
            .map((events)=> {

                return Rx.Observable.defer(()=> {

                    return Promise
                        .map(events, (event)=> {
                            return fetchTrackingDataComposite(event)
                                .then((trackingDataComposite)=> {
                                    return {
                                        source: event,
                                        result: trackingDataComposite
                                    }
                                })
                        })
                        .then((eventsWithSourceAndResult)=> {
                            return syncBufferedToEs(eventsWithSourceAndResult)
                        })
                })
            })
            .concatAll()
            .map((eventsWithSourceAndResult)=> {
                _.each(eventsWithSourceAndResult, (sourceAndResult)=> sourceAndResult.source.ack())
                return eventsWithSourceAndResult
            });

        return base
            .catch((e)=> {
                console.error(e)
                return constructPipeline(config.pipelineFailRetry)
            })
    }

}

function queueToObserver(amqpConnection) {

    return Rx.Observable
        .using(
            () => new DisposableChannel(),
            (disposable) => Rx.Observable.create((observer) => {
                disposable.channel.addSetup(
                    (channel) => Promise.all([
                        channel.assertQueue('es.sync'),
                        channel.bindQueue('es.sync', 'change.events', 'trackingData.insert'),
                        channel.consume('es.sync', RxAmqp.onConsume(channel, observer))
                    ])
                )
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


function syncBufferedToEs(config) {

    return (eventsWithSourceAndResult)=> {

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

    }

    function denormalize(result) {
        const equipment = _.find(result.included, _.matches({type: 'equipment'}))
        const merged = _.merge({}, result.data, {attributes: {equipment: equipment.attributes}})
        return _.omit(merged, ['relationships'])
    }

}


module.exports = {start, esBulkSyncPipeline, queueToObserver, fetchTrackingDataComposite, syncBufferedToEs}

