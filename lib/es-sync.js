const Rx = require('rx')
const RxAmqp = require('./rx-amqp')
const Promise = require('bluebird')
const _ = require('lodash')
const $http = require('http-as-promised')

function start() {
    return esBulkSyncPipeline(queueToObserver, fetchAndDenormalizeTrackingData, syncBufferedToEs)
}

function esBulkSyncPipeline(queueToObserver, fetchAndDenormalizeTrackingData, syncBufferedToEs) {
    return queueToObserver()
        .map((event) => {
            return {
                source: event,
                result: fetchAndDenormalizeTrackingData(event)
            }
        })
        .bufferWithTimeOrCount(5000, 5)
        .flatMap(settleResults)
        .flatMap(syncBufferedToEs)
        .retry(5)
        .map((settled)=> {
            _.each(settled.resolved, (sourceAndResult)=> sourceAndResult.source.ack())
            _.each(settled.rejected, (sourceAndResult)=> {
                return sourceAndResult.source.nack()
            })
            return settled
        });
}

function queueToObserver() {

    const connection = require('./amqp-connection').connect()

    return Rx.Observable.create((observer) => {
        connection.createChannel({
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

function fetchAndDenormalizeTrackingData(event) {

    const trackingData = JSON.parse(event.content.toString);
    return $http.get(`http://localhost:3000/trackingData/${trackingData.data.id}?include=canVariable,equipment`)
        .spread((res, body)=> {
            return body
        })
}

function settleResults(eventsWithSourceAndResults) {
    const reflectedResults = getReflected(eventsWithSourceAndResults)

    const resolved = []
    const rejected = []

    return Promise.all(reflectedResults)
        .map((reflectedResult, index) => {
            if (reflectedResult.isFulfilled()) {
                resolved.push(eventsWithSourceAndResults[index])
            } else {
                rejected.push(eventsWithSourceAndResults[index])
            }
        })
        .then(()=> {
            return {
                resolved,
                rejected
            }
        })
}

function getReflected(eventsWithSourceAndResults) {
    const resultPromises = _.map(eventsWithSourceAndResults, 'result')
    return resultPromises.map(function (promise) {
        return promise.reflect()
    })
}

function syncBufferedToEs(settled) {

    const bulkPayload = settled.resolved.map((eventWithSourceAndResult)=> {
        const result = eventWithSourceAndResult.result;
        const action_and_meta_data = JSON.stringify({index: {_id: result.id}})
        const source = JSON.stringify(denormalize(result))
        return `${action_and_meta_data}\n${source}`
    })

    function denormalize(result) {
        const canVariable = _.find(result.included, _.matches({type: 'canVariables'}))
        const equipment = _.find(result.included, _.matches({type: 'equipment'}))
        return _.merge({}, result.data, {attributes: {canVariable: canVariable, equipment: equipment}})
    }

    return $http({
        uri: `http://${dockerHostName}:9200/telemetry/trackingData/_bulk`,
        method: 'post',
        body: bulkPayload.join('\n')
    }).then(()=> settled)

}


module.exports = {start, esBulkSyncPipeline, queueToObserver, fetchAndDenormalizeTrackingData, syncBufferedToEs}