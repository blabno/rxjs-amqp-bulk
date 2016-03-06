const Rx = require('rx')
const _ = require('lodash')
const Promise = require('bluebird')

module.exports.reliableBufferedObservable = (rabbitToObservable, mapSource, processBuffer) => {

    return rabbitToObservable
        .map((event) => {
            return {
                source: event,
                result: mapSource(event)
            }
        })
        .bufferWithTimeOrCount(5000, 5)
        .flatMap(settleResults)
        .flatMap(processBuffer)
        .retry(5)
        .do((settled)=> {
            _.each(settled.resolved, (eventWithSourceAndResult)=> eventWithSourceAndResult.source.ack())
            _.each(settled.rejected, (eventWithSourceAndResult)=> eventWithSourceAndResult.source.nack())
        })
}

function settleResults (eventsWithSourceAndResults) {
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
