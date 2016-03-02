const Rx = require('rx')
const _ = require('lodash')
const Promise = require('bluebird')

module.exports.ackNack = (settled) => {
    _.each(settled.resolved, (eventWithSourceAndResult)=> eventWithSourceAndResult.source.ack())
    _.each(settled.rejected, (eventWithSourceAndResult)=> eventWithSourceAndResult.source.nack())
}

module.exports.settleResults = (eventsWithSourceAndResults) => {
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

module.exports.fromJackrabbit = (raabit, consume) => {

    return Rx.Observable.create((observer) => {
        consume((data, ack, nack, msg) => {
            observer.onNext({data, ack, nack, msg})
        })

        rabbit.on('error', (err)=> {
            observer.onError(new Error(err))
        })
    })
}
