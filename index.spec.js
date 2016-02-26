const chai = require('chai')
const expect = chai.expect

const Promise = require('bluebird')
const jackrabbit = require('jackrabbit')
const _ = require('lodash')
const Rx = require('rx')
const url = require('url')

Promise.longStackTraces()
chai.config.includeStack = true

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname

function rabbitToObservable(consume) {

    return Rx.Observable.create((observer) => {
        consume((data, ack, nack, msg) => {
            observer.onNext({data, ack, nack, msg})
        })
    })
}

describe('Rxjs AMQP', ()=> {

    beforeEach((done)=> {
        rabbit = jackrabbit(`amqp://${dockerHostName}:5672`)
        exchange = rabbit.default()
        exchange.on('ready', done)
    });

    afterEach((done)=> {
      rabbit.close(done)
    })

    it('converts a jackrabbit queue into an observable', (done)=> {

        const queue = exchange.queue({name: 'hello'})
        const consume = _.partialRight(queue.consume, {noAck: true})

        rabbitToObservable(consume)
            .subscribe(
                (event)=> {
                    if (event.data === `4`) {
                        done()
                    }
                },
                (err) => {
                    done(err)
                }
            );

        _.range(5).map((i)=> `${i}`).forEach((i) => exchange.publish(i, {key: 'hello'}))
    })

    it('should buffer messages', (done)=> {

        const range = _.range(0, 5).map((i)=> `${i}`)
        const queue = exchange.queue({name: 'hello', prefetch: 10})
        const consume = _.partialRight(queue.consume)

        rabbitToObservable(consume)
            .map((event) => {
                return {
                    source: event,
                    result: Promise.delay(1000).then(()=> {
                        return _.merge({}, event, {data: 'a' + event.data})
                    })
                }
            })
            .bufferWithCount(5)
            .flatMap((eventsWithResult)=> {

                var resultPromises = _.map(eventsWithResult, 'result');
                var reflectedResults = resultPromises.map(function (promise) {
                    return promise.reflect();
                });

                return Promise.all(reflectedResults)
                    .map(function (reflectedResult, index) {
                        if (reflectedResult.isFulfilled()) {
                            reflectedResult.value().ack()
                        } else {
                            console.error('promise rejected', reflectedResult.reason());
                            eventsWithResult[index].source.nack()
                        }
                        return reflectedResult
                    })
                    .then((reflectedResults)=> {
                        return reflectedResults
                    })
            })
            .map((reflectedResults) => {
                    const expected = _.map(range.slice(0, 5), (item)=> {
                        return 'a' + item
                    })
                    const resultData = _.map(reflectedResults, (reflectedResult)=> {
                        return reflectedResult.value().data
                    });
                    expect(expected).to.eql(resultData)
                    done()
                },
                (err) => {
                    done(err)
                }
            )
            .subscribe()

        range.forEach((i) =>
            exchange.publish(i, {key: 'hello'}))
    })

})


