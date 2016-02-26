const chai = require('chai')
const expect = chai.expect

const Promise = require('bluebird')
const jackrabbit = require('jackrabbit')
const _ = require('lodash')
const Rx = require('rx')
const url = require('url')
const amqp = require('jackrabbit/node_modules/amqplib');

Promise.longStackTraces()
chai.config.includeStack = true

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname

describe('Rxjs AMQP', ()=> {

    beforeEach((done)=> {
        rabbit = jackrabbit(`amqp://${dockerHostName}:5672`)
        rabbit.on('connected', ()=> {
            const connection = rabbit.getInternals().connection;
            connection.createChannel(function(err, ch) {
                ch.purgeQueue('hello', (err, ok)=> {
                   if (ok) {
                        ch.close(()=> {
                            exchange = rabbit.default()
                            exchange.on('ready', done)
                        })
                    }
                })
           });
        })

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

    it('should execute the afterBuffer function with the results and ack the source messages', (done)=> {

        const range = _.range(0, 5).map((i)=> `${i}`)
        const queue = exchange.queue({name: 'hello', prefetch: 10})
        const consume = _.partialRight(queue.consume)

        const mapFn = (event) => {
            return Promise.delay(1).then(()=> {
                return _.merge({}, event, {data: 'a' + event.data})
            })
        }

        var subscription = reliableBufferedObservable(rabbitToObservable(consume), mapFn, ()=> Promise.resolve())

        subscription
            .do((reflectedResults) => {
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

        range.forEach((i) => exchange.publish(i, {key: 'hello'}))
    })

    it('should not pass down the failures to the afterBuffer function and nack the source messages', (done)=> {

        const range = _.range(0, 5).map((i)=> `${i}`)
        const queue = exchange.queue({name: 'hello', prefetch: 10})
        const consume = _.partialRight(queue.consume)

        const mapFn = (event) => {
            return Promise.delay(1).then(()=> {
                if (_.toNumber(event.data) < 3) {
                    return _.merge({}, event, {data: 'a' + event.data})
                } else {
                    throw new Error('foobar')
                }
            })
        }

        var subscription = reliableBufferedObservable(rabbitToObservable(consume), mapFn, (results)=> {
            expect(results).to.have.length(3)
            return Promise.resolve()
        });

        subscription
            .do((reflectedResults) => {
                    done()
                },
                (err) => {
                    done(err)
                }
            )
            .subscribe()

        range.forEach((i) => exchange.publish(i, {key: 'hello'}))
    })

    function reliableBufferedObservable(rabbitObservable, mapFn, afterBufferFn) {
        return rabbitObservable
            .map((event) => {
                return {
                    source: event,
                    result: mapFn(event)
                }
            })
            .bufferWithTimeOrCount(5000, 5)
            .flatMap((eventsWithResult)=> {

                var resultPromises = _.map(eventsWithResult, 'result');
                var reflectedResults = resultPromises.map(function (promise) {
                    return promise.reflect();
                });

                const fulfilledPromises = []

                return Promise.all(reflectedResults)
                    .map(function (reflectedResult, index) {
                        if (reflectedResult.isFulfilled()) {
                            fulfilledPromises.push(reflectedResult.value())
                        } else {
                            console.error('promise rejected', reflectedResult.reason());
                            eventsWithResult[index].source.nack()
                        }
                        return reflectedResult
                    })
                    .then((reflectedResults)=> {
                        return afterBufferFn(fulfilledPromises)
                            .then(()=> {
                                fulfilledPromises.forEach((fulfilledPromise)=> {fulfilledPromise.ack()})
                                return reflectedResults
                            })
                    })
            });
    }

    function rabbitToObservable(consume) {

        return Rx.Observable.create((observer) => {
            consume((data, ack, nack, msg) => {
                observer.onNext({data, ack, nack, msg})
            })
        })
    }

})


