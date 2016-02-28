const chai = require('chai')
const expect = chai.expect

const Promise = require('bluebird')
const jackrabbit = require('jackrabbit')
const _ = require('lodash')
const Rx = require('rx')
const url = require('url')
const amqp = require('jackrabbit/node_modules/amqplib');
const spies = require('chai-spies')

Promise.longStackTraces()
chai.config.includeStack = true
chai.use(spies);

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname

describe('Rxjs AMQP', ()=> {

    beforeEach((done)=> {
        rabbit = jackrabbit(`amqp://${dockerHostName}:5672`)
        rabbit.on('connected', ()=> {
            const connection = rabbit.getInternals().connection;
            connection.createChannel(function (err, ch) {
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

    afterEach(()=> {
        rabbit.close()
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
                }
            );

        _.range(5).map((i)=> `${i}`).forEach((i) => exchange.publish(i, {key: 'hello'}))
    })

    it('emits the rabbit connection error with the observer.onError', (done)=> {

        const queue = exchange.queue({name: 'hello'})
        const consume = _.partialRight(queue.consume, {noAck: true})

        rabbitToObservable(consume)
            .subscribe(
                (event)=> {
                    if (event.data === `3`) {
                        rabbit.close()
                    }
                },
                (err) => done()
            );

        _.range(5).map((i)=> `${i}`).forEach((i) => exchange.publish(i, {key: 'hello'}))
    })

    it('should execute the processBuffer function with the results and ack the source messages', (done)=> {

        const range = _.range(0, 5).map((i)=> `${i}`)

        const mapSource = (event) => {
            return Promise.delay(1).then(()=> {
                return _.merge({}, event, {data: 'a' + event.data})
            })
        }

        const ack = chai.spy(()=> {
        })
        const nack = chai.spy(()=> {
        })

        var rabbitObservable = fakeRabbitObservableWithSpies(range, ack, nack);

        var subscription = reliableBufferedObservable(rabbitObservable, mapSource, (results)=> {
            expect(results).to.have.length(5)
            const expected = _.map(range.slice(0, 5), (item)=> {
                return 'a' + item
            })
            expect(expected).to.eql(_.map(results, 'data'))
            return Promise.resolve()
        })

        subscription
            .do((reflectedResults) => {
                    expect(ack).to.have.been.called.exactly(5);
                    expect(nack).to.have.been.called.exactly(0);
                    done()
                },
                done
            )
            .subscribe()

        range.forEach((i) => exchange.publish(i, {key: 'hello'}))
    })

    it('should not pass down the failures to the processBuffer function and nack the source messages', (done)=> {

        const range = _.range(0, 5).map((i)=> `${i}`)

        const mapSource = (event) => {
            return Promise.delay(1).then(()=> {
                if (_.toNumber(event.data) < 3) {
                    return _.merge({}, event, {data: 'a' + event.data})
                } else {
                    throw new Error('foobar')
                }
            })
        }

        const ack = chai.spy(()=> {
        })
        const nack = chai.spy(()=> {
        })

        const rabbitObservable = fakeRabbitObservableWithSpies(range, ack, nack);

        const subscription = reliableBufferedObservable(rabbitObservable, mapSource, (results)=> {
            expect(results).to.have.length(3)
            return Promise.resolve()
        })

        subscription
            .do((reflectedResults) => {
                    expect(ack).to.have.been.called.exactly(3);
                    expect(nack).to.have.been.called.exactly(2);
                    done()
                },
                (err)=> {
                    console.error(err)
                }
            )
            .subscribe()
    })

    it('should retry on processBuffer failure', (done)=> {

        const range = _.range(0, 5).map((i)=> `${i}`)

        const mapSource = (event) => {
            return Promise.delay(1).then(()=> {
                return _.merge({}, event, {data: 'a' + event.data})
            })
        }

        const ack = chai.spy(()=> {
        })
        const nack = chai.spy(()=> {
        })

        const rabbitObservable = fakeRabbitObservableWithSpies(range, ack, nack);

        var i = 1
        const subscription = reliableBufferedObservable(rabbitObservable, mapSource, (results)=> {
            expect(results).to.have.length(5)
            if (i++ < 4) {
                return Promise.reject(`force reject ${i}`)
            } else {
                return Promise.resolve()
            }
        })

        subscription
            .do((reflectedResults) => {
                    expect(ack).to.have.been.called.exactly(5);
                    expect(nack).to.have.been.called.exactly(0);
                    done()
                },
                (err) => {
                    done(new Error(err))
                }
            )
            .subscribe()
    })
})

function reliableBufferedObservable(rabbitObservable, mapSource, processBuffer) {
    return rabbitObservable
        .map((event) => {
            return {
                source: event,
                result: mapSource(event)
            }
        })
        .bufferWithTimeOrCount(5000, 5)
        .flatMap((eventsWithResult)=> {

            const resultPromises = _.map(eventsWithResult, 'result')
            const reflectedResults = resultPromises.map(function (promise) {
                return promise.reflect()
            })

            const fulfilledPromises = []

            return Promise.all(reflectedResults)
                .map(function (reflectedResult, index) {
                    if (reflectedResult.isFulfilled()) {
                        fulfilledPromises.push(reflectedResult.value())
                    } else {
                        console.error('promise rejected', reflectedResult.reason())
                        eventsWithResult[index].source.nack()
                    }
                    return reflectedResult
                })
                .then((reflectedResults)=> {
                    return processBuffer(fulfilledPromises)
                        .then(()=> {
                            fulfilledPromises.forEach((fulfilledPromise)=> {
                                fulfilledPromise.ack()
                            })
                            return reflectedResults
                        })
                })
        })
        .retry()


}

function rabbitToObservable(consume) {

    return Rx.Observable.create((observer) => {
        consume((data, ack, nack, msg) => {
            observer.onNext({data, ack, nack, msg})
        })

        rabbit.on('error', (err)=> {
            observer.onError(new Error(err))
        })
    })
}

function fakeRabbitObservableWithSpies(range, ack, nack) {
    return Rx.Observable.create((observer) => {
        range.forEach((i) => {
            observer.onNext({
                data: i,
                ack,
                nack,
                msg: {}
            })
        })
    });
}



