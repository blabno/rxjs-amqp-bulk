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
const rabbit = jackrabbit(`amqp://${dockerHostName}:5672`)
const exchange = rabbit.default()

function rabbitToObservable(consume) {

    return Rx.Observable.create((observer) => {
        consume((data, ack, nack, msg) => {
            observer.onNext({data, ack, nack, msg})
        })
    })
}

describe('Rxjs AMQP', ()=> {

    before((done)=> {
        exchange.on('ready', done)
    });

    it('should convert a jackrabbit queue into an observable', (done)=> {

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

        const queue = exchange.queue({name: 'hello2'})

        const consume = _.partialRight(queue.consume, {noAck: true})

        rabbitToObservable(consume)
            .bufferWithCount(5)
            .subscribe(
                (events)=> {

                    const actual = _.map(events, 'data')
                    const expected = range.slice(0, 5)

                    expect(expected).to.eql(actual)

                    done()

                },
                (err) => {
                    done(err)
                }
            );

        range.forEach((i) =>
            exchange.publish(i, {key: 'hello2'}))
    })

})


