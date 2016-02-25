const chai = require('chai')
const Promise = require('bluebird')
const jackrabbit = require('jackrabbit')
const _ = require('lodash')
const Rx = require('rx')
const url = require('url')

Promise.longStackTraces()
chai.config.includeStack = true

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname
const rabbit = jackrabbit(`amqp://${dockerHostName}:5672`)
var exchange = rabbit.default()

function publish(i) {
    exchange.publish(i, {key: 'hello'})
}

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

        const queue = rabbit
            .default()
            .queue({name: 'hello'})

        const consume = _.partialRight(queue.consume, {noAck: true})

        rabbitToObservable(consume)
            .subscribe(
                (event)=> {
                    if (event.data == 9) {
                        done()
                    }
                },
                done
            )

        _.range(10).forEach((i) => publish(i))

    })

})


