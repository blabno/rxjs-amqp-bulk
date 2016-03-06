const chai = require('chai')
const expect = chai.expect

const Promise = require('bluebird')
const amqp = require('amqp-connection-manager')
const _ = require('lodash')
const Rx = require('rx')
const url = require('url')
const spies = require('chai-spies')
const RxAmqp = require('./rx-amqp')
const index = require('./index')

Promise.longStackTraces()
chai.config.includeStack = true
chai.use(spies);

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname

describe('Rxjs AMQP', ()=> {

    var connection

    beforeEach((done)=> {
        connection = amqp.connect([`amqp://${dockerHostName}:5672`]);
        connection.on('connect', ()=> {
            var channelWrapper = connection.createChannel({
                setup: function (channel) {
                    return channel.assertQueue('es.sync');
                }
            });
            channelWrapper.waitForConnect().then(done)
        })
        connection.on('disconnect', (params) => {
            console.error(params)
        });
    });

    afterEach(()=> {
        return connection.close()
    })

    it('feeds amqp messages with associated channel information and an ack/nack shorthands into an observer', (done)=> {

        Rx.Observable.create((observer) => {
                connection.createChannel({
                        setup: function (channel) {
                            return channel.consume('es.sync', RxAmqp.onConsume(channel, observer))
                        }
                    })
                    .on('error', done)
            })
            .subscribe(
                (event)=> {
                    const content = event.content.toString()
                    event.ack()
                    if (content === `4`) {
                        expect(event.ack).to.not.be.null
                        expect(event.nack).to.not.be.null
                        expect(event.channel).to.not.be.null
                        expect(event.content).to.not.be.null
                        expect(event.msg).to.not.be.null
                        done()
                    }
                }
            )

        const sendChannel = connection.createChannel();
        _.range(5).map((i)=> `${i}`).forEach((i) => {
            return sendChannel.sendToQueue('es.sync', new Buffer(i))
        })

    })

    it('should execute the processBuffer function with the results and ack the source messages', (done)=> {

        const range = _.range(0, 5).map((i)=> `${i}`)

        const mapSource = (event) => {
            return Promise.delay(1).then(()=> {
                return _.merge({}, event, {content: 'a' + event.content})
            })
        }

        const ack = chai.spy(()=> {
        })
        const nack = chai.spy(()=> {
        })

        var rabbitObservable = fakeRabbitObservableWithSpies(range, ack, nack);

        var subscription = index.reliableBufferedObservable(rabbitObservable, mapSource, (settled)=> {
            expect(settled.resolved).to.have.length(5)
            const expected = _.map(range.slice(0, 5), (item)=> {
                return 'a' + item
            })
            expect(expected).to.eql(_.map(settled.resolved, (resolvedItem)=> resolvedItem.result.value().content))
            return Promise.resolve(settled)
        })

        subscription
            .do(() => {
                    expect(ack).to.have.been.called.exactly(5);
                    expect(nack).to.have.been.called.exactly(0);
                    done()
                },
                done
            )
            .subscribe()

    })

    it('should not pass down the rejected results to the processBuffer function and nack the source messages', (done)=> {

        const range = _.range(0, 5).map((i)=> `${i}`)

        const mapSource = (event) => {
            return Promise.delay(1).then(()=> {
                if (_.toNumber(event.content) < 3) {
                    return _.merge({}, event, {content: 'a' + event.content})
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

        const subscription = index.reliableBufferedObservable(rabbitObservable, mapSource, (settled)=> {
            expect(settled.resolved).to.have.length(3)
            return Promise.resolve(settled)
        })

        subscription
            .do((settled) => {
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
                return _.merge({}, event, {content: 'a' + event.content})
            })
        }

        const ack = chai.spy(()=> {
        })
        const nack = chai.spy(()=> {
        })

        const rabbitObservable = fakeRabbitObservableWithSpies(range, ack, nack);

        var i = 1
        const subscription = index.reliableBufferedObservable(rabbitObservable, mapSource, (settled)=> {
            expect(settled.resolved).to.have.length(5)
            if (i++ < 4) {
                return Promise.reject(`force reject ${i}`)
            } else {
                return Promise.resolve(settled)
            }
        })

        subscription
            .do(() => {
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

function fakeRabbitObservableWithSpies(range, ack, nack) {
    return Rx.Observable.create((observer) => {
        range.forEach((i) => {
            observer.onNext({
                content: i,
                ack,
                nack,
                msg: {}
            })
        })
    });
}



