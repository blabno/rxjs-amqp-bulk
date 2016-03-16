const chai = require('chai')
const expect = chai.expect

const Promise = require('bluebird')
const amqp = require('amqp-connection-manager')
const _ = require('lodash')
const Rx = require('rx')
const url = require('url')
const spies = require('chai-spies')
const EsSync = require('../lib/es-sync')
const Api = require('../lib/api')
const uuid = require('node-uuid');
const amqpConnectionFactory = require('../lib/amqp-connection');

Promise.longStackTraces()
chai.config.includeStack = true
chai.use(spies);

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname

describe('Rxjs AMQP', ()=> {

    var connection

    const equipmentId = uuid.v4();

    before(() => {
        return Api.start()
            .then((server)=> {
                global.server = server
                global.adapter = server.plugins['hapi-harvester'].adapter;
                const equipmentModel = global.adapter.models['equipment']
                return equipmentModel.remove({}).lean().exec()
            })
            .then(()=> {
                return global.adapter.create('equipment', {
                    id: equipmentId,
                    type: 'equipment',
                    attributes: {
                        identificationNumber: '5NPE24AF8FH002410'
                    }
                })
            })
    })

    beforeEach((done)=> {
        connection = amqpConnectionFactory.connect()
        const channelWrapper = connection.createChannel({
            setup: (channel) => {
                return Promise.all([
                    channel.assertExchange('change.events', 'topic'),
                    channel.assertQueue('es.sync'),
                    channel.purgeQueue('es.sync'),
                    channel.bindQueue('es.sync', 'change.events', 'trackingData.insert')])
            }
        });
        channelWrapper.waitForConnect()
            .then(()=> {
                done()
            })

        connection.on('disconnect', (params) => {
            console.error(params)
        });
    });

    afterEach(()=> {
        return connection.close()
    })


    const trackingData = _.range(0, 5).map((i)=> {
        return {
            id: uuid.v4(),
            type: 'trackingData',
            attributes: {
                heading: 10,
                canVariableValue: i
            },
            relationships: {
                equipment: {
                    data: {type: 'equipment', id: equipmentId}
                }
            }
        }
    })

    it('feeds amqp messages with associated channel information and an ack/nack shorthands into an observer', (done)=> {

        EsSync.queueToObserver()
            .subscribe(
                (event)=> {
                    event.ack()
                    const content = JSON.parse(event.content.toString())
                    if (content.attributes.canVariableValue === 4) {
                        expect(event.ack).to.not.be.null
                        expect(event.nack).to.not.be.null
                        expect(event.channel).to.not.be.null
                        expect(event.content).to.not.be.null
                        expect(event.msg).to.not.be.null
                        done()
                    }
                }
            )

        const sendChannel = connection.createChannel({json: true});
        trackingData.forEach((trackingDataMessage) => {
            return sendChannel.publish('change.events', 'trackingData.insert', trackingDataMessage)
        })

    })

    it('should execute the syncBufferedToEs function with the results and ack the source messages', (done)=> {


        const ack = chai.spy(()=> {
        })
        const nack = chai.spy(()=> {
        })

        const queueObserver = () => fakeQueueObservableWithSpies(trackingData, ack, nack);

        EsSync.esBulkSyncPipeline(queueObserver,
            (source)=> {
                return Promise.resolve()
            }
            , (settled)=> {
                return Promise.resolve(settled)
            })
            .do((settled) => {
                    expect(settled.resolved).to.have.length(5)
                    expect(nack).to.have.been.called.exactly(0);
                    expect(ack).to.have.been.called.exactly(5);
                    done()
                },
                done
            )
            .subscribe()

    })

    it('should not pass down the rejected results to the syncBufferedToEs function and nack the source messages', (done)=> {

        const ack = chai.spy(()=> {
        })
        const nack = chai.spy(()=> {
        })

        const queueObserver = ()=> fakeQueueObservableWithSpies(trackingData, ack, nack);

        const subscription = EsSync.esBulkSyncPipeline(queueObserver,
            (event) => {
                const trackingDataItem = JSON.parse(event.content.toString())
                return Promise.delay(1).then(()=> {
                    if (trackingDataItem.attributes.canVariableValue < 3) {
                        return trackingDataItem
                    } else {
                        throw new Error()
                    }
                })
            },
            (settled)=> Promise.resolve(settled))

        subscription
            .do((settled) => {
                    expect(settled.resolved).to.have.length(3)
                    expect(ack).to.have.been.called.exactly(3);
                    expect(nack).to.have.been.called.exactly(2);
                    done()
                },
                done
            )
            .subscribe()
    })

    it('should retry on processBuffer failure', (done)=> {

        const ack = chai.spy(()=> {
        })
        const nack = chai.spy(()=> {
        })

        const queueObserver = () => fakeQueueObservableWithSpies(trackingData, ack, nack);

        var i = 0
        const subscription = EsSync.esBulkSyncPipeline(queueObserver, ()=> Promise.resolve(), (settled)=> {
            if (i++ < 4) {
                return Promise.reject(`force reject ${i}`)
            } else {
                return Promise.resolve(settled)
            }
        })

        subscription
            .do(() => {
                    done()
                },
                (err) => {
                    done(new Error(err))
                }
            )
            .subscribe()
    })
})

function fakeQueueObservableWithSpies(trackingData, ack, nack) {
    return Rx.Observable.create((observer) => {
        trackingData.forEach((trackingDataItem) => {
            observer.onNext({
                content: new Buffer(JSON.stringify(trackingDataItem)),
                ack,
                nack,
                msg: {}
            })
        })
    });
}



