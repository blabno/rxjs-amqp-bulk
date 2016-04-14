const chai = require('chai')
const expect = chai.expect

const Promise = require('bluebird')
const _ = require('lodash/fp');
const url = require('url')
const uuid = require('node-uuid')
const $http = require('http-as-promised')

const sinon = require('sinon')

const Api = require('../lib/api')
const amqpConnectionFactory = require('../lib/amqp-connection')
const config = require('./config')

const monkeypatch = require('monkeypatch');

Promise.longStackTraces()
chai.config.includeStack = true

const equipmentId = uuid.v4()
const dealerId = uuid.v4()

const esSync = require('../lib/es-sync')(config)
const esSetup = require('../lib/es-setup')(config)
const amqp = require('amqplib')
const Rx = require('rx')

describe('AMQP Elasticsearch bulk sync', ()=> {

    beforeEach(()=> {

        global.amqpConnection = amqpConnectionFactory.connect(config)

        return amqp.connect(config.amqpUrl)
            .then((conn) => conn.createChannel())
            .then((ch)=> {
                return Promise.resolve()
                    .then(()=> ch.deleteQueue('es.sync.q'))
                    .then(()=> ch.deleteQueue('es.sync.dlq'))
                    .then(()=> ch.close())
            })
            .then(() => {
                return require('../lib/amqp-destinations').setup(config)
            })
    })

    afterEach(()=> {
        return global.amqpConnection.close()
    })

    describe('AMQP rxjs', ()=> {

        it('feeds amqp messages with associated channel information and an ack/nack shorthands into an observer', (done)=> {

            const subscription = esSync.esQueueObservable(global.amqpConnection)
                .subscribe(
                    (event)=> {
                        event.channel.ack(event.msg)
                        const content = JSON.parse(event.msg.content.toString())
                        if (content.attributes.canVariableValue === config.bufferCount - 1) {
                            expect(event.channel).to.not.be.null
                            expect(event.msg).to.not.be.null
                            subscription.dispose()
                            done()
                        }
                    }
                );

            const sendChannel = global.amqpConnection.createChannel({json: true})
            trackingData(config.bufferCount).forEach((trackingDataMessage) => {
                return sendChannel.publish('change.events.exchange', 'trackingData.insert', trackingDataMessage)
            })

        })
    })

    describe('End to End', ()=> {

        beforeEach(() => {
            return Promise.resolve()
                .then(apiStart)
                .then(clearData)
                .then(createDealer)
                .then(createEquipment)
                .then(esSetup.deleteIndex)
                .then(esSetup.putIndex)
                .then(esSetup.putMapping)

            function apiStart() {
                return Api.start(global.amqpConnection, config)
                    .then((server)=> {
                        global.server = server
                        global.adapter = server.plugins['hapi-harvester'].adapter
                    })
            }

            function clearData() {
                const models = global.adapter.models
                const removeModels = _.map((model)=> models[model].remove({}).exec());
                return Promise.all(removeModels(['equipment', 'dealers', 'trackingData']))
            }

            function createDealer() {
                return global.adapter.create('dealers', {
                    id: dealerId,
                    type: 'dealers',
                    attributes: {
                        name: 'AmazingTractors'
                    }
                })
            }

            function createEquipment() {
                return global.adapter.create('equipment', {
                    id: equipmentId,
                    type: 'equipment',
                    attributes: {
                        identificationNumber: '5NPE24AF8FH002410'
                    },
                    relationships: {
                        dealer: {
                            data: {type: 'dealers', id: dealerId}
                        }
                    }
                })
            }
        })


        afterEach(()=> {
            return global.server.stop()
        })

        it('should buffer changes, enrich and bulk sync to ES', (done)=> {

            postTrackingData(config.bufferCount).then((trackingData)=> {

                    const subscription = esSync.pipeline(esSync.esQueueObservable(amqpConnection))
                        .subscribe(
                            (events)=> verifyResultsInEsAndDispose(trackingData, subscription).then(done),
                            done
                        );
                },
                done
            )

        })

        it('should ack the messages', (done)=> {

            postTrackingData(config.bufferCount).then((trackingData)=> {

                const esQueueObservable = esSync.esQueueObservable(amqpConnection)
                    .map((event)=> {
                        // only wrap it once, all events should carry the same channel
                        if (!event.channel.ack.isSinonProxy) {
                            sinon.spy(event.channel, 'ack')
                        }
                        return event
                    })

                esSync.pipeline(esQueueObservable)
                    .subscribe((events)=> {
                            const first = _.first(events);
                            expect(first.source.channel.ack.callCount).to.equal(20)
                            done()
                        },
                        done)
            }, done)
        })

        it('should retry the pipeline on failure', (done)=> {

            var i = 0
            monkeypatch(esSync, 'enrichBufferAndSync', function (original, events) {
                return Promise.resolve()
                    .then(()=> {
                        i++
                        if (i < 3) {
                            throw new Error(`force reject ${i}`)
                        }
                    })
                    .then(()=> original(events))
            })

            postTrackingData(config.bufferCount).then((trackingData)=> {

                    const subscription = esSync.pipeline(esSync.esQueueObservable(amqpConnection))
                        .subscribe(
                            ()=> verifyResultsInEsAndDispose(trackingData, subscription).then(done),
                            done
                        )
                }
            )

        })

        it('should gracefully recover when the AMQP connection fails before messages are ack\'ed', (done)=> {

            var i = 0
            monkeypatch(esSync, 'enrichBufferAndSync', function (original, events) {
                return Promise.resolve()
                    .then(()=> {
                        i++
                        if (i < 3) {
                            _.first(events).channel._connection.connection.stream.destroy()
                        }
                    })
                    .then(()=> original(events))
            })

            postTrackingData(config.bufferCount).then((trackingData)=> {

                    const subscription = esSync.pipeline(esSync.esQueueObservable(amqpConnection))
                        .skip(2) // ack on a broken channel/connection doesn't result in an error so skip the first observed value batches
                        .subscribe(
                            ()=> verifyResultsInEsAndDispose(trackingData, subscription).then(done),
                            done
                        );
                }
            )

        })

        it('should park messages with functional errors on the dlq', (done)=> {

            postTrackingData(config.bufferCount).then((trackingData)=> {

                    // remove the dealer relationship
                    // the fetch of the composite document will not return all included data required for ES insert
                    // hence the messages are sent to the dead letter queue
                    global.adapter.update('equipment', equipmentId, {
                            id: equipmentId,
                            type: 'equipment',
                            attributes: {
                                identificationNumber: '5NPE24AF8FH002410'
                            },
                            relationships: {}
                        })
                        .then(()=> {
                            const subscription = esSync.pipeline(esSync.esQueueObservable(amqpConnection))
                                .subscribe(
                                    ()=> {
                                        amqpQueueBrowseObserver('es.sync.dlq')
                                            .bufferWithCount(config.bufferCount)
                                            .subscribe(
                                                (msgs)=> {
                                                    expect(msgs).to.have.lengthOf(20)
                                                    subscription.dispose()
                                                    done()
                                                }
                                            )
                                    },
                                    done
                                )
                        })
                },
                done
            )

        })

        it('should be able to cope with 1000 trackingData messages under 6 secs', (done)=> {

            const docs = config.bufferCount * 50

            postTrackingData(docs)
                .then(()=> {
                    const begin = new Date()

                    const subscription = esSync.pipeline(esSync.esQueueObservable(amqpConnection))
                        .bufferWithCount(docs / config.bufferCount)
                        .subscribe(
                            (events)=> {
                                const end = new Date()
                                const duration = end - begin;
                                console.log(`time took : ${duration}`)
                                expect(duration).to.be.below(6000)
                                subscription.dispose()
                                done()
                            },
                            done
                        );
                })

        })

        function verifyResultsInEsAndDispose(trackingData, subscription) {
            return Promise.all(lookupTrackingDataInEs((trackingData)))
                .then((trackingDataFromEs)=> {
                    expect(trackingDataFromEs.length).to.equal(config.bufferCount)
                    expect(_.map('_source.attributes')(trackingDataFromEs)).to.not.be.undefined;
                    subscription.dispose()
                })
        }

        const lookupTrackingDataInEs = _.map((trackingDataItem)=> {
            return $http({
                uri: `${config.esHostUrl}/telemetry/trackingData/${trackingDataItem.data.id}`,
                method: 'get',
                json: true
            }).spread((res, body)=> body)
        })

        function postTrackingData(maxRange) {

            return Promise
                .map(trackingData(maxRange), post, {concurrency: 10})

            function post(trackingDataItem) {
                return $http({
                    uri: `http://localhost:3000/trackingData`,
                    method: 'post',
                    json: {data: trackingDataItem}
                }).spread((res, body) => body)
            }
        }

    })

})

function amqpQueueBrowseObserver(queueName) {
    return Rx.Observable.create((observer) => {
        amqp.connect(config.amqpUrl)
            .then((conn) => conn.createChannel())
            .then((ch)=> {
                function channelGet() {
                    ch.get(queueName, {noAck: true}).then((msg)=> {
                        if (!msg) {
                            observer.onCompleted()
                        } else {
                            observer.onNext(msg)
                            channelGet()
                        }
                    })
                }
                channelGet()
            })
    })
}

function trackingData(rangeMax) {
    return _.range(0, rangeMax).map((i)=> {
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
}


