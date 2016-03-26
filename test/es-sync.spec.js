const chai = require('chai')
const expect = chai.expect

const Promise = require('bluebird')
const _ = require('lodash')
const Rx = require('rx')
const url = require('url')
const spies = require('chai-spies')
const uuid = require('node-uuid')
const $http = require('http-as-promised')

const EsSync = require('../lib/es-sync')
const EsSetup = require('../lib/es-setup')
const Api = require('../lib/api')
const amqpConnectionFactory = require('../lib/amqp-connection')
const config = require('./config')

Promise.longStackTraces()
chai.config.includeStack = true
chai.use(spies)

const equipmentId = uuid.v4()

describe('AMQP Elasticsearch bulk sync', ()=> {

    describe('Pipeline rxjs', ()=> {

        it('should execute the syncBufferedToEs function with the results and ack the source messages', (done)=> {

            const ack = chai.spy(()=> {
            })
            const nack = chai.spy(()=> {
            })

            const queueObserver = fakeQueueObservableWithSpies(trackingData(config.bufferCount), ack, nack)

            EsSync.esBulkSyncPipeline(config, queueObserver,
                (source)=> Promise.resolve(),
                (eventsWithSourceAndResult)=> Promise.resolve(eventsWithSourceAndResult),
                EsSync.ack)
                .do((eventsWithSourceAndResult) => {
                        expect(eventsWithSourceAndResult).to.have.length(config.bufferCount)
                        expect(nack).to.have.been.called.exactly(0)
                        expect(ack).to.have.been.called.exactly(config.bufferCount)
                        done()
                    },
                    done
                )
                .subscribe()

        })

        it('should retry the pipeline on failure', (done)=> {

            const ack = chai.spy(()=> {
            })
            const nack = chai.spy(()=> {
            })

            const queueObserver = fakeQueueObservableWithSpies(trackingData(config.bufferCount), ack, nack)

            var i = 0
            const pipeline = EsSync.esBulkSyncPipeline(config, queueObserver, ()=> Promise.resolve(), (eventsWithSourceAndResult)=> {
                    if (i++ < 4) {
                        return Promise.reject(new Error(`force reject ${i}`))
                    } else {
                        return Promise.resolve(eventsWithSourceAndResult)
                    }
                },
                EsSync.ack)

            const subscription = pipeline
                .do((eventsWithSourceAndResult) => {
                        expect(eventsWithSourceAndResult).to.have.length(config.bufferCount)
                        expect(ack).to.have.been.called.exactly(config.bufferCount)
                        subscription.dispose()
                        done()
                    },
                    done
                )
                .subscribe();
        })


        function fakeQueueObservableWithSpies(trackingData, ack, nack) {
            return Rx.Observable.create((observer) => {
                trackingData.forEach((trackingDataItem) => {
                    observer.onNext({
                        channel: {
                            ack,
                            nack
                        },
                        msg: {
                            content: new Buffer(JSON.stringify(trackingDataItem))
                        }
                    })
                })
            })
        }
    })

    beforeEach(()=> {

        global.amqpConnection = amqpConnectionFactory.connect(config)
        const channelWrapper = amqpConnection.createChannel({
            setup: (channel) => {
                return Promise.all([
                    channel.assertExchange('change.events', 'topic'),
                    channel.assertQueue('es.sync'),
                    channel.purgeQueue('es.sync'),
                    channel.bindQueue('es.sync', 'change.events', 'trackingData.insert')])
            }
        })
        return channelWrapper.waitForConnect()
            .then(()=> {
                return channelWrapper.close()
            })

    })

    afterEach(()=> {
        return global.amqpConnection.close()
    })

    describe('AMQP rxjs', ()=> {

        it('feeds amqp messages with associated channel information and an ack/nack shorthands into an observer', (done)=> {

            const subscription = EsSync.esQueueConsumeObservable(global.amqpConnection, config)
                .subscribe(
                    (event)=> {
                        event.channel.ack(event.msg)
                        const content = JSON.parse(event.msg.content.toString())
                        if (content.attributes.canVariableValue === 4) {
                            expect(event.channel).to.not.be.null
                            expect(event.msg).to.not.be.null
                            subscription.dispose()
                            done()
                        }
                    }
                );

            const sendChannel = global.amqpConnection.createChannel({json: true})
            trackingData(config.bufferCount).forEach((trackingDataMessage) => {
                return sendChannel.publish('change.events', 'trackingData.insert', trackingDataMessage)
            })

        })
    })

    describe('End to End', ()=> {

        beforeEach(() => {
            return Promise.resolve()
                .then(apiStart)
                .then(clearData)
                .then(createEquipment)
                .then(deleteIndex)
                .then(putIndex)
                .then(putMapping)

            function apiStart() {
                return Api.start(global.amqpConnection, config)
                    .then((server)=> {
                        global.server = server
                        global.adapter = server.plugins['hapi-harvester'].adapter
                    })
            }

            function deleteIndex() {
                return EsSetup.deleteIndex(config)
            }

            function putIndex() {
                return EsSetup.putIndex(config)
            }

            function putMapping() {
                return EsSetup.putMapping(config)
            }

            function clearData() {
                const models = global.adapter.models
                return Promise.all(_.map(['equipment', 'trackingData'], (model)=> {
                    return models[model].remove({}).exec()
                }))
            }

            function createEquipment() {
                return global.adapter.create('equipment', {
                    id: equipmentId,
                    type: 'equipment',
                    attributes: {
                        identificationNumber: '5NPE24AF8FH002410'
                    }
                })
            }
        })


        afterEach(()=> {
            return global.server.stop()
        })

        it('should buffer changes, parallel enrich and push to ES', (done)=> {

            postTrackingData(config.bufferCount).then((trackingData)=> {
                    const subscription = EsSync.start(amqpConnection, config)
                        .subscribe(
                            ()=> verifyResultsInEsAndDispose(trackingData, subscription).then(done),
                            done
                        );
                },
                done
            )

        })

        it('should gracefully recover when the AMQP connection fails before all messages are ack\'ed', (done)=> {

            postTrackingData(config.bufferCount).then((trackingData)=> {
                    var i = 0
                    const failAck = (eventsWithSourceAndResult)=> {
                        return Promise.resolve()
                            .then(()=> {
                                i++
                                if (i < 3) {
                                    const sourceAndResult = _.first(eventsWithSourceAndResult);
                                    sourceAndResult.source.channel.connection.stream.destroy()
                                }
                            })
                            .then(()=> EsSync.ack(eventsWithSourceAndResult))
                    }

                    const subscription = EsSync.esBulkSyncPipeline(
                        config,
                        EsSync.esQueueConsumeObservable(amqpConnection, config),
                        EsSync.fetchTrackingDataComposite(config),
                        EsSync.syncBufferedToEs(config),
                        failAck)
                        .skip(2) // ack on a broken channel/connection doesn't result in an error so skip the first observed value batches
                        .subscribe(
                            ()=> verifyResultsInEsAndDispose(trackingData, subscription).then(done),
                            done
                        );
                }
            )

        })

        it('should be able to cope with 1000 trackingData messages under 6 secs', (done)=> {

            const docs = config.bufferCount * 50

            postTrackingData(docs)
                .then(()=> {
                    const begin = new Date()
                    const subscription = EsSync.start(global.amqpConnection, config)
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
            return Promise.all(_.map(trackingData, (trackingDataItem)=> {
                    return $http({
                        uri: `${config.esHostUrl}/telemetry/trackingData/${trackingDataItem.data.id}`,
                        method: 'get',
                        json: true
                    }).spread((res, body)=> body)
                }))
                .then((trackingDataFromEs)=> {
                    expect(trackingDataFromEs.length).to.equal(config.bufferCount)
                    subscription.dispose()
                })
        }

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


