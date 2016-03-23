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
                (eventsWithSourceAndResult)=> Promise.resolve(eventsWithSourceAndResult))
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
            })

            pipeline
                .do((eventsWithSourceAndResult) => {
                        expect(eventsWithSourceAndResult).to.have.length(config.bufferCount)
                        expect(ack).to.have.been.called.exactly(config.bufferCount)
                        done()
                    },
                    done
                )
                .subscribe()
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
            })
        }
    })

    beforeEach((done)=> {

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
        channelWrapper.waitForConnect()
            .then(()=> {
                done()
            })

    })

    afterEach(()=> {
        return global.amqpConnection.close()
    })

    describe('AMQP rxjs', ()=> {

        it('feeds amqp messages with associated channel information and an ack/nack shorthands into an observer', (done)=> {

            EsSync.queueToObserver(global.amqpConnection)
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

            postTrackingData(config.bufferCount).then(
                (trackingData)=> {

                    EsSync.start(amqpConnection, config)
                        .subscribe(
                            ()=> {
                                Promise.all(_.map(trackingData, (trackingDataItem)=> {
                                        return $http({
                                            uri: `${config.esHostUrl}/telemetry/trackingData/${trackingDataItem.data.id}`,
                                            method: 'get',
                                            json: true
                                        }).spread((res, body)=> body)
                                    }))
                                    .then((trackingDataFromEs)=> {
                                        expect(trackingDataFromEs.length).to.equal(config.bufferCount)
                                        done()
                                    })

                            },
                            done
                        )
                },
                done
            )

        })

        it('should be able to cope with 1000 trackingData messages under 6 secs', (done)=> {

            const docs = config.bufferCount * 50

            postTrackingData(docs)
                .then(()=> {
                    const begin = new Date()
                    EsSync.start(global.amqpConnection, config)
                        .bufferWithCount(docs / config.bufferCount)
                        .subscribe(
                            (events)=> {
                                const end = new Date()
                                const duration = end - begin;
                                console.log(`time took : ${duration}`)
                                expect(duration).to.be.below(6000)
                                done()
                            },
                            done
                        )
                })

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


