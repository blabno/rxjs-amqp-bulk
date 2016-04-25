const chai = require('chai')
const expect = chai.expect

const Promise = require('bluebird')
const _ = require('lodash/fp')
const url = require('url')
const uuid = require('node-uuid')
const $http = require('http-as-promised')

const sinon = require('sinon')

const Api = require('../lib/api')
const amqpConnectionFactory = require('../lib/amqp-connection')
const config = require('../config')

const monkeypatch = require('monkeypatch')

Promise.longStackTraces()
chai.config.includeStack = true

const amqp = require('amqplib')
const Rx = require('rx')
const RxNode = require('rx-node')
const RxAmqp = require('../lib/rx-amqp')

var nock = require('nock')

const equipmentId = uuid.v4()

const dealerId = uuid.v4()

describe('AMQP Elasticsearch bulk sync', ()=> {

    beforeEach(function () {

        nock.cleanAll()
        nock.enableNetConnect()

        global.amqpConnection = amqpConnectionFactory.connect(config)

        this.esSync = require('../lib/es-sync')(config, amqpConnection)

        return amqp.connect(config.amqpUrl)
            .then((conn) => conn.createChannel())
            .then((ch)=> {
                return Promise.resolve()
                    .then(()=> ch.deleteQueue('es-sync-queue'))
                    .then(()=> ch.deleteQueue('es-mass-reindex-queue'))
                    .then(()=> ch.deleteQueue('es-sync-error-queue'))
                    .then(()=> ch.deleteQueue('es-sync-retry-queue'))
                    .then(()=> ch.deleteExchange('es-sync-retry-exchange'))
                    .then(()=> ch.deleteExchange('es-sync-loop-exchange'))
                    .then(()=> ch.deleteExchange('change-events-exchange'))
                    .then(()=> ch.close())
            })
            .then(() => {
                return require('../lib/amqp-destinations').setup(config)
            })
    })

    afterEach(function () {
        return global.amqpConnection.close()
    })

    describe('AMQP rxjs', ()=> {

        it('feeds amqp messages with associated channel information and an ack/nack shorthands into an observer', function (done) {

            const subscription = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', config.esSyncQueuePrefetch)
                .subscribe(
                    (event)=> {
                        event.channel.ack(event.msg)
                        if (JSON.parse(event.msg.content).attributes.canVariableValue === config.bufferCount - 1) {
                            expect(event.channel).to.not.be.null
                            expect(event.msg).to.not.be.null
                            subscription.dispose()
                            done()
                        }
                    }
                )

            const sendChannel = global.amqpConnection.createChannel({json: true})
            trackingData(config.bufferCount).forEach((trackingDataMessage) => {
                return sendChannel.publish('change-events-exchange', 'trackingData.insert', trackingDataMessage)
            })

        })
    })

    function randomRangeMax() {
        return _.random(1, 200)
    }

    describe('End to End', function () {

        const esSetup = require('../lib/es-setup')(config)

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
                return Api.bootstrap(global.amqpConnection, config)
                    .then((server)=> {
                        global.server = server
                        global.adapter = server.plugins['hapi-harvester'].adapter
                        return server.start()
                    })
            }

            function clearData() {
                const models = global.adapter.models
                const removeModels = _.map((model)=> models[model].remove({}).exec())
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


        afterEach(function () {
            return global.server.stop()
        })

        it('should buffer changes, enrich and bulk sync to ES', function (done) {

            const numberOfTrackingData = randomRangeMax()
            postTrackingData(numberOfTrackingData).then((trackingData)=> {

                    const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', config.esSyncQueuePrefetch)
                    const subscription = processUntilComplete(this.esSync.pipelineOnline(esQueueObservable), numberOfTrackingData)
                        .doOnCompleted(()=> {
                            verifyResultsInEsAndDispose(trackingData, subscription).then(done)
                            //todo check whether queue is empty
                        })
                        .doOnError(done)
                        .subscribe()
                },
                done
            )
        })

        it('should ack the messages', function (done) {

            const numberOfTrackingData = randomRangeMax()
            postTrackingData(numberOfTrackingData).then((trackingData)=> {

                const eventsWithSinon = []
                const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', config.esSyncQueuePrefetch)
                    .map((event)=> {
                        // only wrap it once, all events should carry the same channel
                        if (!event.channel.ack.isSinonProxy) {
                            sinon.spy(event.channel, 'ack')
                        }
                        eventsWithSinon.push(event)
                        return event
                    })

                const subscription = processUntilComplete(this.esSync.pipelineOnline(esQueueObservable), numberOfTrackingData)
                    .doOnCompleted(()=> {
                        const first = _.first(eventsWithSinon)
                        expect(first.channel.ack.callCount).to.equal(numberOfTrackingData)
                        subscription.dispose()
                        done()
                    })
                    .doOnError(done)
                    .subscribe()
            }, done)
        })

        it('should send the errors to the retry queue on runtime failure', function (done) {

            const numberOfTrackingData = randomRangeMax()
            amqp.connect(config.amqpUrl)
                .then((conn) => conn.createChannel())
                .then((ch)=> {
                    return Promise.resolve()
                        // define and bind es-sync-loop-tap-queue so we can listen in on retries during tests
                        .then(()=> ch.deleteQueue('es-sync-loop-tap-queue'))
                        .then(()=> ch.assertQueue('es-sync-loop-tap-queue'))
                        .then(()=> ch.bindQueue('es-sync-loop-tap-queue', 'es-sync-loop-exchange', '#'))
                        .then(()=> ch.close())
                })
                .then(()=>postTrackingData(numberOfTrackingData))
                .then((trackingData)=> {

                    var i = 0
                    const retries = 5
                    const apiChunkFactor = Math.ceil(config.bufferCount / config.apiFetchSize)

                    nock(config.appHostUrl)
                        .persist()
                        .get(/trackingData.*/)
                        .times(retries)
                        .reply(500, function () {
                            i++
                            if (i == retries * apiChunkFactor /* each buffer emitted will result in a <chunkfactor> number of api fetches */) {
                                nock.cleanAll()
                            }
                        })

                    const retriedNumberOfMessages = retries * config.bufferCount
                    const tapQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-loop-tap-queue')
                        //.doOnNext((event)=>console.log('retry msg ' + event.msg.content.toString()))
                        .take(retriedNumberOfMessages)
                        .bufferWithCount(retriedNumberOfMessages)
                    // the tap queue should have received <retriedNumberOfMessages> messages since a <retries> number of fails where forced with nock
                    // a failure sends all of the buffered messages to the retry exchange

                    const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', config.esSyncQueuePrefetch)
                    const esSyncObservable = processUntilComplete(this.esSync.pipelineOnline(esQueueObservable), numberOfTrackingData)

                    Rx.Observable.forkJoin(tapQueueObservable, esSyncObservable)
                        .doOnNext((events)=> {
                            const tapQueueEvents = events[0]
                            expect(tapQueueEvents).to.have.lengthOf(retriedNumberOfMessages)
                        })
                        .doOnCompleted((events)=> done())
                        .subscribe()
                })

        })

        it('should gracefully recover when the AMQP connection fails before messages are ack\'ed', function (done) {

            var i = 0
            monkeypatch(this.esSync, 'enrichBufferAndSync', function (original, events) {
                return Promise.resolve()
                    .then(()=> {
                        i++
                        if (i < 3) {
                            _.first(events).channel.connection.stream.destroy()
                        }
                    })
                    .then(()=> original(events))
            })

            postTrackingData(config.bufferCount).then((trackingData)=> {

                    const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', config.esSyncQueuePrefetch)
                    const subscription = this.esSync.pipelineOnline(esQueueObservable)
                        .skip(2) // ack on a broken channel/connection doesn't result in an error so skip the first observed value batches
                        .subscribe(
                            ()=> verifyResultsInEsAndDispose(trackingData, subscription).then(done),
                            done
                        )
                }
            )

        })

        it('should dispose the pipeline and reprocess the same message when sendToRetryQueue fails', function (done) {


            const numberOfTrackingData = randomRangeMax()
            postTrackingData(numberOfTrackingData).then((trackingData)=> {

                nock(config.appHostUrl)
                    .persist()
                    .get(/trackingData.*/)
                    .reply(500, function () {
                        nock.cleanAll()
                    })

                monkeypatch(this.esSync, 'sendToRetryQueue', function (original, events) {
                    return Promise.resolve()
                        .then(()=> {
                            throw new Error('force error in sendToRetryQueue')
                        })
                        .then(()=> original(events))
                })

                const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', config.esSyncQueuePrefetch)
                const subscription = processUntilComplete(this.esSync.pipelineOnline(esQueueObservable), numberOfTrackingData)
                    .doOnCompleted(()=> verifyResultsInEsAndDispose(trackingData, subscription).then(done))
                    .doOnError(done)
                    .subscribe()
            })

        })

        it('should park messages with functional errors on the error queue', function (done) {

            const numberOfTrackingData = randomRangeMax()
            postTrackingData(numberOfTrackingData).then((trackingData)=> {

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

                            const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', config.esSyncQueuePrefetch)
                            const esSyncSubscription = this.esSync.pipelineOnline(esQueueObservable).subscribe()

                            processUntilComplete(RxAmqp.queueObservable(amqpConnection, 'es-sync-error-queue', {noAck: true}), numberOfTrackingData)
                                .doOnCompleted(()=> {
                                    esSyncSubscription.dispose()
                                    done()
                                })
                                .doOnError(done)
                                .subscribe()
                        })
                }
            )

        })

        it('should be able to cope with 999 trackingData messages under 6 secs', function (done) {

            const numberOfTrackingData = 999

            postTrackingData(numberOfTrackingData)
                .then(()=> {
                    const begin = new Date()

                    const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', config.esSyncQueuePrefetch)
                    const subscription = processUntilComplete(this.esSync.pipelineOnline(esQueueObservable), numberOfTrackingData)
                        .doOnCompleted(()=> {
                            const end = new Date()
                            const duration = end - begin
                            console.log(`time took : ${duration}`)
                            expect(duration).to.be.below(6000)
                            done()
                        })
                        .doOnError(done)
                        .subscribe()
                })

        })

        it('should support a mass reindex', function (done) {
            const numberOfTrackingData = 4113
            const batchSize = 1000
            postTrackingData(numberOfTrackingData)
                .then(()=> {

                        const channel = amqpConnection.createChannel({json: true})

                        const stream = adapter.models.trackingData.find({}, '_id').lean().batchSize(batchSize).stream()
                        const mongodbStreamSubscription = RxNode.fromStream(stream)
                            .map((trackingDataIdObject) => trackingDataIdObject._id)
                            .bufferWithTimeOrCount(10000, batchSize)
                            .doOnNext((trackingDataIds)=>channel.sendToQueue('es-mass-reindex-queue', trackingDataIds))
                            .subscribe()

                        const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-mass-reindex-queue', config.esSyncQueuePrefetch)
                        processUntilComplete(this.esSync.pipelineMassReindex(esQueueObservable), numberOfTrackingData)
                            .doOnCompleted(()=> {
                                mongodbStreamSubscription.dispose()
                                done()
                            })
                            .doOnError(done)
                            .subscribe()
                    },
                    done)
        })
    })
})

function verifyResultsInEsAndDispose(trackingData, subscription) {
    return Promise.all(lookupTrackingDataInEs((trackingData)))
        .then((trackingDataFromEs)=> {
            expect(trackingDataFromEs.length).to.equal(trackingData.length)
            expect(_.map('_source.attributes')(trackingDataFromEs)).to.not.be.undefined
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

function processUntilComplete(observable, shouldBeProcessed) {
    return observable.scan((acc, events)=> acc + events.length, 0)
        .takeWhile((processed)=>processed < shouldBeProcessed)
}

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


