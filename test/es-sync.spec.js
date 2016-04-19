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
const config = require('../config')

const monkeypatch = require('monkeypatch');

Promise.longStackTraces()
chai.config.includeStack = true

const amqp = require('amqplib')
const Rx = require('rx')
const RxAmqp = require('../lib/rx-amqp')

var nock = require('nock');

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

            const subscription = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', {}, config.esSyncQueuePrefetch)
                .subscribe(
                    (event)=> {
                        event.source.channel.ack(event.source.msg)
                        const content = JSON.parse(event.source.msg.content.toString())
                        if (content.attributes.canVariableValue === config.bufferCount - 1) {
                            expect(event.source.channel).to.not.be.null
                            expect(event.source.msg).to.not.be.null
                            subscription.dispose()
                            done()
                        }
                    }
                );

            const sendChannel = global.amqpConnection.createChannel({json: true})
            trackingData(config.bufferCount).forEach((trackingDataMessage) => {
                return sendChannel.publish('change-events-exchange', 'trackingData.insert', trackingDataMessage)
            })

        })
    })

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


        afterEach(function () {
            return global.server.stop()
        })

        it('should buffer changes, enrich and bulk sync to ES', function (done) {

            postTrackingData(config.bufferCount).then((trackingData)=> {

                    const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', {}, config.esSyncQueuePrefetch)
                    const subscription = this.esSync.pipeline(esQueueObservable)
                        .subscribe(
                            (events)=> {
                                verifyResultsInEsAndDispose(trackingData, subscription)
                                    .then(done)
                                //todo check whether queue is empty
                            },
                            done
                        );
                },
                done
            )

        })

        it('should ack the messages', function (done) {

            postTrackingData(config.bufferCount).then((trackingData)=> {

                const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', {}, config.esSyncQueuePrefetch)
                    .map((event)=> {
                        // only wrap it once, all events should carry the same channel
                        if (!event.source.channel.ack.isSinonProxy) {
                            sinon.spy(event.source.channel, 'ack')
                        }
                        return event
                    })

                const subscription = this.esSync.pipeline(esQueueObservable)
                    .subscribe((events)=> {
                            const first = _.first(events);
                            expect(first.source.channel.ack.callCount).to.equal(20)
                            subscription.dispose()
                            done()
                        },
                        done);
            }, done)
        })

        it('should send the errors to the retry queue on runtime failure', function (done) {

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
                .then(()=>postTrackingData(config.bufferCount))
                .then((trackingData)=> {

                        var i = 0
                        const first = _.first(trackingData)
                        const retries = 5

                        nock(config.appHostUrl, {allowUnmocked: true})
                            .persist()
                            .get(`/trackingData/${first.data.id}?include=equipment,equipment.dealer`)
                            .times(retries)
                            .reply(500, function () {
                                i++
                                if (i == retries) {
                                    nock.cleanAll()
                                }
                            })

                        const tapQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-loop-tap-queue', {}, config.esSyncQueuePrefetch)
                            //.doOnNext((event)=>console.log('retry msg ' + event.source.msg.content.toString()))
                            .take(retries)
                            .bufferWithCount(retries) // the tap queue should have received 3 messages since 3 fails where forced with nock

                        const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', {}, config.esSyncQueuePrefetch)
                        const esSyncObservable = this.esSync.pipeline(esQueueObservable)
                            .where((x)=> x.length > 0) // filter out any emitted buffers which don't carry events
                            //.doOnNext((events)=>console.log('events batch size ' + events.length))
                            .take(2)
                            .bufferWithCount(2)

                        Rx.Observable.forkJoin(tapQueueObservable, esSyncObservable)
                            .subscribe(
                                (events)=> {
                                    const tapQueueEvents = events[0]
                                    expect(tapQueueEvents).to.have.lengthOf(retries)
                                    const esSyncEvents = events[1]
                                    expect(esSyncEvents[0]).to.have.lengthOf(19) // first batch of successful events : 20 trackingData events - 1 failure leaves 19 events
                                    expect(esSyncEvents[1]).to.have.lengthOf(1) // after 4 forced fails the final batch of events contains the eventual success event
                                    //todo verify queue depth is zero for both es-sync and retry queues
                                    done()
                                },
                                done
                            )
                    }
                )

        })

        it('should gracefully recover when the AMQP connection fails before messages are ack\'ed', function (done) {

            var i = 0
            monkeypatch(this.esSync, 'enrichBufferAndSync', function (original, events) {
                return Promise.resolve()
                    .then(()=> {
                        i++
                        if (i < 3) {
                            _.first(events).source.channel.connection.stream.destroy()
                        }
                    })
                    .then(()=> original(events))
            })

            postTrackingData(config.bufferCount).then((trackingData)=> {

                    const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', {}, config.esSyncQueuePrefetch)
                    const subscription = this.esSync.pipeline(esQueueObservable)
                        .skip(2) // ack on a broken channel/connection doesn't result in an error so skip the first observed value batches
                        .subscribe(
                            ()=> verifyResultsInEsAndDispose(trackingData, subscription).then(done),
                            done
                        );
                }
            )

        })

        it('should park messages with functional errors on the error queue', function (done) {

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
                            const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', {}, config.esSyncQueuePrefetch)
                            const subscription = this.esSync.pipeline(esQueueObservable)
                                .subscribe(
                                    ()=> {
                                        amqpQueueBrowseObserver('es-sync-error-queue')
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

        it('should be able to cope with 1000 trackingData messages under 6 secs', function (done) {

            const docs = config.bufferCount * 50

            postTrackingData(docs)
                .then(()=> {
                    const begin = new Date()

                    const esQueueObservable = RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', {}, config.esSyncQueuePrefetch)
                    const subscription = this.esSync.pipeline(esQueueObservable)
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


