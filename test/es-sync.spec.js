const chai = require('chai')
const expect = chai.expect

const Promise = require('bluebird')
const amqp = require('amqp-connection-manager')
const _ = require('lodash')
const Rx = require('rx')
const url = require('url')
const spies = require('chai-spies')
const EsSync = require('../lib/es-sync')
const EsMappings = require('../lib/es-mappings')
const Api = require('../lib/api')
const uuid = require('node-uuid');
const amqpConnectionFactory = require('../lib/amqp-connection');
const $http = require('http-as-promised')

Promise.longStackTraces()
chai.config.includeStack = true
chai.use(spies);

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname

describe('AMQP Elasticsearch bulk sync', ()=> {

    var connection

    const equipmentId = uuid.v4();

    function trackingData(rangeMax) {
        var rangeMax = rangeMax || 5
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

    describe('Pipeline rxjs', ()=> {

        it('should execute the syncBufferedToEs function with the results and ack the source messages', (done)=> {

            const ack = chai.spy(()=> {
            })
            const nack = chai.spy(()=> {
            })

            const queueObserver = () => fakeQueueObservableWithSpies(trackingData(), ack, nack);

            EsSync.esBulkSyncPipeline(queueObserver, 5,
                (source)=> Promise.resolve(),
                (settled)=> Promise.resolve(settled))
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

            const queueObserver = ()=> fakeQueueObservableWithSpies(trackingData(), ack, nack);

            const subscription = EsSync.esBulkSyncPipeline(queueObserver, 5,
                (event) => {
                    const trackingDataItem = JSON.parse(event.content.toString())
                    return Promise.resolve().then(()=> {
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

        it('should retry on syncBufferedToEs failure', (done)=> {

            const ack = chai.spy(()=> {
            })
            const nack = chai.spy(()=> {
            })

            const queueObserver = () => fakeQueueObservableWithSpies(trackingData(), ack, nack);

            var i = 0
            const subscription = EsSync.esBulkSyncPipeline(queueObserver, 5, ()=> Promise.resolve(), (settled)=> {
                if (i++ < 4) {
                    return Promise.reject(`force reject ${i}`)
                } else {
                    return Promise.resolve(settled)
                }
            })

            subscription
                .do(
                    () => done(),
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
            });
        }
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

    describe('AMQP rxjs', ()=> {

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
            trackingData().forEach((trackingDataMessage) => {
                return sendChannel.publish('change.events', 'trackingData.insert', trackingDataMessage)
            })

        })
    })

    describe('End to End', ()=> {

        beforeEach(() => {
            return Api.start()
                .then((server)=> {
                    global.server = server
                    global.adapter = server.plugins['hapi-harvester'].adapter;
                })
                .then(()=> clearData)
                .then(()=> createEquipment())
                .then(()=> EsMappings.deleteIndex())
                .then(()=> EsMappings.putIndex())
                .then(()=> EsMappings.putMapping())

            function clearData() {
                const models = global.adapter.models;
                return Promise.all(_.map(['equipment', 'trackingData'], (model)=> {
                    return models[model].remove({}).lean().exec()
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
            global.server.stop()
        })

        it('should buffer changes, parallel enrich and push to ES', (done)=> {

            postTrackingData(5).then(
                (trackingData)=> {

                    const bufferThreshold = 5

                    EsSync.start(bufferThreshold)
                        .subscribe(
                            ()=> {
                                Promise.all(_.map(trackingData, (trackingDataItem)=> {
                                        return $http({
                                            uri: `http://${dockerHostName}:9200/telemetry/trackingData/${trackingDataItem.data.id}`,
                                            method: 'get',
                                            json: true
                                        }).spread((res, body)=>body)
                                    }))
                                    .then((trackingDataFromEs)=> {
                                        expect(trackingDataFromEs.length).to.equal(5)
                                        done()
                                    })

                            },
                            done
                        )
                },
                done
            )

        })

        it('should be able to cope with 100s of trackingData messages', (done)=> {

            const docs = 200
            const bufferThreshold = 20

            postTrackingData(docs)
                .then(()=> {

                    const begin = new Date()
                    EsSync.start(bufferThreshold)
                        .bufferWithCount(docs / bufferThreshold)
                        .subscribe(
                            (events)=> {
                                const end = new Date()
                                console.log(`time took : ${end - begin}`)
                                done()
                            },
                            done
                        )
                })

        })

        function postTrackingData(maxRange) {

            return Promise
                .map(trackingData(maxRange), post, {concurrency: 5})

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


