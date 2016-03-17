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

            EsSync.esBulkSyncPipeline(queueObserver,
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

            const subscription = EsSync.esBulkSyncPipeline(queueObserver,
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
            const subscription = EsSync.esBulkSyncPipeline(queueObserver, ()=> Promise.resolve(), (settled)=> {
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

        it.only('should buffer changes, parallel enrich and push to ES', (done)=> {

            const pipeline = EsSync.start();

            pipeline
                .subscribe(
                    ()=> {
                        // todo this delay now waits for data to become available for search in ES
                        // replace this with a more deterministic mechanism
                        Promise.delay(1000)
                            .then(()=> {
                                return $http({
                                    uri: `http://${dockerHostName}:9200/telemetry/trackingData/_search`,
                                    method: 'get',
                                    json: true
                                })
                            })
                            .spread((res, body)=> {
                                expect(body.hits.hits.length).to.equal(5)
                                const canVariableValues = _(body.hits.hits).map('_source.attributes.canVariableValue')
                                expect(canVariableValues.includes(0)).to.be.true
                                expect(canVariableValues.includes(1)).to.be.true
                                expect(canVariableValues.includes(2)).to.be.true
                                expect(canVariableValues.includes(3)).to.be.true
                                expect(canVariableValues.includes(4)).to.be.true
                                done()
                            })

                    },
                    done
                )

            postTrackingData().catch(done)

        })

        function postTrackingData(maxRange) {
            return Promise.all(trackingData(maxRange).map((trackingDataMessage) => {
                return $http({
                    uri: `http://localhost:3000/trackingData`,
                    method: 'post',
                    json: {data: trackingDataMessage}
                })
            }))
        }

    })

})


