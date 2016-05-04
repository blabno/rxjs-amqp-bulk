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

const patch = require('monkeypatch')

Promise.longStackTraces()
chai.config.includeStack = true

const amqp = require('amqplib')
const Rx = require('rx')
const RxNode = require('rx-node')
const RxAmqp = require('../lib/rx-amqp')

var nock = require('nock')

const equipmentId = uuid.v4()

const userId = uuid.v4()

describe('AMQP event processing', ()=> {

    beforeEach(function () {

        nock.cleanAll()
        nock.enableNetConnect()

        global.amqpConnection = amqpConnectionFactory.connect(config)

        return amqp.connect(config.amqpUrl)
            .then((conn) => conn.createChannel())
            .then((ch)=> {
                return Promise.resolve()
                    .then(()=> ch.deleteQueue('users-retry-queue'))
                    .then(()=> ch.deleteExchange('users-retry'))
                    .then(()=> ch.deleteQueue('users-queue'))
                    .then(()=> ch.deleteExchange('change-events'))
                    .then(()=> ch.close())
            })
            .then(() => {
                return require('../lib/amqp-destinations').setup(config)
            })
    })

    afterEach(function () {
        return global.amqpConnection.close()
    })

    describe('End to End', function () {

        beforeEach(function () {
            return Promise.resolve()
                .then(apiStart)
                .then(clearData)
                .then(createUser)

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
                return Promise.all(removeModels(['users']))
            }

            function createUser() {
                return global.adapter.create('users', {
                    id: userId,
                    type: 'users',
                    attributes: {
                        status: 'activePending',
                        name: 'John',
                        firstName: 'Doe'
                    }
                })
            }

        })


        afterEach(function () {
            return global.server.stop()
        })

        it('should dequeue the users.insert event and feed it to OpenIDM', function (done) {

            const postSpy = sinon.spy();

            nock(config.openIDMHostUrl)
                .persist()
                .post('/users')
                .reply(200, function () {
                    postSpy()
                })

            const sendChannel = global.amqpConnection.createChannel({json: true});
            sendChannel.publish('change-events', 'users.insert', userId)
                .then(()=> {
                        sendChannel.close()
                        const online = require('../lib/online')(config, amqpConnection)

                        online.pipeline()
                            .take(1)
                            .doOnCompleted(()=> {
                                expect(postSpy.callCount).to.equal(1)
                                RxAmqp.queueBrowserObservable('users-queue')
                                    .doOnNext(()=> {
                                        throw new Error('there shouldn\'t be any messages left on the sync queue')
                                    })
                                    .doOnCompleted(done)
                                    .subscribe()
                            })
                            .doOnError(done)
                            .subscribe()
                    },
                    done
                )
        })


        it('should send the errors to the retry queue on runtime failure', function (done) {

            amqp.connect(config.amqpUrl)
                .then((conn) => conn.createChannel())
                .then((ch)=> {
                    return Promise.resolve()
                        // define and bind es-sync-loop-tap-queue so we can listen in on retries during tests
                        .then(()=> ch.deleteQueue('users-retry-tap-queue'))
                        .then(()=> ch.assertQueue('users-retry-tap-queue'))
                        .then(()=> ch.bindQueue('users-retry-tap-queue', 'users-retry', '#'))
                        .then(()=> ch.close())
                })
                .then(()=> {

                    const sendChannel = global.amqpConnection.createChannel({json: true});
                    sendChannel.publish('change-events', 'users.insert', userId)

                    const failures = 4

                    const postSpy = sinon.spy()

                    nock(config.openIDMHostUrl)
                        .post('/users')
                        .times(failures)
                        .reply(500, () => {
                            console.log('forcing 500')
                        })
                        .post('/users')
                        .reply(200, () => {
                            postSpy()
                            nock.cleanAll()
                        })

                    const online = require('../lib/online')(config, amqpConnection)
                    const esSyncObservable = online.pipeline()
                        .take(failures + 1)
                        .bufferWithCount(failures + 1)

                    const tapQueueObservable = RxAmqp.queueObservable(amqpConnection, 'users-retry-tap-queue')
                        .take(failures)
                        .bufferWithCount(failures)

                    Rx.Observable.forkJoin(tapQueueObservable, esSyncObservable)
                        .doOnNext((forkJoinEvents) => {
                            const successEvents = _.compact(forkJoinEvents[1])
                            expect(successEvents.length).to.equal(1)// should all be falsy values except for last success attempt
                            done()
                        })
                        .doOnError(done)
                        .subscribe()

                })
        })

        it('should gracefully recover when the AMQP connection fails before messages are ack\'ed', function (done) {

            const postSpy = sinon.spy();

            nock(config.openIDMHostUrl)
                .persist()
                .post('/users')
                .reply(200, function () {
                    postSpy()
                })

            const sendChannel = global.amqpConnection.createChannel({json: true});
            sendChannel.publish('change-events', 'users.insert', userId)
                .then(()=> {

                        var i = 0

                        const online = require('../lib/online')(config, amqpConnection)

                        patch(online, 'updateOpenIDM', (original, event) => {
                            return Promise.resolve()
                                .then(()=> {
                                    i++
                                    if (i < 3) {
                                        event.channel.connection.stream.destroy()
                                    } else {
                                        online.updateOpenIDM.unpatch()
                                    }
                                })
                                .then(()=> original(event))
                        })


                        const subscription = online.pipeline()
                            .skip(2) // ack on a broken channel/connection doesn't result in an error so skip first 2 attempts
                            .subscribe(
                                ()=> {
                                    expect(postSpy.callCount).to.equal(3)
                                    subscription.dispose()
                                    done()
                                }
                            )
                    }
                )

        })

        it('should dispose the pipeline and reprocess the same message when sendToRetryQueue fails', function (done) {

            const postSpy = sinon.spy()

            nock(config.openIDMHostUrl)
                .post('/users')
                .reply(500, () => {
                    console.log('forcing 500')
                })
                .post('/users')
                .reply(200, () => {
                    postSpy()
                    nock.cleanAll()
                })

            const sendChannel = global.amqpConnection.createChannel({json: true});
            sendChannel.publish('change-events', 'users.insert', userId)
                .then((trackingData)=> {

                    const online = require('../lib/online')(config, amqpConnection)

                    patch(online, 'sendToRetryQueue', (original, events) => {
                        return Promise.resolve()
                            .then(()=> {
                                online.sendToRetryQueue.unpatch()
                                throw new Error('force error in sendToRetryQueue')
                            })
                            .then(()=> original(events))
                    })

                    online.pipeline().take(1)
                        .doOnCompleted(()=> {
                            expect(postSpy.callCount).to.equal(1)
                            done()
                        })
                        .doOnError(done)
                        .subscribe()
                })

        })

    })
})





