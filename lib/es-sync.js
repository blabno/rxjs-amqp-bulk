const Promise = require('bluebird')
const _ = require('lodash/fp')
const $http = require('http-as-promised')
const url = require('url')


module.exports = (config, amqpConnection) => {

    const sendChannel = amqpConnection.createChannel();

    return {

        pipeline(esQueueObservable) {

            return esQueueObservable
                .bufferWithTimeOrCount(config.bufferTimeout, config.bufferCount)
                .doOnNext((events)=> console.log(`${events.length} events were emitted from the buffer`))
                .flatMap((events) => this.enrichBufferAndSync(events))
                .retryWhen((errors) => {
                    return errors.do(console.error).delay(config.pipelineFailRetry)
                })
        },

        enrichBufferAndSync(events) {
            return Promise.resolve(events)
                .map(this.fetchTrackingDataComposite)
                .then(this.syncBufferedToEs)
                .then(this.ack(events))
        },

        fetchTrackingDataComposite(event) {
            const msg = event.source.msg;
            const trackingData = JSON.parse(msg.content.toString());

            return Promise.resolve().then(()=> {
                return $http.get({
                        // todo referring to id with _id due to a bug in hapi-harvester
                        uri: `${config.appHostUrl}/trackingData/${trackingData.data._id}?include=equipment,equipment.dealer`,
                        json: true
                    })
                    .spread((res, body)=> {
                        // check for functional error
                        if (!_.find({type: 'dealers'}, body.included)) {
                            // send the contents to the dlq
                            return sendChannel.sendToQueue('es-sync-error-queue', msg.content)
                        } else {
                            // return the original event with added result
                            return _.merge(event, {result: body})
                        }
                    })
                    .catch((e)=> {
                        // todo check what happens if a runtime error is raised here in catch
                        console.error('Fetch composite document failed', e)
                        // send the msg to the es retry queue on runtime failure with an incremental backoff
                        return logAndRetry(msg, sendChannel);
                    })
            })
        },

        syncBufferedToEs(events) {

            const eventsWithResults = _.filter()('result', events)
            console.log(`${eventsWithResults.length} events yielded a result and will be fed into Elasticsearch`)

            return Promise.resolve().then(()=> {

                    if (eventsWithResults.length > 0) {

                        const transformToBulkDocumentItems = _.map((eventWithResult)=> {
                            const trackingDataComposite = eventWithResult.result
                            const action_and_meta_data = JSON.stringify({index: {_id: trackingDataComposite.data.id}})
                            const source = JSON.stringify(denormalize(trackingDataComposite))
                            return `${action_and_meta_data}\n${source}`
                        })

                        const bulkDocumentItems = transformToBulkDocumentItems(eventsWithResults);

                        const bulkDocument = bulkDocumentItems.join('\n').concat('\n');
                        return $http({
                            uri: `${config.esHostUrl}/telemetry/trackingData/_bulk`,
                            method: 'post',
                            body: bulkDocument
                        }).then(()=> eventsWithResults)

                    } else {
                        return []
                    }

                })
                .catch((e)=> {
                    console.error('Elasticsearch bulk insert failed ', e)
                    const sendAllToWaitQueue = _.map((event)=> {
                        return logAndRetry(event.source.msg, sendChannel);
                    })
                    return Promise.all(sendAllToWaitQueue(eventsWithResults))
                })


            function denormalize(result) {
                const equipment = _.find(_.matches({type: 'equipment'}))(result.included)
                const dealer = _.find(_.matches({type: 'dealers'}))(result.included)
                const merged = _.merge(result.data, {
                    attributes: {
                        equipment: equipment.attributes,
                        dealer: dealer.attributes
                    }
                })
                return _.omit(['relationships'])(merged)
            }

        },

        ack(events) {

            return (eventsWithSuccess) => {
                const ackEvents = _.map((event)=> {
                    const source = event.source;
                    const deliveryTag = source.msg.fields.deliveryTag
                    const trackingDataId = JSON.parse(source.msg.content.toString()).data._id
                    console.log(`ack for message with deliveryTag ${deliveryTag} and trackingData id ${trackingDataId}`)
                    return source.channel.ack(source.msg)
                })

                return Promise.all(ackEvents(events))
                    .then(()=> eventsWithSuccess)
            }

        }
    }

    function logAndRetry(msg, sendChannel) {
        const expiration = calculateExpiration(msg.properties.headers);
        console.log(`sending trackingData msg with id ${JSON.parse(msg.content.toString()).data._id} to the retry queue, expiration set to ${expiration} ms`)
        return sendChannel.publish('es-sync-retry-exchange', msg.fields.routingKey, msg.content, {expiration})
    }

    function calculateExpiration(headers) {
        if (headers["x-death"]) {
            const candidateExpiration = (headers["x-death"][0]["original-expiration"] * config.retryMultiply)
            return (candidateExpiration > config.retryMax ) ? config.retryMax : candidateExpiration
        } else {
            return config.retryInitial
        }
    }

}





