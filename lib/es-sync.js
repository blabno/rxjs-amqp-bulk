const Promise = require('bluebird')
const _ = require('lodash/fp')
const $http = require('http-as-promised')
const url = require('url')

module.exports = (config, amqpConnection)=> {

    const sendChannel = amqpConnection.createChannel({json: true});

    return {

        pipelineOnline(esQueueObservable) {

            return esQueueObservable
                .map(withJsonContent)
                .bufferWithTimeOrCount(config.bufferTimeout, config.bufferCount)
                .doOnNext((events)=> console.log(`${events.length} trackingData events were emitted from the online pipeline`))
                .filter((events) => events.length > 0)
                .flatMap((events) => this.enrichBufferAndSync(events))
                .retryWhen((errors) => {
                    return errors.do(console.error).delay(config.pipelineOnDisposeRetry)
                })
        },

        pipelineMassReindex(esQueueObservable) {

            return esQueueObservable
                .map(withJsonContent)
                .doOnNext((event)=> console.log(`${event.json.length} trackingData ids were received from the mass reindex pipeline`))
                .filter((event) => event.json.length > 0)
                .flatMap((events) => this.enrichBufferAndSync([events])) // force array to unify online & batch processing
                .retryWhen((errors) => {
                    return errors.do(console.error).delay(config.pipelineOnDisposeRetry)
                })
        },

        enrichBufferAndSync(events) {

            return Promise.resolve(events)
                .then(this.fetchTrackingDataComposite)
                .then(this.syncBufferedToEs)
                .catch((e)=> {
                    console.error('enrichBufferAndSync failed', e)
                    return this.sendToRetryQueue(events);
                })
                .then((successEvents)=> {
                    return this.ack(events).then(()=> successEvents)
                })
        },

        fetchTrackingDataComposite(events) {

            return Promise.resolve().then(()=> {
                const allTrackingDataIds = extractTrackingIds(events)
                const trackingDataIdsChunks = _.chunk(config.apiFetchSize)(allTrackingDataIds)

                return Promise.map(trackingDataIdsChunks, invokeAndProcessTrackingDataGet,
                    {concurrency: config.apiFetchConcurrency}).then(_.flatten)

                function invokeAndProcessTrackingDataGet(trackingDataIdsChunk) {
                    return invokeTrackingDataGetWithInclude(trackingDataIdsChunk)
                        .spread((res, body)=> {
                            const processTrackingData = _.map((trackingDataItem)=> {
                                if (isFunctionalError(trackingDataItem, body.included)) {
                                    return sendChannel.sendToQueue('es-sync-error-queue', trackingDataItem)
                                } else {
                                    return {data: trackingDataItem, included: body.included}
                                }
                            })
                            return processTrackingData(body.data)
                        })
                }
            })
        },

        syncBufferedToEs(events) {

            return Promise.resolve().then(()=> {
                const successEvents = _.filter()('data', events)
                console.log(`${successEvents.length} success events will be fed into Elasticsearch`)
                if (successEvents.length > 0) {
                    const bulkDocumentItems = _.map(toBulkDocumentItem)(successEvents)
                    const bulkDocument = bulkDocumentItems.join('\n').concat('\n')
                    return invokeEsBulk(bulkDocument).then(()=> successEvents)
                } else {
                    return []
                }
            })
        },

        ack(events) {
            const deliveryTags = _.map('msg.fields.deliveryTag', (events))
            const trackingIds = extractTrackingIds(events)
            console.log(`ack for messages with deliveryTag ${deliveryTags} and trackingData ids ${trackingIds}`)
            return Promise.all(_.map(ackEvent)(events))
        },

        sendToRetryQueue(events) {
            console.log(`forwarding trackingData msgs to the retry queue. ids : ${extractTrackingIds(events)}`)
            return Promise.all(_.map(retryAndLog)(events)).then(()=> [])
        }
    }

    function withJsonContent(msgAndChannel) {
        return _.merge(msgAndChannel, {json: JSON.parse(msgAndChannel.msg.content.toString())})
    }

    function extractTrackingIds(events) {
        return _.flow(_.map('json'), _.flatten)(events)
    }

    function emptyBuffers(events) {
        return events.length > 0
    }

    function isFunctionalError(trackingDataItem, included) {
        const equipmentId = _.get('relationships.equipment.data.id', trackingDataItem)
        const equipment = _.find({type: 'equipment', id: equipmentId}, included)
        const dealerId = _.get('relationships.dealer.data.id', equipment)
        return !_.find({type: 'dealers', id: dealerId}, included)
    }

    function invokeTrackingDataGetWithInclude(trackingDataIdsChunk) {
        return $http.get({
            uri: `${config.appHostUrl}/trackingData?filter[id]=${trackingDataIdsChunk.join()}&include=equipment,equipment.dealer`,
            json: true
        });
    }

    function invokeEsBulk(bulkDocument) {
        return $http({
            uri: `${config.esHostUrl}/telemetry/trackingData/_bulk`,
            method: 'post',
            body: bulkDocument
        })
    }

    function toBulkDocumentItem(trackingDataComposite) {
        const action_and_meta_data = JSON.stringify({index: {_id: trackingDataComposite.data.id}})
        const source = JSON.stringify(denormalizeTrackingData(trackingDataComposite))
        return `${action_and_meta_data}\n${source}`
    }

    function denormalizeTrackingData(trackingDataComposite) {
        const equipment = _.find(_.matches({type: 'equipment'}))(trackingDataComposite.included)
        const dealer = _.find(_.matches({type: 'dealers'}))(trackingDataComposite.included)
        const merged = _.merge(trackingDataComposite.data, {
            attributes: {
                equipment: equipment.attributes,
                dealer: dealer.attributes
            }
        })
        return _.omit(['relationships'])(merged)
    }

    function ackEvent(event) {
        return event.channel.ack(event.msg)
    }

    function retryAndLog(event) {
        const msg = event.msg
        const expiration = calculateExpiration(msg.properties.headers);
        return sendChannel.publish('es-sync-retry-exchange', msg.fields.routingKey, event.json, {expiration})
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





