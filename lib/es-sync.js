const Promise = require('bluebird')
const _ = require('lodash/fp')
const $http = require('http-as-promised')
const url = require('url')

module.exports = (config, amqpConnection)=> {

    const sendChannel = amqpConnection.createChannel({json: true});

    return {

        pipeline(esQueueObservable) {

            return esQueueObservable
                .map(withSourceAndJsonContent)
                .bufferWithTimeOrCount(config.bufferTimeout, config.bufferCount)
                .doOnNext((events)=> console.log(`${events.length} events were emitted from the buffer`))
                .filter(emptyBuffers)
                .flatMap((events) => this.enrichBufferAndSync(events))
                .retryWhen((errors) => {
                    return errors.do(console.error).delay(config.pipelineFailRetry)
                })
        },

        enrichBufferAndSync(events) {
            return Promise.resolve(events)
                .then(this.fetchTrackingDataComposite)
                .then(this.syncBufferedToEs)
                .catch((e)=> { // todo check what happens if a runtime error is raised here in catch ?
                    console.error('enrichBufferAndSync failed', e)
                    return Promise.all(_.map(retryAndLog)(events)).then(()=> [])
                })
                .then((eventsWithResults)=> {
                    return this.ack(events).then(()=> eventsWithResults)
                })
        },

        fetchTrackingDataComposite(events) {
            
            return Promise.resolve().then(()=> {
                const trackingDataIds = _.map('source.json.data._id')(events)
                return invokeTrackingDataGetWithInclude(trackingDataIds)
                    .spread((res, body)=> {
                        return _.map((trackingDataItem)=> {
                            if (isFunctionalError(trackingDataItem, body.included)) {
                                return sendChannel.sendToQueue('es-sync-error-queue', new Buffer(JSON.stringify(trackingDataItem))).then(()=> {return {}})
                            } else {
                                return {result: {data: trackingDataItem, included: body.included}}
                            }
                        })(body.data)
                    })
            })
        },

        syncBufferedToEs(events) {

            return Promise.resolve().then(()=> {
                const eventsWithResults = _.filter()('result', events)
                console.log(`${eventsWithResults.length} events yielded a result and will be fed into Elasticsearch`)
                if (eventsWithResults.length > 0) {
                    const bulkDocumentItems = _.map(toBulkDocumentItem)(eventsWithResults)
                    const bulkDocument = bulkDocumentItems.join('\n').concat('\n')
                    return invokeEsBulk(bulkDocument).then(()=> eventsWithResults)
                } else {
                    return []
                }
            })
        },

        ack(events) {
            return Promise.all(_.map(ackEvent)(events))
        }
    }

    function withSourceAndJsonContent(msgAndChannel) {
        const rawContent = msgAndChannel.msg.content
        msgAndChannel.json = JSON.parse(rawContent.toString())
        return {source: msgAndChannel}
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

    function invokeTrackingDataGetWithInclude(trackingDataIds) {
        const getWithIncludes = `${config.appHostUrl}/trackingData?filter[id]=${trackingDataIds.join()}&include=equipment,equipment.dealer`;
        console.log(`/trackingData?filter[id]=${trackingDataIds.join()}&include=equipment,equipment.dealer`)
        return $http.get({
            uri: getWithIncludes,
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

    function toBulkDocumentItem(eventWithResult) {
        const trackingDataComposite = eventWithResult.result
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
        const source = event.source;
        const deliveryTag = source.msg.fields.deliveryTag
        const trackingDataId = source.json.data._id
        console.log(`ack for message with deliveryTag ${deliveryTag} and trackingData id ${trackingDataId}`)
        return source.channel.ack(source.msg)
    }

    function retryAndLog(event) {
        const source = event.source;
        const msg = source.msg
        const expiration = calculateExpiration(msg.properties.headers);
        console.log(`sending trackingData msg with id ${source.json.data._id} to the retry queue, expiration set to ${expiration} ms`)
        return sendChannel.publish('es-sync-retry-exchange', msg.fields.routingKey, source.json, {expiration})
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





