const url = require('url')
const $http = require('http-as-promised')

const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname

const mapping = {
    trackingData: {
        properties: {
            id: {
                type: "string",
                index: "not_analyzed"
            },
            heading: {
                type: "long"
            },
            timeOfOccurrence: {
                type: "date"
            },
            timeOfReception: {
                type: "date"
            },
            canVariableValue: {
                type: "long"
            },
            canVariable: {
                type: "nested",
                properties: {
                    id: {
                        type: "string",
                        index: "not_analyzed"
                    },
                    canId: {
                        type: "string",
                        index: "not_analyzed"
                    },
                    name: {
                        type: "string",
                        index: "not_analyzed"
                    }

                }
            },
            equipment: {
                type: "nested",
                properties: {
                    id: {
                        type: "string",
                        index: "not_analyzed"
                    },
                    identificationNumber: {
                        type: "string",
                        index: "not_analyzed"
                    }
                }
            }
        }
    }
};

function deleteIndex() {
    return $http({
        uri: `http://${dockerHostName}:9200/telemetry`,
        method: 'delete'
    })
}

function putIndex() {
    return $http({
        uri: `http://${dockerHostName}:9200/telemetry`,
        method: 'put',
        body: ""
    })
}

function putMapping() {
    const mappingStr = JSON.stringify(mapping)
    return $http({
        uri: `http://${dockerHostName}:9200/telemetry/trackingData/_mapping`,
        method: 'put',
        body: mappingStr
    })
}

module.exports = {mapping, putMapping, putIndex, deleteIndex}

