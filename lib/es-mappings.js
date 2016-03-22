const url = require('url')
const $http = require('http-as-promised')

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

function deleteIndex(config) {
    return $http({
        uri: `${config.esHostUrl}/telemetry`,
        method: 'delete'
    })
}

function putIndex(config) {
    return $http({
        uri: `${config.esHostUrl}/telemetry`,
        method: 'put',
        body: ""
    })
}

function putMapping(config) {
    return $http({
        uri: `${config.esHostUrl}/telemetry/trackingData/_mapping`,
        method: 'put',
        body: JSON.stringify(mapping)
    })
}

module.exports = {mapping, putMapping, putIndex, deleteIndex}

