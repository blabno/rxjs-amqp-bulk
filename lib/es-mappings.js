const mapping = {
    "properties": {
        "id": {
            "type": "string",
            "index": "not_analyzed"
        },
        "heading": {
            "type": "long"
        },
        "timeOfOccurrence": {
            "type": "date"
        },
        "timeOfReception": {
            "type": "date"
        },
        "canVariableValue": {
            "type": "long"
        },
        "canVariable": {
            "type": "nested",
            "properties": {
                "id": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "canId": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "name": {
                    "type": "string",
                    "index": "not_analyzed"
                }

            }
        },
        "equipment": {
            "type": "nested",
            "properties": {
                "id": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "identificationNumber": {
                    "type": "string",
                    "index": "not_analyzed"
                }
            }
        }
    }
};

function start() {
    const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname
    return $http({
        uri: `http://${dockerHostName}:9200/telemetry/_mapping/trackingData`,
        method: 'put',
        json: mapping
    })
}

module.exports = {mapping, start}

