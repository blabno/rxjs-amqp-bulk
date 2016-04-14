const $http = require('http-as-promised')

module.exports = (config) => {

    return {

        deleteIndex() {
            return $http({
                uri: `${config.esHostUrl}/telemetry`,
                method: 'delete',
                error: false
            })
        },

        putIndex() {
            return $http({
                uri: `${config.esHostUrl}/telemetry`,
                method: 'put',
                body: ""
            })
        },

        putMapping() {

            const mapping = {
                trackingData: {
                    properties: {
                        id: {
                            type: "string",
                            index: "not_analyzed"
                        },
                        attributes: {
                            type: "nested",
                            properties: {
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
                                },
                                dealer: {
                                    type: "nested",
                                    properties: {
                                        id: {
                                            type: "string",
                                            index: "not_analyzed"
                                        },
                                        name: {
                                            type: "string",
                                            index: "not_analyzed"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return $http({
                uri: `${config.esHostUrl}/telemetry/trackingData/_mapping`,
                method: 'put',
                body: JSON.stringify(mapping)
            })
        }

    }
}





