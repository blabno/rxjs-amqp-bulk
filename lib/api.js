const Hapi = require('hapi'),
    harvester = require('hapi-harvester'),
    Joi = require('joi'),
    url = require('url'),
    _ = require('lodash'),
    Promise = require('bluebird')

function start() {

    const server = new Hapi.Server()
    server.connection({port: 3000})

    const dockerHostName = url.parse(process.env.DOCKER_HOST).hostname

    return server.register([
            {
                register: harvester,
                options: {
                    adapter: harvester.getAdapter('mongodb')(`mongodb://${dockerHostName}/test`),
                    adapterSSE: harvester.getAdapter('mongodb/sse')(`mongodb://${dockerHostName}/local`)
                }
            }
        ])
        .then(()=> {

            const trackingData = {
                type: 'trackingData',
                attributes: {
                    heading: Joi.number(),
                    timeOfOccurrence: Joi.date(),
                    timeOfReception: Joi.date(),
                    canVariableValue: Joi.number()
                },
                relationships: {
                    canVariable: {data: {type: 'canVariable'}},
                    equipment: {data: {type: 'equipment'}}
                }
            }

            const canVariables = {
                type: 'canVariables',
                attributes: {
                    canId: Joi.number().description('iso standard canbus identifier'),
                    name: Joi.string().description('...')
                }
            }

            const equipment = {
                type: 'equipment',
                attributes: {
                    identificationNumber: Joi.string().description('...')
                }
            }

            const hh = server.plugins['hapi-harvester']

            const connection = require('./amqp-connection').connect()
            const channelWrapper = connection.createChannel({
                setup: function (channel) {
                    return channel.assertExchange('change.events', 'topic')
                }
            })

            const routes = _.concat(
                hh.routes.all(equipment),
                hh.routes.all(canVariables),
                _.merge({}, hh.routes.post(trackingData), {
                    config: {
                        ext: {
                            onPreHandler: {
                                method: function (req, reply) {
                                    reply(channelWrapper.publish('change.events', 'trackingData.insert', req.payload))
                                }
                            }
                        }
                    }
                })
            );
            routes.forEach((route)=> {
                server.route(route)
            })
        })
        .then(()=> {
            return server.start();
        }).then(() => {
            return server
        })
}

module.exports.start = start