const Hapi = require('hapi'),
    harvester = require('hapi-harvester'),
    Joi = require('joi'),
    url = require('url'),
    _ = require('lodash/fp'),
    Promise = require('bluebird')

module.exports.start = (amqpConnection, config) => {

    const server = new Hapi.Server()
    server.connection({port: config.appHostPort})

    return server.register([
            {
                register: harvester,
                options: {
                    adapter: harvester.getAdapter('mongodb')(config.mongodbUrl),
                    adapterSSE: harvester.getAdapter('mongodb/sse')(config.mongodbOplogUrl)
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
                    equipment: {data: {type: 'equipment'}}
                }
            }

            const equipment = {
                type: 'equipment',
                attributes: {
                    identificationNumber: Joi.string().description('...')
                }
            }

            const hh = server.plugins['hapi-harvester']

            const channelWrapper = amqpConnection.createChannel({
                json: true,
                setup: function (channel) {
                    return channel.assertExchange('change.events', 'topic')
                }
            })

            const routes = _(hh.routes.all(equipment))
                .concat(hh.routes.pick(trackingData, ['get', 'getById']))
                .concat(
                    _.merge(hh.routes.post(trackingData), {
                        config: {
                            ext: {
                                onPostHandler: {
                                    method: function (req, reply) {
                                        channelWrapper.publish('change.events', 'trackingData.insert', req.payload)
                                            .then(()=> reply.continue())
                                    }
                                }
                            }
                        }
                    })
                )

            routes.forEach((route)=> {
                server.route(route)
            })
        })
        .then(()=> {
            return server.start();
        })
        .then(() => server)

}

