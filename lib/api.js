const Hapi = require('hapi'),
    harvester = require('hapi-harvester'),
    Joi = require('joi'),
    url = require('url'),
    _ = require('lodash/fp'),
    Promise = require('bluebird')

module.exports.bootstrap = (amqpConnection, config) => {

    const server = new Hapi.Server()
    server.connection({port: config.appHostPort})

    return server.register([
            {
                register: harvester,
                options: {
                    adapter: harvester.getAdapter('mongodb')(config.mongodbUrl)
                }
            }
        ])
        .then(()=> {

            const users = {
                type: 'users',
                attributes: {
                    status: Joi.string(),
                    name: Joi.string(),
                    firstName: Joi.string()
                }
            }

            const hh = server.plugins['hapi-harvester']

            const channelWrapper = amqpConnection.createChannel({json: true})

            const routes = hh.routes.all(users)
            routes.forEach((route)=> {
                server.route(route)
            })
        })
        .then(()=> server)

}

