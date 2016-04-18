const Api = require('./lib/api');
const config = require('./config')
const AmqpConnectionFactory = require('./lib/amqp-connection')
const AmqpDestinations = require('./lib/amqp-destinations')
const EsSync = require('./lib/es-sync')
const RxAmqp = require('./lib/rx-amqp')

const amqpConnection = AmqpConnectionFactory.connect(config)
const esSync = EsSync(config, amqpConnection)

AmqpDestinations.setup(config)
    .then(()=> Api.bootstrap(amqpConnection, config).start())
    .then(()=> esSync.pipeline(RxAmqp.queueObservable(amqpConnection, 'es-sync-queue', {}, config.esSyncQueuePrefetch).subscribe()))