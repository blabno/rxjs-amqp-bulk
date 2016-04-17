const config = require('./config');

const Api = require('../lib/api');
const AmqpConnectionFactory = require('../lib/amqp-connection');
const EsSync = require('../lib/es-sync');
const EsSetup = require('../lib/es-setup');

const esSync = EsSync(config);
const esSetup = EsSetup(config);

const subscription = esSync.pipeline(esSync.esQueueObservable(amqpConnection)).subscribe(events => verifyResultsInEsAndDispose(trackingData, subscription).then(done), done);
//# sourceMappingURL=index.js.map