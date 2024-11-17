const amqplib = require('amqplib')
let _connection = null
let _channel = getChannel('eb-node-express-2')
createQueues().then()

process.once('SIGINT', () => {
  console.log('SIGINT', 'closing connection')
  try {
    _connection && _connection.close()
  } catch (e) {
    console.error('ERROR SIGINT', e.message)
  }
})

function createQueues () {
  return _createQueues('E', 'positions', 'P', 'events', 'E')
}

async function _createQueues (exchange, positionsQueue, positionsKey, eventsQueue, eventsKey) {
  try {
    const channel = await _channel
    channel.assertExchange(exchange, 'direct', { durable: true, autoDelete: false })
    await createQueue(positionsQueue, exchange, positionsKey)
    await createQueue(eventsQueue, exchange, eventsKey)
  } catch (e) {
    console.error('ERROR', 'createQueues', e.message)
  }
}

async function createQueue (queueName, exchange, routingKey) {
  const channel = await _channel
  const deadLetter = 'z_dead_' + queueName
  await channel.assertQueue(deadLetter, { durable: true })
  await channel.assertQueue(queueName, { durable: true, deadLetterExchange: '', deadLetterRoutingKey: deadLetter })
  await channel.bindQueue(queueName, exchange, routingKey)
}
async function getChannel (name) {
  if (_connection) {
    try {
      await _connection.close()
    } catch (e) {
      console.error('ERROR closing connection', e.message)
    }
  }
  _connection = await amqplib.connect({
    hostname: 'b-8585b2dd-1dc6-4ee8-a83f-00b7e7805494.mq.us-east-1.amazonaws.com',
    port: 5671,
    protocol: 'amqps',
    username: 'rabbit',
    password: process.env.RABBIT_PASS
  }, {
    clientProperties: { connection_name: name }
  })
  return _connection.createConfirmChannel()
}

async function tryChannel (name, retries = 2) {
  try {
    return await _channel
  } catch (e) {
    console.error('ERROR tryChannel, retries: ', retries, e.message)
    if (--retries) {
      await reCreateChannel(name)
      return tryChannel(name, retries)
    } else { throw e }
  }
}

async function reCreateChannel (name) {
  try {
    (await _channel).close()
  } catch (e) {
    console.error('ERROR closing channel', e.message)
  }
  _channel = getChannel(name)
  await createQueues()
}

const send = async (message, exchange = 'E', routingKey = 'P', name = 'eb-node-express-positions', retries = 3) => {
  try {
    const channel = await tryChannel(name)
    channel.publish(exchange, routingKey, Buffer.from(message), { persistent: true })
    await channel.waitForConfirms()
  } catch (e) {
    console.log(message)
    console.error(retries, e.message)
    if (--retries) {
      await reCreateChannel(name)
      return send(message, exchange, routingKey, name, retries)
    } else {
      throw e
    }
  }
}

exports.send = send
