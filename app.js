const cluster = require('cluster')
const { sendToTraccar, getCountry } = require('./sync')

if (cluster.isMaster) {
  const cpuCount = require('os').cpus().length
  for (let i = 0; i < cpuCount; i += 1) { cluster.fork() }

  cluster.on('exit', function (worker) {
    console.error(`worker ${worker.id} died :(`)
    cluster.fork()
  })
} else {
  const rabbit = require('./rabbit')
  const sqs = require('./sqs')
  const express = require('express')
  const bodyParser = require('body-parser')
  const app = express()
  app.use(bodyParser.json())

  app.get('/', async (req, res) => {
    // load balancer health check
    res.send(`Worker ${cluster.worker.id} is up!`)
  })

  app.post('/push', async (req, res) => {
    const message = JSON.stringify(req.body)
    try {
      await sqs.sendMessage(message, process.env.SQS_EVENTS_QUEUE)
      res.end()
    } catch (e) {
      console.error(message, e.message)
      res.status(500).end()
    }
  })

  app.post('/pushPositions', async (req, res) => {
    const message = JSON.stringify(req.body)
    try {
      const { device, position } = req.body
      if (device && device.attributes.integration) {
        await sqs.sendMessage(message, process.env.SQS_POSITIONS_QUEUE)
      }
      // ignore forwarded positions (coming from the other traccar and already sent to rabbit)
      if (position.attributes.source !== 'us-east-1-old') {
        position.attributes.source ||= 'eu-west-3'
        await rabbit.send(JSON.stringify(req.body))
      } else {
        // console.log('ignoring  ', position.fixTime, getCountry(position), position.attributes.source, device.name)
      }
      res.end()
    } catch (e) {
      console.error(message, e.message)
      try {
        await sqs.sendMessage(message, process.env.SQS_DLQ)
      } catch (e) {
        console.error(e.message)
      }
      res.status(500).end()
    }
  })

  // this is invoked for events after being processed on the backend
  app.post('/pushRabbit', async (req, res) => {
    const message = JSON.stringify(req.body)
    try {
      await rabbit.send(message, 'E', 'E', 'eb-node-express-events')
      res.end()
    } catch (e) {
      console.error(message, e.message)
      res.status(500).end()
    }
  })

  const port = process.env.PORT || 3000
  app.listen(port, function () {
    console.log(`Worker ${cluster.worker.id} running at http://127.0.0.1:${port}/`)
  })
}
