'use strict'

const {
  Queue,
  QueueScheduler,
  Worker,
} = require('bullmq')

const fs = require('fs')

const BullBoard = require('bull-board')

const { BullMQAdapter } = require('bull-board/bullMQAdapter')

class BullManager {
  constructor(Logger, Config, jobs, app, resolver) {
    this.Logger = Logger
    this.jobs = jobs

    this.app = app
    this.resolver = resolver

    this._queues = null
    this._shutdowns = []

    this.config = Config.get('bull')
  }

  get queues() {
    if (this._queues) {
      return this._queues
    }

    this._queues = this.jobs.reduce((queues, path) => {
      const jobDefinition = this.app.use(path)

      const queueConfig = {
        connection: this.config.connections[this.config.connection],
        defaultJobOptions: jobDefinition.options,
        ...jobDefinition.queueOptions,
      }

      const jobListeners = this._getEventListener(jobDefinition)

      // eslint-disable-next-line no-new
      new QueueScheduler(jobDefinition.key, queueConfig)

      queues[jobDefinition.key] = Object.freeze({
        bull: new Queue(jobDefinition.key, queueConfig),
        ...jobDefinition,
        instance: jobDefinition,
        listeners: jobListeners,
        boot: jobDefinition.boot,
      })

      return queues
    }, {})

    return this.queues
  }

  _getEventListener(job) {
    const jobListeners = Object.getOwnPropertyNames(
      Object.getPrototypeOf(job)
    ).reduce((events, method) => {
      if (method.startsWith('on')) {
        const eventName = method
          .replace(/^on(\w)/, (_, group) => group.toLowerCase())
          .replace(/([A-Z]+)/, (_, group) => ` ${group.toLowerCase()}`)

        events.push({ eventName, method })
      }

      return events
    }, [])

    return jobListeners
  }

  getByKey(key) {
    return this.queues[key]
  }

  add(
    key,
    data,
    jobOptions
  ) {
    return this.getByKey(key).bull.add(key, data, jobOptions)
  }

  schedule(
    key,
    data,
    date,
    options
  ) {
    const delay = typeof date === 'number' ? date : date.getTime() - Date.now()

    if (delay <= 0) {
      throw new Error('Invalid schedule time')
    }

    return this.add(key, data, { ...options, delay })
  }

  async remove(key, jobId) {
    const job = await this.getByKey(key).bull.getJob(jobId)
    return job?.remove()
  }

  /* istanbul ignore next */
  ui(port = 9999) {
    const board = BullBoard.createBullBoard(
      Object.keys(this.queues).map(
        (key) => new BullMQAdapter(this.getByKey(key).bull)
      )
    )

    const server = board.router.listen(port, () => {
      this.Logger.info(`bull board on http://localhost:${port}`)
    })

    const shutdown = async () => {
      await server.close(() => {
        this.Logger.info('Stopping bull board server')
      })
    }

    this._shutdowns = [...this._shutdowns, shutdown]
  }

  process() {
    this.Logger.info('Queue processing started')

    const shutdowns = Object.keys(this.queues).map((key) => {
      const jobDefinition = this.getByKey(key)

      if (typeof jobDefinition.boot !== 'undefined') {
        jobDefinition.boot(jobDefinition.bull)
      }

      const workerOptions = {
        concurrency: jobDefinition.concurrency ?? 1,
        connection: this.config.connections[this.config.connection],
        ...jobDefinition.workerOptions,
      }

      const processor = async (job) => {
        try {
          return await jobDefinition.instance.handle(job)
        } catch (error) {
          await this.handleException(error, job)
          return Promise.reject(error)
        }
      }

      const worker = new Worker(key, processor, workerOptions)

      jobDefinition.listeners.forEach(function (item) {
        worker.on(
          item.eventName,
          jobDefinition.instance[item.method].bind(jobDefinition.instance)
        )
      })

      const shutdown = () =>
        Promise.all([jobDefinition.bull.close(), worker.close()])

      return shutdown
    })

    this._shutdowns = [...this._shutdowns, ...shutdowns]

    return this
  }

  async handleException(error, job) {
    try {
      const exceptionHandlerFile = this.resolver
        .forDir('exceptions')
        .getPath('QueueHandler.js')
      fs.accessSync(exceptionHandlerFile, fs.constants.R_OK)

      const namespace = this.resolver
        .forDir('exceptions')
        .translate('QueueHandler')
      const handler = this.app.make(this.app.use(namespace))
      handler.report(error, job)
    } catch (err) {
      this.Logger.error(`name=${job.queue.name} id=${job.id}`)
    }
  }

  async shutdown() {
    await Promise.all(this._shutdowns.map((shutdown) => shutdown()))
  }
}

module.exports = BullManager
