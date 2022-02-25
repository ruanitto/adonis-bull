const { ServiceProvider, resolver } = require('@adonisjs/fold')
const path = require('path')

class Bullv5Provider extends ServiceProvider {
  register() {
    this.app.singleton('Rocketseat/Bullv5', (app) => {
      const BullManager = require('../src/BullManager')
      const Helpers = app.use('Adonis/Src/Helpers')
      const Logger = app.use('Adonis/Src/Logger')
      const Config = app.use('Adonis/Src/Config')

      const jobs = require(path.join(Helpers.appRoot(), 'start/jobs-v5.js')) || []

      return new BullManager(Logger, Config, jobs, app, resolver)
    })

    this.app.alias('Rocketseat/Bullv5', 'Bullv5')
  }
}

module.exports = Bullv5Provider
