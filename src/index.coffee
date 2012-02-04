leveldb = exports = module.exports = require '../build/leveldb.node'
leveldb.version = '0.5.1'

leveldb.createClient = ->
  client = new leveldb.DB
  client.open.apply client, arguments
  client
