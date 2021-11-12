
const { Kafka, logLevel } = require('kafkajs')
 const kafka = new Kafka({
  logLevel: logLevel.ERROR, // NOTHING, ERROR, WARN, INFO, and DEBUG
  clientId: 'my-app',
  brokers: ['localhost:9092'] // single broker test
})

const producer = kafka.producer()

const ccxws = require("ccxws");
const binance = new ccxws.Binance()

// market could be from CCXT or genearted by the user
const market = {
  id: "BTCUSDT", // remote_id used by the exchange
  base: "BTC", // standardized base symbol for Bitcoin
  quote: "USDT", // standardized quote symbol for Tether
}

// handle trade events
binance.on("trade", trade => {
  console.log(trade)
  // daily = trade.unix / 86,400,000
  const ts = parseInt(trade.unix / 86400000)
  producer.send({ topic: 'topic-trades', messages: [ { key: String(ts), value: String(trade.amount) } ] }) // unix (ms): 1604766473082, amount: 15140.40
})
// subscribe to trades
async function init() {
  await producer.connect()
  binance.subscribeTrades(market)  
}

init()

// handle level2 orderbook snapshots
// binance.on("l2snapshot", snapshot => console.log(snapshot))
// subscribe to level2 orderbook snapshots
// binance.subscribeLevel2Snapshots(market)

// {
//   exchange: 'Binance',
//   quote: 'USDT',
//   base: 'BTC',
//   tradeId: '414545692',
//   unix: 1604765553058,
//   side: 'sell',
//   price: '15339.70000000',
//   amount: '0.12605400',
//   buyOrderId: undefined,
//   sellOrderId: undefined
// }

// https://gist.github.com/sid24rane/2b10b8f4b2f814bd0851d861d3515a10