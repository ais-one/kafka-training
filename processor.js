'use strict'
const opts = 
{
    "noptions": {
        "metadata.broker.list": "localhost:9092",
        "group.id": "kafka-streams-test-native",
        "client.id": "kafka-streams-test-name-native",
        "event_cb": true,
        "compression.codec": "snappy",
        "api.version.request": true,
        "socket.keepalive.enable": true,
        "socket.blocking.max.ms": 100,
        "enable.auto.commit": false,
        "auto.commit.interval.ms": 100,
        "heartbeat.interval.ms": 250,
        "retry.backoff.ms": 250,
        "fetch.min.bytes": 100,
        "fetch.message.max.bytes": 2 * 1024 * 1024,
        "queued.min.messages": 100,
        "fetch.error.backoff.ms": 100,
        "queued.max.messages.kbytes": 50,
        "fetch.wait.max.ms": 1000,
        "queue.buffering.max.ms": 1000,
        "batch.num.messages": 10000
    },
    "tconf": {
        "auto.offset.reset": "earliest",
        "request.required.acks": 1
    },
    "batchOptions": {
        "batchSize": 5,
        "commitEveryNBatch": 1,
        "concurrency": 1,
        "commitSync": false,
        "noBatchCommits": false
    }
}

const { KafkaStreams } = require('kafka-streams')
const factory = new KafkaStreams({ noptions: opts.noptions })
factory.on("error", (error) => console.log("Processor error occured:", error.message))

async function runStreamProcessor() {
    try {
        const ktable = factory.getKTable('topic-trades', (kafkaMessage) => {
            // const value = kafkaMessage.value.toString("utf8")
            // const elements = value.toLowerCase().split(" ")
            // return {
            //     key: elements[0],
            //     value: elements[1]
            // }
            return {
                key: kafkaMessage.key.toString("utf8"),
                value: kafkaMessage.value.toString("utf8")
            }
        })
        ktable
            .sumByKey("key", "value")
            .tap(kv => console.log(kv))
            .to("topic-trades-daily-amount")
            // .forEach(row => { console.log(row) })

        await ktable.start()
        console.log('started', ktable)

/*
        const kstream = factory.getKStream()
        kstream
        .from('topic-trades')
        // .mapBufferKeyToString() //key: Buffer -> key: string
        // .mapBufferValueToString() //value: Buffer -> value: string
        // .mapJSONConvenience() //{key: Buffer, value: Buffer} -> {key: string, value: Object}
        // .mapWrapKafkaValue()
        // .mapStringToKV(" ", 0, 1)
        // .sumByKey("key", "value", "sum")
        // .map(kv => kv.key + " " + kv.sum)
        // .tap(kv => console.log(kv))
        // .wrapAsKafkaValue() //value -> {key, value, ..}
        // .to("topic-trades-amount")
        .forEach(msg => console.log(msg))

        await kstream.start()
        console.log('started', kstream)
*/
    } catch (e) {
        console.log(e)
    }
}

runStreamProcessor()
