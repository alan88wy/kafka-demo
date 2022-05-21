const { Kafka} = require("kafkajs");
const msg = process.argv[2];

async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "myapp",
            "brokers": ["localhost:9092"]
        })

        const producer = kafka.producer();
        console.log("Connecting ...");
        await producer.connect();
        console.log("Connected");

        const partition = (msg[0] < "N" || msg[0] < "n") ? 0 : 1
        const res = await producer.send({
            "topic": "Users",
            "messages": [
                {
                    "value": msg,
                    "partition": partition
                }
            ]
        })

        console.log(`Sent Successfully! ${JSON.stringify(res)}`);
        await producer.disconnect();

    } catch (error) {
        console.error("Error ", error);
    }
}

run();