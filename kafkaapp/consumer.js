const { Kafka} = require("kafkajs");

async function run() {
    try {
        const kafka = new Kafka({
            "clientId": "myapp",
            "brokers": ["localhost:9092"]
        })

        const consumer = kafka.consumer({"groupId": "test"});
        console.log("Connecting ...");
        await consumer.connect();
        console.log("Connected");

        await consumer.subscribe({
           "topic": "Users",
           "fromBeginning": true
        })

        await consumer.run({
            "eachMessage": async res => {
                console.log(`RCV Msg ${res.message.value} on partition ${res.partition}`);
            }
        })

    } catch (error) {
        console.error("Error ", error);
    } finally {

    }
}

run();