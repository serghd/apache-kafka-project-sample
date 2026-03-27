import { Kafka, EachMessagePayload } from 'kafkajs'

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async (): Promise<void> => {
  // Producing
  await producer.connect()

  await producer.send({
    topic: 'test-topic',
    messages: [
      { value: 'Hello KafkaJS user!' },
    ],
  })

  // Consuming
  await consumer.connect()

  await consumer.subscribe({
    topic: 'test-topic',
    fromBeginning: true,
  })

  await consumer.run({
    eachMessage: async ({
      topic,
      partition,
      message,
    }: EachMessagePayload): Promise<void> => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        value: message.value?.toString(),
      })
    },
  })
}

run().catch((err: unknown) => {
  console.error('Error in Kafka run:', err)
})