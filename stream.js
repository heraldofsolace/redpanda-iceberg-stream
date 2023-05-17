import { Kafka } from 'kafkajs'
import { faker } from "@faker-js/faker"

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:19092'],
})

function generateData() {
    const name = faker.person.fullName()
    const email = faker.internet.email()
    const uid = faker.string.nanoid()
    const hotels_visited = faker.number.int({ min: 0, max: 500 })
    const vip = hotels_visited > 30

    return {
        name,
        email,
        uid,
        hotels_visited,
        vip
    }
}

async function stream() {
    const producer = kafka.producer()

    await producer.connect()

    for(let i = 1; i < 20; i++) {
        let user = generateData()
        const key = {
            "schema": {
                "type": "struct",
                "fields": [
                    {
                        "type": "string",
                        "optional": false,
                        "field": "uid"
                    },
                ],
                "optional": false,
                "name": "pandaplates.Key"
            },
            "payload": {
                "uid": user.uid
            }
        }

        const value = {
            "schema": {
                
                "type": "struct",
                "fields": [
                    {
                        "type": "struct",
                        "fields": [
                            {
                                "type": "string",
                                "optional": false,
                                "field": "email"
                            },
                            {
                                "type": "string",
                                "optional": false,
                                "field": "name"
                            },
                            {
                                "type": "string",
                                "optional": false,
                                "field": "uid"
                            },
                            {
                                "type": "boolean",
                                "optional": false,
                                "field": "vip"
                            },
                            {
                                "type": "int64",
                                "optional": false,
                                "field": "hotels_visited"
                            },
                        ],
                        "optional": true,
                        "name": "pandaplates.Value",
                        "field": "before"
                    },
                    {
                        "type": "struct",
                        "fields": [
                            {
                                "type": "string",
                                "optional": false,
                                "field": "email"
                            },
                            {
                                "type": "string",
                                "optional": false,
                                "field": "name"
                            },
                            {
                                "type": "string",
                                "optional": false,
                                "field": "uid"
                            },
                            {
                                "type": "boolean",
                                "optional": false,
                                "field": "vip"
                            },
                            {
                                "type": "int64",
                                "optional": false,
                                "field": "hotels_visited"
                            },
                        ],
                        "optional": true,
                        "name": "pandaplates.Value",
                        "field": "after"
                    },
                    {
                        "type": "struct",
                        "fields": [
                          {
                            "type": "int64",
                            "optional": false,
                            "field": "ts_ms"
                          },
                          {
                            "type": "string",
                            "optional": false,
                            "field": "db"
                          },
                          {
                            "type": "string",
                            "optional": false,
                            "field": "table"
                          }
                        ],
                        "optional": false,
                        "name": "pandaplates.Source",
                        "field": "source"
                    },
                    {
                        "type": "string",
                        "optional": false,
                        "field": "op"
                    }
                    
                ],
                "optional": false,
                "name": "pandaplates.Envelope",
                "version": 1
            },
            "payload": {
                before: null,
                after: user,
                source: {
                    ts_ms: Date.now(),
                    db: "my-db",
                    table: "my-table"
                },
                op: "c"
            }
        }
        await producer.send({
            topic: 'customer-data',
            messages: [
                { key: JSON.stringify(key), value: JSON.stringify(value) },
            ],
        })
    }


await producer.disconnect()
}

stream()
