'use strict';
const amqplib = require('amqplib');

module.exports = class RabbitMQ {
    constructor(url, exchange) {
        this.queues = {};
        this.channel = (async () => {
            for (;;) {
                try {
                    log.debug('Connecting to RabbitMQ...');
                    const connection = await amqplib.connect(url);
                    const channel = await connection.createChannel();
                    if ((this.exchange = exchange)) await channel.assertExchange(exchange, 'x-delayed-message', {arguments: {'x-delayed-type': 'direct'}});
                    return channel;
                } catch (err) {
                    if (__DEV__ && err.code == 'ECONNREFUSED') {
                        await new Promise((resolve) => setTimeout(resolve, 1000));
                        continue;
                    }
                    throw err;
                }
            }
        })();
    }

    async assertQueue(queue) {
        const channel = await this.channel;
        if (!this.queues[queue]) this.queues[queue] = channel.assertQueue(queue);
        return await this.queues[queue];
    }

    async send(queue, content, {delay} = {}) {
        await this.assertQueue(queue);
        return await (await this.channel).publish(this.exchange || '', queue, Buffer.from(JSON.stringify(content)), delay ? {headers: {'x-delay': delay}} : {});
    }

    async consume(queue, handler, {prefetch} = {}) {
        await this.assertQueue(queue);
        const channel = await this.channel;
        if (prefetch) await channel.prefetch(prefetch);
        if (this.exchange) await channel.bindQueue(queue, this.exchange, queue);
        channel.consume(queue, async (msg) => {
            if (!msg) return;
            try {
                await handler(JSON.parse(msg.content.toString('utf8')));
                await channel.ack(msg);
            } catch (err) {
                log.error({err, msg: msg.content.toString('utf8')});
                await channel.nack(msg);
            }
        });
    }
};
