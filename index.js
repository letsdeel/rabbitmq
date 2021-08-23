'use strict';
const EventEmitter = require('events');
const amqplib = require('amqplib');

module.exports = class RabbitMQ extends EventEmitter {
    constructor(url, exchange = '') {
        super();
        Object.assign(this, {url, exchange, queues: {}});
        (function connect(retry) {
            this.connection = (async () => {
                for (;;) {
                    try {
                        log.debug('Connecting to RabbitMQ...');
                        const connection = await amqplib.connect(this.url);
                        this.channel = await connection.createChannel();
                        if (this.exchange) await this.channel.assertExchange(this.exchange, 'x-delayed-message', {arguments: {'x-delayed-type': 'direct'}});
                        connection.on('close', (err) => {
                            setImmediate(() => this.emit('close'));
                            if (!err) return;
                            delete this.channel;
                            this.queues = {};
                            connect.call(this, 3000);
                        });
                        setImmediate(() => this.emit('connect', this));
                        return connection;
                    } catch (err) {
                        if (retry || (global.__DEV__ && err.code == 'ECONNREFUSED')) {
                            await new Promise((resolve) => setTimeout(resolve, retry || 1000));
                            continue;
                        }
                        delete this.exchange;
                        delete this.channel;
                        delete this.url;
                        this.emit('error', err);
                    }
                }
            })();
        }.call(this));
    }

    async assertQueue(queue) {
        await this.connection;
        if (!this.queues[queue]) this.queues[queue] = this.channel.assertQueue(queue);
        return await this.queues[queue];
    }

    async send(queue, content, {delay} = {}) {
        await this.assertQueue(queue);
        return await this.channel.publish(this.exchange, queue, Buffer.from(JSON.stringify(content)), delay ? {headers: {'x-delay': delay}} : {});
    }

    async consume(queue, handler, {prefetch} = {}) {
        await this.assertQueue(queue);
        if (prefetch) await this.channel.prefetch(prefetch);
        if (this.exchange) await this.channel.bindQueue(queue, this.exchange, queue);
        this.channel.consume(queue, async (msg) => {
            if (!msg) return;
            try {
                await handler(JSON.parse(msg.content.toString('utf8')));
                await this.channel.ack(msg);
            } catch (err) {
                log.error({err, msg: msg.content.toString('utf8')});
                await this.channel.nack(msg);
            }
        });
    }

    async close() {
        if (!this.channel) return;
        const connection = await this.connection;
        await connection.close();
        delete this.channel;
        delete this.exchange;
        delete this.url;
        this.connection = Promise.reject(new Error('RabbitMQ connection is closed'));
        this.queues = {};
    }
};
