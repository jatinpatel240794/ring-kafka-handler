<?php

/**
 * This class has been derived from the below GitHub repository
 * @link https://github.com/anam-hossain/laravel-kafka-pub-example
 */

namespace Kisshteventproducer\Kafka\Handlers;

use Exception;
use RdKafka\Producer;
use RdKafka\ProducerTopic;

class KafkaProducerHandler {
    /**
     * Topic missing error message
     */
    const TOPIC_MISSING_ERROR_MESSAGE = 'Topic is not set';

    /**
     * Flush error message
     */
    const FLUSH_ERROR_MESSAGE = 'librdkafka unable to perform flush, messages might be lost';

    /**
     * Message payload
     *
     * @var string
     */
    protected $payload;

    /**
     * Kafka topic
     *
     * @var string
     */
    protected $topic;

    /**
     * RdKafka producer
     *
     * @var \RdKafka\Producer
     */
    protected $producer;

    /**
     * Kafka message
     *
     * @var array
     */
    private $message;

    /**
     * Kafka key
     *
     * @var string
     */
    private $key;

    /**
     * Kafka headers
     *
     * @var array
     */
    private $headers;

    /**
     * KafkaProducer's constructor
     *
     * @param \RdKafka\Producer $producer
     */
    public function __construct(Producer $producer) {
        $this->producer = $producer;
    }

    /**
     * Set kafka topic
     *
     * @param  string  $topic
     * @return $this
     */
    public function setTopic(string $topic) {
        $this->topic = $topic;

        return $this;
    }

    /**
     * Get topic
     *
     * @return string
     */
    public function getTopic() {
        if (!$this->topic) {
            throw new Exception(self::TOPIC_MISSING_ERROR_MESSAGE);
        }

        return $this->topic;
    }

    /**
     * Produce and send a single message to broker
     *
     * @param  string $message
     * @param  mixed  $key
     * @return void
     */
    public function send() {
        $topic = $this->producer->newTopic($this->getTopic());

        $this->produceMessage($topic);

        // pull for any events
        $this->producer->poll(0);

        $this->flush();
    }

    /**
     * librdkafka flush waits for all outstanding producer requests to be handled.
     * It ensures messages produced properly.
     *
     * @param  int    $timeout "timeout in milliseconds"
     * @return void
     */
    protected function flush(int $timeout = 10000) {
        $result = $this->producer->flush($timeout);

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new Exception(self::FLUSH_ERROR_MESSAGE);
        }
    }

    /**
     * Set Message Payload
     *
     * @param  array  $data Message Data
     * @return void
     */
    public function setMessage(array $data) {
        $this->message = json_encode($data);
    }

    /**
     * Set Kafka Key
     *
     * @param  string $key Key
     * @return void
     */
    public function setKey(string $key) {
        $this->key = $key;
    }

    /**
     * Set Kafka Headers
     *
     * @param  string $key Key
     * @return void
     */
    public function setHeaders(array $headers) : void {
        foreach ($headers as $key => $value) {
            $this->setHeader($key, $value);
        }
    }

    public function setHeader(string $key, string $value) : void {
        $this->headers[$key] = $value;
    }

    /**
     * Produce Kafka message
     * 
     * @param  ProducerTopic $topic Reference to Kafka Topic object
     * @return void
     */
    protected function produceMessage(ProducerTopic $topic): void {
        // RD_KAFKA_PARTITION_UA, lets librdkafka choose the partition.
        // Messages with the same "$key" will be in the same topic partition.
        // This ensure that messages are consumed in order.
        if (method_exists($topic, 'producev')) {
            $topic->producev(
                RD_KAFKA_PARTITION_UA,
                0,
                $this->message,
                $this->key,
                $this->headers
            );
        } else {
            $topic->produce(
                RD_KAFKA_PARTITION_UA,
                0,
                $this->message,
                $this->key
            );
        }
    }
}
