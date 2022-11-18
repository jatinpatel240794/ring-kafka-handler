<?php

/**
 * Kafka Wrapper which is used to push tasks into Kafka
 * This file takes care of the conversation between PHP and Kafka
 */

namespace Ring\KafkaWrapper\Services;

use Ring\KafkaWrapper\Handlers\KafkaProducerHandler;

abstract class Kafka
{

    private static function isKafkaEnabled()
    {
        return config('kafkawrapper.is_kafka_enabled');
    }

    /**
     * Push data into kafka
     * @param string $topic Indicates Kafka Topic
     * @param string $key   Indicates message key
     * @param array  $data  Indicates data to be pushed to Kafka
     * @return void
     */
    public static function push(string $topic, array $data, string $key = null, array $headers = array())
    {
        if (self::isKafkaEnabled()) {
            $obj = app(KafkaProducerHandler::class);
            $obj->setTopic($topic);
            $obj->setMessage($data);

            if ($headers)
                $obj->setHeaders($headers);

            if ($key)
                $obj->setKey($key);

            $obj->send();
        }
    }
}
