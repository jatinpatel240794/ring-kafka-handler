<?php

/**
 * Kafka Wrapper which is used to push tasks into Kafka
 * This file takes care of the conversation between PHP and Kafka
 */

namespace Kisshteventproducer\Kafka\Services;

use Kisshteventproducer\Kafka\Handlers\KafkaProducerHandler;

abstract class Kafka {

    private static function isKafkaEnabled() {
        return env("IS_KAFKA_ENABLED", 0);
    }

    /**
     * Push data into kafka
     * @param  string $topic   Indicates Kafka Topic
     * @param  string $key     Indicates Kafka key
     * @param  array  $data    Indicates Kafka data
     * @param  array  $headers Indicates Kafka headers
     * @return void
     */
    public static function push(string $topic, array $data, string $key = null, array $headers = []) {
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

    public static function getHeader(string $key, int $schemaVersion = 1) :array {
        return [
            "Content-Type"   => "application/json",
            "Schema-URL"     => "user-attributes-$key",
            "Schema-Subject" => "user-attributes-$key",
            "Schema-Version" => $schemaVersion,
        ];
    }
}