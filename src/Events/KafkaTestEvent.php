<?php
namespace KisshtEventProducer\Kafka\Events;

use App\Helpers\AppLogHelper as AppLog;
use KisshtEventProducer\Kafka\Services\Kafka;

abstract class KafkaTestEvent {

    /**
     * publish KafkaTestEvent
     *
     * @param string $identifier
     * @return void
     */
    public static function publish(string $identifier = "Kafka-Test"): void{
        $topic = "ring-test-topic";

        $event_body = [
            "namespace" => env('APP_ENV') . "." . env('APP_NAME'),
            "name"      => "Kafka-Test",
            "data"      => [
                "app" => "Ring",
                "version" => "1.1.4",
                "platform" => "android",
                "last_updated_on" => date("Y-m-d"),
                "min_android_version" => "5.0"
            ],
        ];

        AppLog::debug('KafkaKafka-Test', "publish", 'event_body', [$event_body]);

        Kafka::push($topic, $event_body, $identifier, Kafka::getHeader('Kafka-Test'));
    }
}