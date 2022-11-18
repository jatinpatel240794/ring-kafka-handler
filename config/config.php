<?php

return [
    'is_kafka_enabled' => env('IS_KAFKA_ENABLED', 0),
    'kafka_brokers' => env('KAFKA_BROKERS', ''),
    'kafka_debug' => env('kafka_debug', false)
];
