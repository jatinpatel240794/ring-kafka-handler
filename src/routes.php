<?php 
use \KisshtEventProducer\Kafka\Events\KafkaTestEvent;

Route::get('kafka-test', function (){

    KafkaTestEvent::publish();
    
    return view("kafka::kafkatest",[
        'IS_KAFKA_ENABLED' => env('IS_KAFKA_ENABLED'),
        'KAFKA_BROKERS' => env('KAFKA_BROKERS'),
        'KAFKA_DEBUG' => env('KAFKA_DEBUG'),
    ]);
});