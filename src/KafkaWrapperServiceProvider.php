<?php

namespace Ring\KafkaWrapper;

use \Illuminate\Support\ServiceProvider;
use RdKafka\Conf;
use RdKafka\Producer;

class KafkaWrapperServiceProvider extends ServiceProvider
{
    const PRODUCER_ONLY_CONFIG_OPTIONS = [
        'security.protocol'  => 'ssl',
        'compression.type'   => 'snappy',
        'log_level'          => LOG_ERR,
        'debug'              => 'broker,topic,msg',
        'enable.idempotence' => true
    ];

    /**
     * Boot method
     *
     * @return void
     */
    public function boot()
    {
        if ($this->app->runningInConsole()) {

            $this->publishes([
                __DIR__.'/../config/config.php' => config_path('kafkawrapper.php'),
            ], 'config');

        }
        $conf = $this->setConf(self::PRODUCER_ONLY_CONFIG_OPTIONS);

        $this->app->bind(Producer::class, function () use ($conf) {
            return new Producer($conf);
        });
    }

    /**
     * Set the Kafka Configuration.
     *
     * @param  array           $options
     * @return \RdKafka\Conf
     */
    private function setConf($options)
    {
        $conf = new Conf();

        foreach ($options as $key => $value) {
            $conf->set($key, $value);
        }

        # Custom Brokers
        $conf->set('metadata.broker.list', config("kafkawrapper.kafka_brokers"));

        if (config('kafkawrapper.kafka_debug')) {
            $conf->set('log_level', LOG_DEBUG);
            $conf->set('debug', 'all');
        }

        return $conf;
    }

    public function register()
    {
        $this->mergeConfigFrom(__DIR__.'/../config/config.php', 'kafkawrapper');
    }
}
