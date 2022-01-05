<?php

namespace Junges\Kafka\Support\Testing\Fakes;

use Junges\Kafka\Config\Config;
use Junges\Kafka\Message\Message;
use RdKafka\Conf;

class ProducerFake
{
    /**
     * @var mixed[]
     */
    private $messages = [];
    private $produceCallback = null;
    /**
     * @var \Junges\Kafka\Config\Config
     */
    private $config;
    /**
     * @var string
     */
    private $topic;

    public function __construct(Config $config, string $topic)
    {
        $this->config = $config;
        $this->topic = $topic;
    }

    public function setConf(array $options = []): Conf
    {
        return new Conf();
    }

    public function withProduceCallback(callable $callback): self
    {
        $this->produceCallback = $callback;

        return $this;
    }

    public function produce(Message $message): bool
    {
        if ($this->produceCallback !== null) {
            $callback = $this->produceCallback;
            $callback($message);
        }

        return true;
    }
}
