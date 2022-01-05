<?php

namespace Junges\Kafka\Producers;

use Junges\Kafka\Config\Config;
use Junges\Kafka\Config\Sasl;
use Junges\Kafka\Contracts\CanProduceMessages;
use Junges\Kafka\Contracts\KafkaProducerMessage;
use Junges\Kafka\Contracts\MessageSerializer;
use Junges\Kafka\Message\Message;

class ProducerBuilder implements CanProduceMessages
{
    /**
     * @var mixed[]
     */
    private $options = [];
    /**
     * @var \Junges\Kafka\Contracts\KafkaProducerMessage
     */
    private $message;
    /**
     * @var \Junges\Kafka\Contracts\MessageSerializer
     */
    private $serializer;
    /**
     * @var \Junges\Kafka\Config\Sasl|null
     */
    private $saslConfig;
    /**
     * @var string
     */
    private $broker;
    /**
     * @var string
     */
    private $topic;
    public function __construct(string $topic, ?string $broker = null)
    {
        $this->topic = $topic;
        /** @var KafkaProducerMessage $message */
        $message = app(KafkaProducerMessage::class);
        $this->message = $message->create($topic);
        $this->serializer = app(MessageSerializer::class);
        $this->broker = $broker ?? config('kafka.brokers');
    }

    /**
     * Return a new Junges\Commit\ProducerBuilder instance
     * @param string $topic
     * @param string|null $broker
     * @return static
     */
    public static function create(string $topic, string $broker = null): \Junges\Kafka\Contracts\CanProduceMessages
    {
        return new ProducerBuilder(
            $topic,
            $broker ?? config('kafka.brokers')
        );
    }

    /**
     * @return $this
     */
    public function withConfigOption(string $name, string $option): \Junges\Kafka\Contracts\CanProduceMessages
    {
        $this->options[$name] = $option;

        return $this;
    }

    /**
     * @return $this
     */
    public function withConfigOptions(array $options): \Junges\Kafka\Contracts\CanProduceMessages
    {
        foreach ($options as $name => $value) {
            $this->withConfigOption($name, $value);
        }

        return $this;
    }

    /**
     * Set the message headers.
     * @param array $headers
     * @return $this
     */
    public function withHeaders(array $headers): \Junges\Kafka\Contracts\CanProduceMessages
    {
        $this->message->withHeaders($headers);

        return $this;
    }

    /**
     * Set the message key.
     * @param string $key
     * @return $this
     */
    public function withKafkaKey(string $key): \Junges\Kafka\Contracts\CanProduceMessages
    {
        $this->message->withKey($key);

        return $this;
    }

    /**
     * Set a message array key.
     * @param string $key
     * @param mixed $message
     * @return ProducerBuilder
     */
    public function withBodyKey(string $key, $message): \Junges\Kafka\Contracts\CanProduceMessages
    {
        $this->message->withBodyKey($key, $message);

        return $this;
    }

    /**
     * @return $this
     * @param \Junges\Kafka\Contracts\KafkaProducerMessage $message
     */
    public function withMessage($message): \Junges\Kafka\Contracts\CanProduceMessages
    {
        $this->message = $message;

        return $this;
    }

    /**
     * @return $this
     */
    public function withDebugEnabled(bool $enabled = true): \Junges\Kafka\Contracts\CanProduceMessages
    {
        if ($enabled) {
            $this->withConfigOptions([
                'log_level' => LOG_DEBUG,
                'debug' => 'all',
            ]);
        } else {
            unset($this->options['log_level']);
            unset($this->options['debug']);
        }

        return $this;
    }

    /**
     * @param Sasl $saslConfig
     * @return $this
     */
    public function withSasl(Sasl $saslConfig): \Junges\Kafka\Contracts\CanProduceMessages
    {
        $this->saslConfig = $saslConfig;

        return $this;
    }

    public function usingSerializer(MessageSerializer $serializer): CanProduceMessages
    {
        $this->serializer = $serializer;

        return $this;
    }

    public function withDebugDisabled(): self
    {
        return $this->withDebugEnabled(false);
    }

    public function getTopic(): string
    {
        return $this->topic;
    }

    public function send(): bool
    {
        $producer = $this->build();

        return $producer->produce($this->message);
    }

    private function build(): Producer
    {
        $conf = new Config($this->broker, [$this->getTopic()], null, null, null, null, $this->saslConfig, null, -1, 6, true, $this->options);

        return app(Producer::class, [
            'config' => $conf,
            'topic' => $this->topic,
            'serializer' => $this->serializer,
        ]);
    }
}
