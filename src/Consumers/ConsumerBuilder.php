<?php

namespace Junges\Kafka\Consumers;

use Closure;
use InvalidArgumentException;
use Junges\Kafka\Commit\Contracts\CommitterFactory;
use Junges\Kafka\Config\Config;
use Junges\Kafka\Config\Sasl;
use Junges\Kafka\Contracts\MessageDeserializer;

class ConsumerBuilder
{
    /**
     * @var mixed[]
     */
    private $topics;
    /**
     * @var int
     */
    private $commit;
    /**
     * @var string|null
     */
    private $groupId;
    /**
     * @var \Closure
     */
    private $handler;
    /**
     * @var int
     */
    private $maxMessages;
    /**
     * @var int
     */
    private $maxCommitRetries;
    /**
     * @var string
     */
    private $brokers;
    /**
     * @var mixed[]
     */
    private $middlewares;
    /**
     * @var \Junges\Kafka\Config\Sasl|null
     */
    private $saslConfig;
    /**
     * @var string|null
     */
    private $dlq;
    /**
     * @var string
     */
    private $securityProtocol;
    /**
     * @var bool
     */
    private $autoCommit;
    /**
     * @var mixed[]
     */
    private $options;
    /**
     * @var \Junges\Kafka\Contracts\MessageDeserializer
     */
    private $deserializer;
    /**
     * @var \Junges\Kafka\Commit\Contracts\CommitterFactory|null
     */
    private $committerFactory;

    /**
     * @param string $brokers
     * @param array $topics
     * @param string|null $groupId
     */
    private function __construct(string $brokers, array $topics = [], string $groupId = null)
    {
        if (count($topics) > 0) {
            foreach ($topics as $topic) {
                $this->validateTopic($topic);
            }
        }

        $this->brokers = $brokers;
        $this->groupId = $groupId;
        $this->topics = array_unique($topics);

        $this->commit = 1;
        $this->handler = function () {
        };

        $this->maxMessages = -1;
        $this->maxCommitRetries = 6;
        $this->middlewares = [];
        $this->securityProtocol = 'PLAINTEXT';
        $this->autoCommit = false;
        $this->options = [];

        $this->deserializer = resolve(MessageDeserializer::class);
    }

    /**
     * Creates a new ConsumerBuilder instance.
     *
     * @param string $brokers
     * @param array $topics
     * @param string|null $groupId
     * @return static
     */
    public static function create(string $brokers, array $topics = [], string $groupId = null): self
    {
        return new ConsumerBuilder(
            $brokers,
            $topics,
            $groupId
        );
    }

    /**
     * Subscribe to a Kafka topic.
     *
     * @param mixed ...$topics
     * @return $this
     */
    public function subscribe(...$topics): self
    {
        if (is_array($topics[0])) {
            $topics = $topics[0];
        }

        foreach ($topics as $topic) {
            $this->validateTopic($topic);

            if (! collect($this->topics)->contains($topic)) {
                $this->topics[] = $topic;
            }
        }

        return $this;
    }

    /**
     * Set the brokers the kafka consumer should use.
     *
     * @param ?string $brokers
     * @return $this
     */
    public function withBrokers(?string $brokers): self
    {
        $this->brokers = $brokers ?? config('kafka.brokers');

        return $this;
    }

    /**
     * Specify the consumer group id.
     *
     * @param ?string $groupId
     * @return $this
     */
    public function withConsumerGroupId(?string $groupId): self
    {
        $this->groupId = $groupId;

        return $this;
    }

    /**
     * Specify the commit batch size.
     *
     * @param int $size
     * @return $this
     */
    public function withCommitBatchSize(int $size): self
    {
        $this->commit = $size;

        return $this;
    }

    /**
     * Specify the class used to handle consumed messages.
     *
     * @param callable $handler
     * @return $this
     */
    public function withHandler(callable $handler): self
    {
        $this->handler = Closure::fromCallable($handler);

        return $this;
    }

    /**
     * Specify the class that should be used to deserialize messages.
     *
     * @param MessageDeserializer $deserializer
     * @return $this
     */
    public function usingDeserializer(MessageDeserializer $deserializer): self
    {
        $this->deserializer = $deserializer;

        return $this;
    }

    /**
     * Specify the factory that should be used to build the committer.
     *
     * @param CommitterFactory $committerFactory
     * @return $this
     */
    public function usingCommitterFactory(CommitterFactory $committerFactory): self
    {
        $this->committerFactory = $committerFactory;

        return $this;
    }

    /**
     * Define the max number of messages that should be consumed.
     *
     * @param int $maxMessages
     * @return $this
     */
    public function withMaxMessages(int $maxMessages): self
    {
        $this->maxMessages = $maxMessages;

        return $this;
    }

    /**
     * Specify the max retries attempts.
     *
     * @param int $maxCommitRetries
     * @return $this
     */
    public function withMaxCommitRetries(int $maxCommitRetries): self
    {
        $this->maxCommitRetries = $maxCommitRetries;

        return $this;
    }

    /**
     * Set the Dead Letter Queue to be used. If null, the dlq is created from the topic name.
     *
     * @param string|null $dlqTopic
     * @return $this
     */
    public function withDlq(?string $dlqTopic = null): self
    {
        if (null === $dlqTopic) {
            $dlqTopic = $this->topics[0] . '-dlq';
        }

        $this->dlq = $dlqTopic;

        return $this;
    }

    /**
     * Set the Sasl configuration.
     *
     * @param Sasl $saslConfig
     * @return $this
     */
    public function withSasl(Sasl $saslConfig): self
    {
        $this->saslConfig = $saslConfig;

        return $this;
    }

    /**
     * Specify middlewares to be executed before handling the message.
     * The middlewares get executed in the order they are defined.
     * The middleware is a callable in which the first argument is the message itself and the second is the next handler
     *
     * @param callable(mixed, callable): void $middleware
     * @return $this
     */
    public function withMiddleware(callable $middleware): self
    {
        $this->middlewares[] = $middleware;

        return $this;
    }

    /**
     * Specify the security protocol that should be used.
     *
     * @param string $securityProtocol
     * @return $this
     */
    public function withSecurityProtocol(string $securityProtocol): self
    {
        $this->securityProtocol = $securityProtocol;

        return $this;
    }

    /**
     * Enable or disable consumer auto commit option.
     *
     * @return $this
     */
    public function withAutoCommit(bool $autoCommit = true): self
    {
        $this->autoCommit = $autoCommit;

        return $this;
    }

    /**
     * Set the configuration options.
     *
     * @param array $options
     * @return $this
     */
    public function withOptions(array $options): self
    {
        foreach ($options as $name => $value) {
            $this->withOption($name, $value);
        }

        return $this;
    }

    /**
     * Set a specific configuration option.
     *
     * @param string $name
     * @param string $value
     * @return $this
     */
    public function withOption(string $name, string $value): self
    {
        $this->options[$name] = $value;

        return $this;
    }

    /**
     * Build the Kafka consumer.
     *
     * @return Consumer
     */
    public function build(): Consumer
    {
        $config = new Config(
            $this->brokers,
            $this->topics,
            $this->securityProtocol,
            $this->commit,
            $this->groupId,
            new CallableConsumer($this->handler, $this->middlewares),
            $this->saslConfig,
            $this->dlq,
            $this->maxMessages,
            $this->maxCommitRetries,
            $this->autoCommit,
            $this->options
        );

        return new Consumer($config, $this->deserializer, $this->committerFactory);
    }

    /**
     * Validates each topic before subscribing.
     *
     * @param mixed $topic
     * @return void
     */
    private function validateTopic($topic)
    {
        if (! is_string($topic)) {
            $type = ucfirst(gettype($topic));

            throw new InvalidArgumentException("The topic name should be a string value. [{$type}] given.");
        }
    }
}
