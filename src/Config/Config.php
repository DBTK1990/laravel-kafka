<?php

namespace Junges\Kafka\Config;

use JetBrains\PhpStorm\Pure;
use Junges\Kafka\Contracts\Consumer;

class Config
{
    /**
     * @var string
     */
    private $broker;
    /**
     * @var mixed[]
     */
    private $topics;
    /**
     * @var string|null
     */
    private $securityProtocol;
    /**
     * @var int|null
     */
    private $commit;
    /**
     * @var string|null
     */
    private $groupId;
    /**
     * @var \Junges\Kafka\Contracts\Consumer|null
     */
    private $consumer;
    /**
     * @var \Junges\Kafka\Config\Sasl|null
     */
    private $sasl;
    /**
     * @var string|null
     */
    private $dlq;
    /**
     * @var int
     */
    private $maxMessages = -1;
    /**
     * @var int
     */
    private $maxCommitRetries = 6;
    /**
     * @var bool
     */
    private $autoCommit = true;
    /**
     * @var mixed[]
     */
    private $customOptions = [];
    public function __construct(string $broker, array $topics, ?string $securityProtocol = null, ?int $commit = null, ?string $groupId = null, ?Consumer $consumer = null, ?Sasl $sasl = null, ?string $dlq = null, int $maxMessages = -1, int $maxCommitRetries = 6, bool $autoCommit = true, array $customOptions = [])
    {
        $this->broker = $broker;
        $this->topics = $topics;
        $this->securityProtocol = $securityProtocol;
        $this->commit = $commit;
        $this->groupId = $groupId;
        $this->consumer = $consumer;
        $this->sasl = $sasl;
        $this->dlq = $dlq;
        $this->maxMessages = $maxMessages;
        $this->maxCommitRetries = $maxCommitRetries;
        $this->autoCommit = $autoCommit;
        $this->customOptions = $customOptions;
    }

    public function getCommit(): int
    {
        return $this->commit;
    }

    public function getMaxCommitRetries(): int
    {
        return $this->maxCommitRetries;
    }

    public function getTopics(): array
    {
        return $this->topics;
    }

    public function getConsumer(): Consumer
    {
        return $this->consumer;
    }

    public function getDlq(): ?string
    {
        return $this->dlq;
    }

    public function getMaxMessages(): int
    {
        return $this->maxMessages;
    }

    public function isAutoCommit(): bool
    {
        return $this->autoCommit;
    }

    public function getConsumerOptions(): array
    {
        $options = [
            'metadata.broker.list' => $this->broker,
            'auto.offset.reset' => config('kafka.offset_reset', 'latest'),
            'enable.auto.commit' => config('kafka.auto_commit', true) === true ? 'true' : 'false',
            'compression.codec' => config('kafka.compression', 'snappy'),
            'group.id' => $this->groupId,
            'bootstrap.servers' => $this->broker,
        ];

        if (isset($this->autoCommit)) {
            $options['enable.auto.commit'] = $this->autoCommit === true ? 'true' : 'false';
        }

        return array_merge($options, $this->customOptions, $this->getSaslOptions());
    }

    #[Pure]
    public function getProducerOptions(): array
    {
        $config = [
            'compression.codec' => 'snappy',
            'bootstrap.servers' => $this->broker,
            'metadata.broker.list' => $this->broker,
        ];

        return array_merge($config, $this->customOptions, $this->getSaslOptions());
    }

    #[Pure]
    private function getSaslOptions(): array
    {
        if ($this->usingSasl() && $this->sasl !== null) {
            return [
                'sasl.username' => $this->sasl->getUsername(),
                'sasl.password' => $this->sasl->getPassword(),
                'sasl.mechanisms' => $this->sasl->getMechanisms(),
                'security.protocol' => $this->sasl->getSecurityProtocol(),
            ];
        }

        return [];
    }

    private function usingSasl(): bool
    {
        return $this->securityProtocol === 'SASL_PLAINTEXT' || $this->securityProtocol === 'SASL_SSL';
    }
}
