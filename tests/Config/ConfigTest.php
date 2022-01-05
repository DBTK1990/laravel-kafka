<?php

namespace Junges\Kafka\Tests\Config;

use Junges\Kafka\Config\Config;
use Junges\Kafka\Config\Sasl;
use Junges\Kafka\Contracts\Consumer;
use Junges\Kafka\Tests\LaravelKafkaTestCase;

class ConfigTest extends LaravelKafkaTestCase
{
    public function testItReturnsDefaultKafkaConfiguration()
    {
        $config = new Config('broker', ['topic'], 'security', 1, 'group', $this->createMock(Consumer::class), null, null);

        $expectedOptions = [
            'auto.offset.reset' => 'latest',
            'enable.auto.commit' => 'true',
            'compression.codec' => 'snappy',
            'group.id' => 'group',
            'bootstrap.servers' => 'broker',
            'metadata.broker.list' => 'broker',
        ];

        $this->assertEquals(
            $expectedOptions,
            $config->getConsumerOptions()
        );
    }

    public function testItOverrideDefaultOptionsIfUsingCustom()
    {
        $config = new Config(
            'broker',
            ['topic'],
            'security',
            1,
            'group',
            $this->createMock(Consumer::class),
            null,
            null,
            -1,
            6,
            true,
            ['auto.offset.reset' => 'smallest', 'compression.codec' => 'gzip']
        );

        $expectedOptions = [
            'auto.offset.reset' => 'smallest',
            'enable.auto.commit' => 'true',
            'compression.codec' => 'gzip',
            'group.id' => 'group',
            'bootstrap.servers' => 'broker',
            'metadata.broker.list' => 'broker',
        ];

        $this->assertEquals(
            $expectedOptions,
            $config->getConsumerOptions()
        );
    }

    public function testItUsesSaslConfigWhenSet()
    {
        $config = new Config(
            'broker',
            ['topic'],
            'SASL_SSL',
            1,
            'group',
            $this->createMock(Consumer::class),
            new Sasl('foo', 'bar', 'SCRAM-SHA-512', 'SASL_SSL'),
            null,
            -1,
            6,
            true,
            ['auto.offset.reset' => 'smallest', 'compression.codec' => 'gzip']
        );

        $expectedOptions = [
            'auto.offset.reset' => 'smallest',
            'enable.auto.commit' => 'true',
            'compression.codec' => 'gzip',
            'group.id' => 'group',
            'bootstrap.servers' => 'broker',
            'metadata.broker.list' => 'broker',
            'security.protocol' => 'SASL_SSL',
            'sasl.username' => 'foo',
            'sasl.password' => 'bar',
            'sasl.mechanisms' => 'SCRAM-SHA-512',
        ];

        $this->assertEquals(
            $expectedOptions,
            $config->getConsumerOptions()
        );
    }

    public function testItReturnsProducerOptions()
    {
        $sasl = new Sasl(
            'user',
            'pass',
            'mec'
        );

        $config = new Config('broker', ['topic'], 'SASL_PLAINTEXT', 1, 'group', $this->createMock(Consumer::class), $sasl, null);

        $expectedOptions = [
            'compression.codec' => 'snappy',
            'bootstrap.servers' => 'broker',
            'sasl.username' => 'user',
            'sasl.password' => 'pass',
            'sasl.mechanisms' => 'mec',
            'metadata.broker.list' => 'broker',
            'security.protocol' => 'SASL_PLAINTEXT',
        ];

        $this->assertEquals(
            $expectedOptions,
            $config->getProducerOptions()
        );
    }

    public function testItAcceptsCustomOptionsForProducersConfig()
    {
        $customOptions = [
            'bootstrap.servers' => '[REMOTE_ADDRESS]',
            'metadata.broker.list' => '[REMOTE_ADDRESS]',
            'security.protocol' => 'SASL_SSL',
            'sasl.mechanisms' => 'PLAIN',
            'sasl.username' => '[API_KEY]',
            'sasl.password' => '[API_KEY]',
        ];

        $config = new Config(
            'broker',
            ['topic'],
            'SASL_PLAINTEXT',
            1,
            'group',
            $this->createMock(Consumer::class),
            null,
            null,
            -1,
            6,
            true,
            $customOptions
        );

        $expectedOptions = [
            'compression.codec' => 'snappy',
            'bootstrap.servers' => '[REMOTE_ADDRESS]',
            'metadata.broker.list' => '[REMOTE_ADDRESS]',
            'security.protocol' => 'SASL_SSL',
            'sasl.mechanisms' => 'PLAIN',
            'sasl.username' => '[API_KEY]',
            'sasl.password' => '[API_KEY]',
        ];

        $this->assertEquals(
            $expectedOptions,
            $config->getProducerOptions()
        );
    }
}
