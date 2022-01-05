<?php

namespace Junges\Kafka\Tests\Handlers;

use Closure;
use Junges\Kafka\Contracts\KafkaConsumerMessage;
use Junges\Kafka\Handlers\RetryableHandler;
use Junges\Kafka\Handlers\RetryStrategies\DefaultRetryStrategy;
use Junges\Kafka\Tests\FailingHandler;
use Junges\Kafka\Tests\Fakes\FakeSleeper;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class RetryableHandlerTest extends TestCase
{
    public function testItPassesWhenNoExceptionOccurred(): void
    {
        $failingHandler = new FailingHandler(0, new RuntimeException('test'));
        $handler = new RetryableHandler(Closure::fromCallable($failingHandler), new DefaultRetryStrategy(), new FakeSleeper());

        $messageMock = $this->createMock(KafkaConsumerMessage::class);
        $handler($messageMock);

        $this->assertSame(1, $failingHandler->getTimesInvoked());
    }

    public function testItDoesRetriesOnException(): void
    {
        $failingHandler = new FailingHandler(4, new RuntimeException('test'));
        $sleeper = new FakeSleeper();
        $handler = new RetryableHandler(Closure::fromCallable($failingHandler), new DefaultRetryStrategy(), $sleeper);

        $messageMock = $this->createMock(KafkaConsumerMessage::class);
        $handler($messageMock);

        $this->assertSame(5, $failingHandler->getTimesInvoked());
        $this->assertEquals([1000000.0, 2000000.0, 4000000.0, 8000000.0], $sleeper->getSleeps());
    }

    public function testItBubblesExceptionWhenRetriesExceeded(): void
    {
        $failingHandler = new FailingHandler(100, new RuntimeException('test'));
        $sleeper = new FakeSleeper();
        $handler = new RetryableHandler(Closure::fromCallable($failingHandler), new DefaultRetryStrategy(), $sleeper);

        $messageMock = $this->createMock(KafkaConsumerMessage::class);

        try {
            $handler($messageMock);

            $this->fail('Handler passed but a \RuntimeException is expected.');
        } catch (RuntimeException $exception) {
            $this->assertSame(7, $failingHandler->getTimesInvoked());
            $this->assertEquals([1000000.0, 2000000.0, 4000000.0, 8000000.0, 16000000.0, 32000000.0], $sleeper->getSleeps());
        }
    }
}
