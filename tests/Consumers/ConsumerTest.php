<?php

namespace Junges\Kafka\Tests\Consumers;

use Junges\Kafka\Commit\Contracts\CommitterFactory;
use Junges\Kafka\Commit\VoidCommitter;
use Junges\Kafka\Config\Config;
use Junges\Kafka\Consumers\Consumer;
use Junges\Kafka\Contracts\KafkaConsumerMessage;
use Junges\Kafka\Exceptions\KafkaConsumerException;
use Junges\Kafka\Facades\Kafka;
use Junges\Kafka\Message\ConsumedMessage;
use Junges\Kafka\Message\Deserializers\JsonDeserializer;
use Junges\Kafka\Tests\Fakes\FakeConsumer;
use Junges\Kafka\Tests\Fakes\FakeHandler;
use Junges\Kafka\Tests\LaravelKafkaTestCase;
use RdKafka\Message;

class ConsumerTest extends LaravelKafkaTestCase
{
    /**
     * @var \Junges\Kafka\Consumers\Consumer|null
     */
    private $stoppableConsumer;
    /**
     * @var bool
     */
    private $stoppableConsumerStopped = false;

    public function testItConsumesAMessageSuccessfullyAndCommit()
    {
        $fakeHandler = new FakeHandler();

        $message = new Message();
        $message->err = 0;
        $message->key = 'key';
        $message->topic_name = 'test-topic';
        $message->payload = '{"body": "message payload"}';

        $this->mockConsumerWithMessage($message);

        $this->mockProducer();

        $config = new Config(
            'broker',
            ['test-topic'],
            'security',
            1,
            'group',
            $fakeHandler,
            null,
            null,
            1,
            1
        );

        $consumer = new Consumer($config, new JsonDeserializer());
        $consumer->consume();

        $this->assertInstanceOf(ConsumedMessage::class, $fakeHandler->lastMessage());
    }

    public function testItCanConsumeMessages()
    {
        $message = new Message();
        $message->err = 0;
        $message->key = 'key';
        $message->topic_name = 'test';
        $message->payload = '{"body": "message payload"}';

        $this->mockConsumerWithMessage($message);

        $this->mockProducer();

        $consumer = Kafka::createConsumer(['test'])
            ->withHandler($fakeConsumer = new FakeConsumer())
            ->withAutoCommit()
            ->withMaxMessages(1)
            ->build();

        $consumer->consume();

        $this->assertInstanceOf(ConsumedMessage::class, $fakeConsumer->getMessage());
    }

    public function testConsumeMessageWithError()
    {
        $this->mockProducer();

        $this->expectException(KafkaConsumerException::class);

        $fakeHandler = new FakeHandler();

        $config = new Config(
            'broker',
            ['test-topic'],
            'security',
            1,
            'group',
            $fakeHandler,
            null,
            null,
            1,
            1
        );

        $message = new Message();
        $message->err = 1;
        $message->topic_name = 'test-topic';

        $this->mockConsumerWithMessageFailingCommit($message);

        $consumer = new Consumer($config, new JsonDeserializer());
        $consumer->consume();
    }

    public function testCanStopConsume()
    {
        $message = new Message();
        $message->err = 0;
        $message->key = 'key';
        $message->topic_name = 'test';
        $message->payload = '{"body": "message payload"}';

        $message2 = new Message();
        $message2->err = 0;
        $message2->key = 'key2';
        $message2->topic_name = 'test2';
        $message2->payload = '{"body": "message payload2"}';

        $this->mockConsumerWithMessage($message, $message2);

        $this->mockProducer();

        $this->stoppableConsumer = Kafka::createConsumer(['test'])
            ->withHandler(function (KafkaConsumerMessage $message) {
                if ($message->getKey() === 'key2' && $this->stoppableConsumer) {
                    $this->stoppableConsumer->stopConsume(function () {
                        $this->stoppableConsumerStopped = true;
                    });
                }
            })
            ->withAutoCommit()
            ->build();

        $this->stoppableConsumer->consume();

        $this->assertSame(2, $this->stoppableConsumer->consumedMessagesCount());
        $this->assertTrue($this->stoppableConsumerStopped);
    }

    public function testItAcceptsCustomCommitter(): void
    {
        $fakeHandler = new FakeHandler();

        $message = new Message();
        $message->err = 0;
        $message->key = 'key';
        $message->topic_name = 'test-topic';
        $message->payload = '{"body": "message payload"}';

        $this->mockConsumerWithMessage($message);

        $this->mockProducer();

        $config = new Config(
            'broker',
            ['test-topic'],
            'security',
            1,
            'group',
            $fakeHandler,
            null,
            null,
            1,
            1
        );

        $mockedCommitterFactory = $this->createMock(CommitterFactory::class);
        $mockedCommitterFactory->expects($this->once())
            ->method('make')
            ->willReturn(new VoidCommitter());

        $consumer = new Consumer($config, new JsonDeserializer(), $mockedCommitterFactory);
        $consumer->consume();

        $this->assertInstanceOf(ConsumedMessage::class, $fakeHandler->lastMessage());

        $committer = $this->getPropertyWithReflection('committer', $consumer);
        $this->assertInstanceOf(VoidCommitter::class, $committer);
    }
}
