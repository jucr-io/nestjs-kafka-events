import {
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { KafkaEventFunctionsService } from './kafka-event-functions.service';
import { KafkaModuleConfigurationProvider } from './providers';
import { retry } from './helpers/retry.helper';
import { KafkaLogger } from './loggers';
import {
  Consumer,
  Kafka,
  Producer,
  Admin,
  SeekEntry,
  TopicMessages,
} from 'kafkajs';
import { KafkaAvroSerializer } from './serializer';
import { KafkaAvroDeserializer } from './deserializer';
import { IKafkaModuleConfiguration } from './interfaces';
import { EmitKafkaEventPayload } from './interfaces';
import { getSchemaRegistryValueSubjectByTopic } from './helpers/topic-subject.helper';

@Injectable()
export class KafkaService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private kafka: Kafka;
  private producer: Producer;
  private consumer: Consumer;
  private admin: Admin;
  private config: IKafkaModuleConfiguration;
  protected topicOffsets: Map<
    string,
    (SeekEntry & { high: string; low: string })[]
  > = new Map();

  constructor(
    private readonly kafkaModuleConfigurationProvider: KafkaModuleConfigurationProvider,
    private readonly kafkaEventFunctionsService: KafkaEventFunctionsService,
    private readonly kafkaLogger: KafkaLogger,
    private readonly kafkaAvroSerializer: KafkaAvroSerializer,
    private readonly kafkaAvroDeserializer: KafkaAvroDeserializer,
  ) {
    this.config = kafkaModuleConfigurationProvider.get();
    this.kafka = new Kafka({
      ...this.config.client,
      logCreator: this.kafkaLogger.getKafkaJSLoggingAdapter.bind(
        this.kafkaLogger,
      ),
    });
    if (this.config.consumer) {
      this.consumer = this.kafka.consumer(this.config.consumer);
    }
    if (this.config.producer) {
      this.producer = this.kafka.producer(this.config.producer);
    }
    this.admin = this.kafka.admin();
  }

  /**
   * Connect to Kafka brokers and initialize schema registry and serializers
   */
  private async connectToKafka(): Promise<void> {
    if (this.producer) {
      await this.producer.connect();
    }
    if (this.consumer) {
      await this.consumer.connect();
    }
    await this.admin.connect();
  }

  /**
   * Connect to Schema Registry
   */
  private async connectToSchemaRegistry(): Promise<void> {
    const eventEmitterTopics =
      this.kafkaEventFunctionsService.getEventEmitterTopics() ?? [];
    if (!!this.producer !== eventEmitterTopics.length > 0) {
      this.kafkaLogger.error(
        `When having producer config set, event emitters need to be registered too (and vice versa)`,
      );
    }
    if (this.producer) {
      await this.kafkaAvroSerializer.initialize(
        this.config.schemaRegistry,
        eventEmitterTopics,
      );
    }
    if (this.consumer) {
      const sTopics =
        this.kafkaEventFunctionsService.getEventHandlerTopics() ?? [];
      let subjectProbe: string = undefined;
      if (sTopics.length > 0) {
        subjectProbe = getSchemaRegistryValueSubjectByTopic(
          sTopics[Math.floor(Math.random() * sTopics.length)],
        );
      }
      await this.kafkaAvroDeserializer.initialize(
        this.config.schemaRegistry,
        subjectProbe,
      );
    }
  }

  /**
   * Disconnect everything
   */
  private async disconnect(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
    }
    if (this.consumer) {
      await this.consumer.stop();
      await this.consumer.disconnect();
    }
    await this.admin.disconnect();
  }

  /**
   * Bootstrap all needed data before application is started.
   * Has the good side-effect that errors are directly detected so
   * a not running or wrong configured Kafka setup does not lead to data inconsistencies.
   */
  async onApplicationBootstrap(): Promise<void> {
    await this.connectToKafka();
    // Retry only this op manually as rest is retried by KafkaJS
    await retry(async () => await this.connectToSchemaRegistry(), 5, 2);
    await this.fetchTopicOffsets();
    await this.subscribeToTopics();
    await this.bindEventHandlers();
  }

  /**
   * Disconnect after all listeners are closed
   */
  async onApplicationShutdown(): Promise<void> {
    await this.disconnect();
  }

  /**
   * Fetch all topic offsets
   * (for later ...)
   * @private
   */
  private async fetchTopicOffsets(): Promise<void> {
    for await (const topic of this.kafkaEventFunctionsService.getEventHandlerTopics()) {
      try {
        const topicOffsets = await this.admin.fetchTopicOffsets(topic);
        this.topicOffsets.set(topic, topicOffsets);
      } catch (reject) {
        this.kafkaLogger.error(
          `Error while fetching topic offset for ${topic}`,
          reject,
        );
        throw reject;
      }
    }
  }

  /**
   * Subscribe to all topics where an event handler is registered
   * @private
   */
  private async subscribeToTopics(): Promise<void> {
    if (!this.consumer) {
      return;
    }
    for await (const topic of this.kafkaEventFunctionsService.getEventHandlerTopics()) {
      try {
        await this.consumer.subscribe({ topic, fromBeginning: false });
      } catch (reject) {
        this.kafkaLogger.error(
          `Error while subscribing to topic ${topic}`,
          reject,
        );
        throw reject;
      }
    }
  }

  /**
   * Bind all registered event handlers to topics
   * @private
   */
  private bindEventHandlers(): void {
    if (!this.consumer) {
      return;
    }
    this.consumer.run({
      ...(this.config?.consumerRunConfig ?? {}),
      eachMessage: async ({ topic, message }) => {
        try {
          // retry
          await retry(
            async () => {
              const event = await this.kafkaAvroDeserializer.deserialize(
                message,
              );
              await this.kafkaEventFunctionsService.callEventHandler(
                topic,
                event,
              );
            },
            5,
            this.config?.consumer?.retry?.retries ?? 2,
            (reason) => {
              this.kafkaLogger.warn(
                `Retrying to process message on topic ${topic}.`,
                reason,
              );
            },
          );
        } catch (reject) {
          this.kafkaLogger.error(
            `Error while processing message on topic ${topic}.`,
            reject,
          );
        }
      },
    });
  }

  /**
   * Emit one or more events
   * @param payload
   * @param failSilent
   */
  async emit<V, K>(
    payload: EmitKafkaEventPayload<V, K> | EmitKafkaEventPayload<V, K>[],
    failSilent = true,
  ): Promise<void> {
    if (!this.producer) {
      this.kafkaLogger.error(
        `Cannot emit event(s) on topic(s) ${
          Array.isArray(payload)
            ? payload.map((p) => p.topic).join(', ')
            : payload.topic
        }`,
      );
      return;
    }
    try {
      if (Array.isArray(payload)) {
        const topicMessages: TopicMessages[] = [];
        for await (const p of payload) {
          topicMessages.push({
            messages: [await this.kafkaAvroSerializer.serialize(p)],
            topic: p.topic,
          });
        }
        await this.producer.sendBatch({
          topicMessages,
        });
        return;
      }
      await this.producer.send({
        topic: payload.topic,
        messages: [await this.kafkaAvroSerializer.serialize(payload)],
      });
    } catch (reject) {
      this.kafkaLogger.error(
        `Error while emitting event(s): ${JSON.stringify(payload)}`,
        reject,
      );
      if (!failSilent) {
        throw reject;
      }
    }
  }
}
