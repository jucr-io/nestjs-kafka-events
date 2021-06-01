import { Injectable } from '@nestjs/common';
import { MetadataScanner, ModulesContainer } from '@nestjs/core';
import {
  KAFKA_EVENT_EMITTER,
  KAFKA_EVENT_HANDLER,
  KafkaEventEmitterMetadata,
  KafkaEventHandlerMetadata,
} from './decorators';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import {
  Controller,
  Injectable as IInjectable,
} from '@nestjs/common/interfaces';
import { IKafkaEvent } from './interfaces';
import { KafkaLogger } from './loggers';

@Injectable()
export class KafkaEventFunctionsService {
  private readonly eventHandlerMap: Map<
    string,
    { methodName: string | symbol; target: any }
  >;
  private eventEmitterTopics: Set<string>;

  constructor(
    private readonly modulesContainer: ModulesContainer,
    private readonly metadataScanner: MetadataScanner,
    private readonly kafkaLogger: KafkaLogger,
  ) {
    this.eventHandlerMap = new Map();
    this.eventEmitterTopics = new Set();
  }

  /**
   * Utilize NestJS to scan for metadata.
   * Event Handler methods need to resist in a mounted controller or injectable
   * and need to be decorated with the EventHandler(topic)/EventEmitter(topics) decorator
   */
  explore(): void {
    const modules = [...this.modulesContainer.values()];
    const providersMap = modules
      .filter(({ providers }) => providers.size > 0)
      .map(({ providers }) => providers)
      .concat(
        modules
          .filter(({ controllers }) => controllers.size > 0)
          .map(({ controllers }) => controllers),
      );
    const instanceWrappers: InstanceWrapper<IInjectable | Controller>[] = [];
    providersMap.forEach((map) => {
      const mapKeys = [...map.keys()];
      const r = mapKeys.map((key) => {
        return map.get(key);
      });
      instanceWrappers.push(...r.filter((i) => !!i.instance));
    });
    const eventHandlers = instanceWrappers
      .map(({ instance }) => {
        const instancePrototype = Object.getPrototypeOf(instance);
        return this.metadataScanner.scanFromPrototype(
          instance,
          instancePrototype,
          (method) =>
            this.exploreMethodMetadata(
              instance,
              instancePrototype,
              method,
              KAFKA_EVENT_HANDLER,
            ),
        );
      })
      .reduce((prev, curr) => {
        return prev.concat(curr);
      });
    const eventEmitters = instanceWrappers
      .map(({ instance }) => {
        const instancePrototype = Object.getPrototypeOf(instance);
        const r = this.metadataScanner.scanFromPrototype(
          instance,
          instancePrototype,
          (method) =>
            this.exploreMethodMetadata(
              instance,
              instancePrototype,
              method,
              KAFKA_EVENT_EMITTER,
            ),
        );
        return r;
      })
      .reduce((prev, curr) => {
        return prev.concat(curr);
      });
    for (const eventHandler of eventHandlers) {
      this.eventHandlerMap.set(eventHandler.topic, {
        target: eventHandler.target,
        methodName: eventHandler.methodName,
      });
    }
    for (const eventEmitter of eventEmitters) {
      this.eventEmitterTopics = new Set([
        ...Array.from(this.eventEmitterTopics),
        ...(Array.isArray(eventEmitter.topics)
          ? eventEmitter.topics
          : [eventEmitter.topics]),
      ]);
    }
    this.kafkaLogger.debug(
      `Registered ${this.eventHandlerMap.size} event handlers`,
    );
    this.kafkaLogger.debug(
      `Registered ${eventEmitters.length} event emitters with ${this.eventEmitterTopics.size} topics`,
    );
  }

  /**
   * Get all event handler topics
   */
  getEventHandlerTopics(): string[] {
    return Array.from(this.eventHandlerMap.keys());
  }

  /**
   * Get all event emitter topics
   */
  getEventEmitterTopics(): string[] {
    return Array.from(this.eventEmitterTopics);
  }

  /**
   * Call registered event handler for given topic
   * @param topic
   * @param event
   */
  async callEventHandler(topic: string, event: IKafkaEvent): Promise<void> {
    const handler = this.eventHandlerMap.get(topic);
    if (!handler) {
      this.kafkaLogger.warn(`Unable to find handler for topic ${topic}`);
      return;
    }
    return await handler.target[handler.methodName].apply(handler.target, [
      event,
    ]);
  }

  /**
   * Explore methods metadata
   * @param instance
   * @param instancePrototype
   * @param methodKey
   * @param metadataKey
   * @private
   */
  private exploreMethodMetadata(
    instance: any,
    instancePrototype: Controller,
    methodKey: string,
    metadataKey: typeof KAFKA_EVENT_EMITTER,
  ): KafkaEventEmitterMetadata | null;
  private exploreMethodMetadata(
    instance: any,
    instancePrototype: Controller,
    methodKey: string,
    metadataKey: typeof KAFKA_EVENT_HANDLER,
  ): KafkaEventHandlerMetadata | null;
  private exploreMethodMetadata(
    instance: any,
    instancePrototype: Controller,
    methodKey: string,
    metadataKey: typeof KAFKA_EVENT_HANDLER | typeof KAFKA_EVENT_EMITTER,
  ): KafkaEventHandlerMetadata | KafkaEventEmitterMetadata | null {
    const targetCallback = instancePrototype[methodKey];
    const handler = Reflect.getMetadata(metadataKey, targetCallback);
    if (handler == null) {
      return null;
    }
    return { ...handler, target: instance };
  }
}
