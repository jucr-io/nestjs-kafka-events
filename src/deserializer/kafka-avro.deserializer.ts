import { Injectable } from '@nestjs/common';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { KafkaLogger } from '../loggers';
import { IKafkaModuleSchemaRegistryConfiguration } from '../interfaces';
import { IKafkaEvent } from '../interfaces';
import { KafkaMessage } from 'kafkajs';

@Injectable()
export class KafkaAvroDeserializer {
  private schemaRegistry: SchemaRegistry;

  constructor(private readonly kafkaLogger: KafkaLogger) {}

  /**
   * Initialize Schema Registry and try to connect by
   * doing a sample request.
   * @param configuration
   * @param randomSubject
   */
  async initialize(
    configuration: IKafkaModuleSchemaRegistryConfiguration,
    randomSubject?: string,
  ): Promise<void> {
    this.schemaRegistry = new SchemaRegistry(
      configuration.api,
      configuration?.options,
    );
    if (!randomSubject) {
      return;
    }
    try {
      await this.schemaRegistry.getLatestSchemaId(randomSubject);
    } catch (reject) {
      this.kafkaLogger.error(
        `Error while testing schema registry connection (tested subject: ${randomSubject})`,
      );
      throw reject;
    }
  }

  /**
   * Deserialize message from Kafka using Schema Registry
   * @param message
   */
  async deserialize(message: KafkaMessage): Promise<IKafkaEvent> {
    try {
      const key = await this.schemaRegistry.decode(message.key);
      const event = message?.value
        ? await this.schemaRegistry.decode(message.value)
        : message?.value;
      return {
        arrival: new Date(Number(message.timestamp)) ?? new Date(),
        event,
        key,
      };
    } catch (reject) {
      this.kafkaLogger.error(`Error while deserializing message`, reject);
      throw reject;
    }
  }
}
