/* istanbul ignore file */
import { SetMetadata } from '@nestjs/common';
import { IKafkaEvent, KafkaEventHandlerFunction } from '../interfaces';

export type KafkaEventHandlerMetadata = {
  topic: string;
  target: any;
  methodName: string | symbol;
  callback: KafkaEventHandlerFunction;
};

export const KAFKA_EVENT_HANDLER = 'KAFKA_EVENT_HANDLER';

export const KafkaEventHandler = (topic: string): MethodDecorator => {
  return <T = (event: IKafkaEvent) => Promise<void>>(
    target,
    propertyKey,
    descriptor,
  ) => {
    SetMetadata<string, KafkaEventHandlerMetadata>(KAFKA_EVENT_HANDLER, {
      topic,
      target,
      methodName: propertyKey,
      callback: descriptor.value,
    })(target, propertyKey, descriptor);
  };
};
