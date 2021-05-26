# NestJS Kafka Events 🏄‍♀️
Lightweight, tested, straight-forward wrapper around KafkaJS and Confluent's Schema Registry
to integrate with NestJS.

As the Kafka transporter provided by `@nestjs/microservices` is more focused on the request-response pattern, 
it was more convenient to build a custom module instead of hacking everything into the provided transporter.

As we use this module at [@jucr-io](https://github.com/jucr-io) to let our microservices communicate with each other
via events, maintenance should not be a problem - feel free to use and request features if you need some!🚀

▶️ This module is based on the concept that schemas are managed in a central schema registry and are not registered
automatically when emitting the first event. This is following the best practices to this topic provided by Confluent.

Just to give you an idea how a workflow could look like:

- Developers registering AVRO schemas at the schema registry (either manually or automatically when someone
  pushes some changes to a specific branch in a repository).
- As soon as a schema is registered the first time in the registry, it's available to use for all applications
  relying on this schema registry.

### Usage
➡️ Install via your favorite package manager e.g. `yarn install @jucr/nestjs-kafka-events`

▶️ Register module at the root of your application:

```typescript
// (...)
import { KafkaModule } from '@jucr/nestjs-kafka-events';

@Module({
  imports: [
    KafkaModule.registerAsync({
      useFactory: async (configService: ConfigService) => {
        return {
          client: {
            brokers: configService.brokers,
            clientId: 'my-service',
          },
          consumer: {
            groupId: 'my-service',
            allowAutoTopicCreation: true,
          },
          producer: {
            allowAutoTopicCreation: true,
          },
          schemaRegistry: {
            api: {
              host: 'http://127.0.0.1:9093',
            },
          },
        };
      },
      inject: [ConfigService],
      imports: [ConfigModule],
    }),
  ],
  controllers: [AppController],
  providers: [AppService, ConfigService],
})
export class AppModule {}
```
Configuration options are the same as mentioned on the documentations of [KafkaJS](https://kafka.js.org/docs/configuration)
and [@kafkajs/confluent-schema-registry](https://kafkajs.github.io/confluent-schema-registry/).


▶️ Register event handlers:

```typescript
// (...)
import { IKafkaEvent, KafkaEventHandler } from '@jucr/nestjs-kafka-events';

interface MyEvent {
  userId: string;
  email: string;
}

interface MyEventKey {
  userId: string;
}

@Controller()
export class AppController {
  
  @KafkaEventHandler('com.example.events.user.created')
  async myHandler(
    payload: IKafkaEvent<MyEvent, MyEventKey>,
  ): Promise<any> {
    console.log('User registered: ', payload.event);
    console.log('For co-partitioning: ', payload.key);
    console.log('Event is arrived at: ', payload.arrival.toDateString());
  }
  
}
```

▶️ Produce events:
```typescript
// (...)
import { KafkaEventEmitter, KafkaService } from '@jucr/nestjs-kafka-events';

interface MyEvent {
  userId: string;
  email: string;
}

interface MyEventKey {
  userId: string;
}

@Injectable()
export class AppService {
  constructor(private readonly kafkaService: KafkaService) {}

  @KafkaEventEmitter('com.example.events.user.created')
  async sendMe(): Promise<void> {
    await this.kafkaService.emit<MyEvent, MyEventKey>({
      topic: 'com.example.events.user.created',
      event: {
        userId: 'my-user-id',
        email: 'mail@example.com',
      },
      key: {
        userId: 'my-user-id',
      },
    });
  }

  @KafkaEventEmitter([
    'com.example.events.user.created',
    'com.example.events.customer.added',
  ])
  async sendMeBatch(): Promise<void> {
    await this.kafkaService.emit([
      {
        topic: 'com.example.events.user.created',
        event: {
          userId: 'my-user-id',
          email: 'mail@example.com',
          createdAt: new Date().valueOf(),
        },
        key: {
          userId: 'my-user-id',
        },
      },
      {
        topic: 'com.example.events.customer.added',
        event: {
          customerId: 'my-customer-id',
          email: 'mail@example.com',
        },
        key: {
          department: 'sales',
        },
      },
    ]);
  }
}
```

▶️ The `IKafkaEvent` interface and `emit()` method are generic can be used with custom types to stay type-safe.

The idea behind using decorators for handling and producing events was to simplify the workflow when new schemas/events
are introduced. By doing it this way, it's easy to fetch all schemas at application start up which are needed for the
deserialization done by the library.
No need to specify all schemas needed somewhere in application configuration 😎.

### Contributing
PR's are welcome💕

