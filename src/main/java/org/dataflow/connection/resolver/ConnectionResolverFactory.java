package org.dataflow.connection.resolver;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.dataflow.common.ConnectionType;
import org.dataflow.data.serializer.Serializer;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConnectionResolverFactory {

    public static Resolvable getResolver(ConnectionType connectionType) {
        return switch (connectionType) {
            case PRODUCER -> new ProducerConnectionResolver(new Serializer());
            case CONSUMER -> new ConsumerConnectionResolver();
            case BROKER -> new BrokerConnectionResolver();
            default -> throw new IllegalArgumentException("Invalid connection type");
        };
    }

}
