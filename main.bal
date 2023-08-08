import ballerina/io;
import ballerinax/kafka;

configurable string groupId = "order-consumers";
configurable string orders = "orders";
configurable string paymentSuccessOrders = "payment-success-orders";
configurable decimal pollingInterval = 1;
configurable string kafkaEndpoint = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092";
configurable string kafkaUsername = "5LPT2NCBNGUOHUP7";
configurable string kafkaPass = "xOn3o/Svk9sH+qjPp4Qvm5giDvWTgBngEeu4XSBF08bOmQYNfwfTE+VS8pZpdGnB";


public type Order readonly & record {|
    int id;
    string desc;
    PaymentStatus paymentStatus;
|};

public enum PaymentStatus {
    SUCCESS,
    FAIL
}

final kafka:ConsumerConfiguration consumerConfigs = {
    groupId: groupId,
    topics: [orders],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    auth: { username: kafkaUsername, password: kafkaPass },
    sessionTimeout: 45,
    pollingInterval: pollingInterval,
    securityProtocol: kafka:PROTOCOL_SASL_SSL
};

final kafka:ProducerConfiguration producerConfigs = {
    auth: { username: kafkaUsername, password: kafkaPass },
    securityProtocol: kafka:PROTOCOL_SASL_SSL
};

service on new kafka:Listener(kafkaEndpoint, consumerConfigs) {
    private final kafka:Producer orderProducer;

    function init() returns error? {
        io:println("Order consumer service configs: " , consumerConfigs.auth , kafkaEndpoint);
        self.orderProducer = check new (kafkaEndpoint, producerConfigs);
    }

    remote function onConsumerRecord(Order[] orders) returns error? {
        from Order 'order in orders
            where 'order.paymentStatus == SUCCESS
            do {
                io:println("Order received: " + 'order.desc);
                check self.orderProducer->send({
                    topic: paymentSuccessOrders,
                    value: 'order
                });
            };
    }
}
