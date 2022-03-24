package org.apache.rocketmqdemos;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class StartupWithACL {
    public static final String TOPIC_NAME = "topicB";
    public static String NAMESRV_ADDRESSES = "127.0.0.1:9876";

    public static final String PRODUCER_GROUP_NAME = "tiger_producer_group_01";

    public static final String CONSUMER_GROUP_NAME = "tiger_consumer_group_01";
    public static final String ALL_TAGS_IN_TOPIC = "*";

    private static DefaultMQPushConsumer CONSUMER = null;
    private static final String ACCESS_KEY = "RocketMQ";
    private static final String SECRET_KEY = "12345678";
    private static DefaultMQProducer PRODUCER = null;

    // 发送和消费消息的条数限制
    private static final int MESSAGE_COUNT = 10;
    // 计数器 锁
    private static CountDownLatch COUNTER = new CountDownLatch(MESSAGE_COUNT);

    public static void main(String[] args) throws InterruptedException, MQClientException {
        if (args != null && args.length > 0) {
            NAMESRV_ADDRESSES = args[0];
        }
        startOneProducer();

        startOneConsumer();

        // 消费消息的条数与发送条数一致， 才会关闭生产者和消费者实例
        COUNTER.await();

        PRODUCER.shutdown();
        CONSUMER.shutdown();
    }

    // 启动一个生产者实例， 并且生产10条消息
    public static void startOneProducer() throws InterruptedException, MQClientException {
        // 1. 启动一个生产者实例
        PRODUCER = new DefaultMQProducer(PRODUCER_GROUP_NAME, new AclClientRPCHook(new SessionCredentials(ACCESS_KEY, SECRET_KEY)));

        // 2. 设置生产者参数
        PRODUCER.setNamesrvAddr(NAMESRV_ADDRESSES);

        // 3. 启动生产者
        PRODUCER.start();
        System.out.printf("Producer Started.%n");

        // 4. 发送消息
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            try {
                Message msg = new Message(TOPIC_NAME, "TagA",
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
                );
                SendResult res = PRODUCER.send(msg);
                System.out.printf("[正在发送消息] %s = %s\n", res.getMsgId(), new String(msg.getBody()));
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }// end for

        System.out.printf("\n\n\n%s", "");
    }

    public static void startOneConsumer() throws MQClientException {
        // 1. 创建一个消费者实例
        CONSUMER = new DefaultMQPushConsumer(new AclClientRPCHook(new SessionCredentials(ACCESS_KEY, SECRET_KEY)));
        CONSUMER.setConsumerGroup(CONSUMER_GROUP_NAME);
        // 2. 设置消费者参数
        CONSUMER.setNamesrvAddr(NAMESRV_ADDRESSES);
        CONSUMER.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        CONSUMER.subscribe(TOPIC_NAME, ALL_TAGS_IN_TOPIC);

        // 3. 注册消费消息回调方法
        CONSUMER.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    COUNTER.countDown();
                    System.out.printf("[正在消费消息] %s = %s\n", msg.getMsgId(), new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 4. 启动消费者
        CONSUMER.start();
        System.out.printf("Consumer Started.%n");
    }
}
