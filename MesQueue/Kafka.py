from confluent_kafka import Producer
import socket

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname(),
    'security.protocol': 'plaintext',  # 明确指定协议
    # 'api.version.request': True,       # 明确请求API版本
    'socket.timeout.ms': 10000,        # 增加超时
    'message.timeout.ms': 10000,
    # 如果是较旧的Kafka版本(<0.10)可能需要添加
    # 'broker.version.fallback': '0.9.0',
    # 'api.version.fallback.ms': 0
}

producer = Producer(conf)

def delivery_report(err, msg):
    """回调函数，报告消息传递成功或失败"""
    if err is not None:
        print(f'消息发送失败: {err}')
    else:
        print(f'消息发送成功到 {msg.topic()} [{msg.partition()}]')

for i in range(10):
    producer.produce(
        'test_topic', 
        key=str(i), 
        value=f"消息 {i}", 
        callback=delivery_report # type: ignore
    )
    # 立即发送，通常在生产环境中会批量发送
    producer.flush()