from confluent_kafka import Producer,KafkaError,Message,Consumer,TopicPartition
from confluent_kafka import KafkaException
from uuid import uuid4
from typing import Any
import socket


consumer_default_conf:dict[str,Any]= {
    'bootstrap.servers': "localhost:9092",
    'group.id': "consumer_group",
    'auto.offset.reset': 'earliest',
}

produce_default_conf:dict[str,Any]= {
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



class KaMessage:
    def __init__(self,topic:str,key:str,value:str):
        self.topic = topic 
        self.key = key
        self.value = value

def delivery_report(err:KafkaError|None, msg:Message):
    """回调函数，报告消息传递成功或失败"""
    if err is not None:
        print(f'消息发送失败: {err}')
    else:
        print(f'消息发送成功到 {msg.topic()} [{msg.partition()}]')

class KaProducer:
    '''
    Kafka生产者
    count 参数用于计数
    '''
    _count = 0
    _control_topic = "control-topic"
    producer_consume_conf:dict[str,Any] = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'producer-control-group',
    'auto.offset.reset': 'earliest'
    }
    _consumer = Consumer(producer_consume_conf)
    _consumer.subscribe(["control-topic"]) # type: ignore

    
    def __init__(self,conf:dict[str,Any]) -> None:
        self.producer = Producer(conf)
        self.control_uuid_key = uuid4().hex
        KaProducer._count += 1 

    async def produce_data(self,message:KaMessage)->None: # type: ignore
        self.producer.produce(topic = message.topic,value = message.value,key=message.key,callback=delivery_report) # type: ignore
        self.producer.flush()

    async def wait_for_trigger(self):
        while True:
            msg = KaProducer._consumer.poll(1)
            if msg is not None and msg.error():
                raise KafkaException(msg.error())
            if msg is None:
                continue
            if msg.value().decode() == "PROCEED_NEXT_BATCH": # type: ignore
                print("生产者: 收到触发信号，准备推送下一批数据")
    
    # def close(self,)->None:
    #     self.producer.



class KaConsumer:
    count = 0

    def __init__(self,subscribe:list[str],conf:dict[str,Any]) -> None:
        self.consumer = Consumer(conf)
        self.consumer.subscribe(topics=subscribe) # type: ignore
        self.count  += 1

    
    
    def listeningAndTrigger(self,num_messages:int=1,timeout:int=-1):
        try:
            while True:
                partitions:list[TopicPartition] = self.consumer.consume(num_messages=num_messages,timeout=timeout)
                body = partitions.pop().value()
                print(body)

                if body == b'EOF' :
                    break
        except Exception as e:

            print(str(e))

    def close(self,):
        self.consumer.close()

if __name__ == '__main__':
    kap = KaProducer(produce_default_conf)
    # for i in range(1,10):
    #     mes = KaMessage(topic='test',key=str(i),partition=int(i),value=f"message body:{i}")
    #     kap.produce(mes)
    # mes = KaMessage(topic='test',key='0',partition=0,value=f"EOF")
    # kap.produce(mes)
    # kap.post()

    kac = KaConsumer(['test'],consumer_default_conf)
    kac.listening()
    kac.close()