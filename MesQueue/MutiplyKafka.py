from confluent_kafka import Consumer,Producer,TopicPartition,Message
from confluent_kafka import KafkaError
from typing import Any
from queue import Queue
import threading
import json 



BASE_CONFIG:dict[str,Any] = {'bootstrap.servers': 'localhost:9092'}

DEFAULT_PRODUCER_CONFIG:dict[str,Any] = BASE_CONFIG | {
    'enable.idempotence': True,
    'security.protocol': 'plaintext',  # 明确指定协议
    'socket.timeout.ms': 10000,        # 增加超时
    'message.timeout.ms': 10000,
}
DEFAULT_CONSUMER_CONFIG:dict[str,Any] =  BASE_CONFIG |{
    'group.id': 'producer-control-group',
    'auto.offset.reset': 'earliest'
}

CONTROL_TOPIC = "control_topic"

class DataStruct:

    def __init__(self,id:str,body:list[str]) -> None:
        self._id = id
        self._body = json.dumps(body)

    @property
    def body(self,)->str:
        return self._body
    
    @property
    def id(self,)->str:
        return self._id

DATA_TOPIC = "data_topic" 
class AsyncProducer:
 

    def __init__(self,producer_config:dict[str,Any]) -> None:
        "初始化异步监控生产者"
        self.producer = Producer(config = DEFAULT_PRODUCER_CONFIG|producer_config)
        self.running = True
    
    def _delivery_report(self,err:str,msg:Message)->None:
        "发送完成回调"
        if err:
            print(f"Post fail:{err}")
        else:
            print(f"Message have been post to:\n \
                  topic:{msg.topic} \n \
                  parititon:{msg.partition}")
            
    async def produce_date(self,data:DataStruct):
        "异步推送数据"
        self.producer.produce(  # type: ignore
            topic = DATA_TOPIC,
            key = data.id,
            value= data.body,
            callback=self._delivery_report # type: ignore
        )
        self.producer.flush()

    async def wait_for_trigger(self,consumer_config:dict[str,Any])->None:
        "监听等待消费者触发信号"
        consumer = Consumer(config =DEFAULT_CONSUMER_CONFIG|consumer_config)
        consumer.subscribe(topics=[CONTROL_TOPIC]) # type: ignore

        while self.running:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                if  msg.error().code() != KafkaError._PARTITION_EOF: # type: ignore
                    print(f"Async consumer fail: {msg.error()}")
                continue
            if msg.value().decode() == "PROCEED_NEXT_BATCH": # type: ignore
                print("Producer: recieved!")
                break
        consumer.close()

class AsyncConsumer:
    def __init__(self,consumer_id:str,config:dict[str,Any]={}) -> None:
        self.consumer_id = consumer_id
        self.producer = Producer(config =DEFAULT_PRODUCER_CONFIG|config)
        self.ack_queue:Queue[Any] = Queue()  # 线程安全的ACK任务队列
        self.running = True

         # 启动后台ACK发送线程
        self.ack_thread = threading.Thread(target=self._ack_worker)
        self.ack_thread.daemon = True
        self.ack_thread.start()


    def _ack_worker(self):
        """ 后台线程：从队列取出ACK并异步发送 """
        while self.running:
            partition, offset = self.ack_queue.get()
            if partition is None:  # 终止信号
                break
            self._send_ack(partition, offset)

    def _send_ack(self,partition:TopicPartition,offset:int):
        ack_msg:dict[str,Any] = {
            "partition": partition,
            "offset": offset,
            "consumer_id": self.consumer_id
        }
        
        self.producer.produce( # type: ignore
            topic="ack-topic",
            value=json.dumps(ack_msg),
            callback=lambda err, msg: print(f"ACK发送: {err if err else '成功'}") # type: ignore
        )
        self.producer.poll(0)

    def run(self):
        """ 启动消费者线程 """
        consumer = Consumer(BASE_CONFIG|{
            'group.id': 'data-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })

        consumer.subscribe([DATA_TOPIC]) # type: ignore
        try:
            while self.running:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"消费者错误: {msg.error()}")
                    continue

                # 处理消息（同步）
                data = json.loads(msg.value().decode()) # type: ignore
                print(f"消费者 {self.consumer_id}: 处理数据 {data}")

                # 提交偏移量（同步）
                consumer.commit(message=msg)  # 精确提交当前消息

                # 将ACK任务放入队列（非阻塞）
                self.ack_queue.put((msg.partition(), msg.offset())) # type: ignore

        finally:
            self.running = False
            self.ack_queue.put((None, None))  # 通知ACK线程退出
            consumer.close()
            self.producer.flush()