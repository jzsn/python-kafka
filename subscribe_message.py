import threading

import sys
from kafka import KafkaConsumer



threads = []


class MyThread(threading.Thread):
    def __init__(self, thread_name, topic, partition):
        threading.Thread.__init__(self)
        self.thread_name = thread_name
        self.partition = partition
        self.topic = topic

    def run(self):
        print("Starting " + self.name)
        Consumer(self.thread_name, self.topic, self.partition)

    def stop(self):
        sys.exit()


def Consumer(thread_name, topic, partition):
    # kafka 服务地址
    broker_list = '127.0.0.1:6667, 127.0.0.1:6667, 127.0.0.1:6667'
    '''
    fetch_min_bytes（int） - 服务器为获取请求而返回的最小数据量，否则请等待
    fetch_max_wait_ms（int） - 如果没有足够的数据立即满足fetch_min_bytes给出的要求，服务器在回应提取请求之前将阻塞的最大时间量（以毫秒为单位）
    fetch_max_bytes（int） - 服务器应为获取请求返回的最大数据量。这不是绝对最大值，如果获取的第一个非空分区中的第一条消息大于此值，
                则仍将返回消息以确保消费者可以取得进展。注意：使用者并行执行对多个代理的提取，因此内存使用将取决于包含该主题分区的代理的数量。
                支持的Kafka版本> = 0.10.1.0。默认值：52428800（50 MB）。
    enable_auto_commit（bool） - 如果为True，则消费者的偏移量将在后台定期提交。默认值：True。
    max_poll_records（int） - 单次调用中返回的最大记录数poll()。默认值：500
    max_poll_interval_ms（int） - poll()使用使用者组管理时的调用之间的最大延迟 。这为消费者在获取更多记录之前可以闲置的时间量设置了上限。
                  如果 poll()在此超时到期之前未调用，则认为使用者失败，并且该组将重新平衡以便将分区重新分配给另一个成员。默认300000
    '''
    # 构建kafka消费者
    consumer = KafkaConsumer(bootstrap_servers=broker_list,
                             topic=topic,
                             group_id="",
                             client_id=thread_name,
                             enable_auto_commit=False,
                             # fetch_min_bytes=1024 * 1024,
                             # fetch_max_bytes=1024 * 1024 * 1024 * 10,
                             fetch_max_wait_ms=60000,
                             request_timeout_ms=305000, # 请求超时时间 默认值305秒
                             )
    print("program first run \t Thread:", thread_name, "分区:", partition, "\t开始消费...")
    num = 0  # 记录该消费者消费次数
    # end_offset = consumer.end_offsets([tp])[tp]
    # print(end_offset)
    while True:
        msg = consumer.poll(timeout_ms=60000)
        end_offset = consumer.end_offsets(partition)
        if len(msg) > 0:
            print("线程:", thread_name, "分区:", partition, "最大偏移量:", end_offset, "有无数据,", len(msg))
            lines = 0
            for data in msg.values():
                for line in data:
                    lines += 1
                    line = eval(line.value.decode('utf-8'))
                    '''
                    do something
                    '''
            # 线程此批次消息条数
            print(thread_name, "lines", lines)
        else:
            print(thread_name, '没有数据')
        num += 1
        print(thread_name, "第", num, "次")


if __name__ == '__main__':
    try:
        t1 = MyThread("Thread-0", "test", 0)
        threads.append(t1)
        t2 = MyThread("Thread-1", "test", 1)
        threads.append(t2)
        t3 = MyThread("Thread-2", "test", 2)
        threads.append(t3)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        print("exit program with 0")
    except:
        print("Error: failed to run consumer program")
