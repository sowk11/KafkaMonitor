# -- coding: utf8 --
"""
    @author Evan Le
"""
# kafka监控lag
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition
from email.mime.text import MIMEText
from email.header import Header
import logging
import requests
import socket
import datetime
import sys
import os
import time
from apscheduler.schedulers.blocking import BlockingScheduler
# https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
bootstrap_server = ['192.168.0.1:9092']  # 不需要完整列表仅一个节点即可

note_api = 'http://xxxxx'  # 短信接口
email_api = 'http://xxxxx'  # 邮件接口

# kafka Topic-Group--------------------------------
topic_dic = {
    'topic': 'group',
    'topic2': 'group2',
    'topic3': 'group3',
    'topic4': 'group4',
    'topic5': 'group5',
    'topic6': 'group6'
}
# kafka--------------------------------


producer = KafkaProducer(bootstrap_servers=bootstrap_server)

# 获取Lag值


def getLag(consumer, topic):
    partitions = producer.partitions_for(topic)  # {topic.key}
    sum = 0
    for pt in partitions:
        partition = TopicPartition(topic=topic, partition=pt)
        try:
            beginning_offsets = consumer.committed(partition)
            if beginning_offsets:
                end_offsets = consumer.end_offsets([partition])
                sum = sum + end_offsets[partition] - beginning_offsets
        except Exception as e:
            raise e
    return sum


def send_Email(message):
    msg_data = message.encode('utf-8')
    try:
        response = requests.post(url=email_api, data=msg_data)
    except Exception as e:
        print e
    if response.status_code != 200:
        print(连接短信接口失败)
    else:
        print response.text


def isBlock():
    message = []
    note = None
    for topic in topic_dic.keys():
        group_id = topic_dic[topic]
        consumer = KafkaConsumer(
            topic, group_id=group_id, bootstrap_servers=bootstrap_server)
        lag = int(getLag(consumer, topic))
        # 自定义预警规则
        if lag > 10000:
            send_message(note)
    if message:
        print(发现堵塞)
        return message


def monitor():
    # print "kafka监控开始----"
    message = isBlock()
    msg = 'kafka消费情况：'
    if message:
        for i in range(len(message)):
            msg = msg + '\n' + message[i]
        send_Email(msg)


# 自我重启
def restart_program():
    python = sys.executable
    os.execl(python, python, * sys.argv)


def send_message(msg):  # 发送短信
    msg_data = msg.encode('utf-8')
    try:
        response = requests.post(url=note_api, data=msg_data)
    except Exception as e:
        print e
    if response.status_code != 200:
        print(连接短信接口失败)
    else:
        print response.text


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    log = logging.getLogger('apscheduler.executors.default')
    log.setLevel(logging.INFO)  # DEBUG
    fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    h = logging.StreamHandler()
    h.setFormatter(fmt)
    log.addHandler(h)
    now = datetime.datetime.now()
    # restart_time = now + datetime.timedelta(days=1)
    print '开启任务'
    scheduler.add_job(monitor, 'interval', hours=1,
                      next_run_time=datetime.datetime.now())
    # scheduler.add_job(monitor, 'interval', minutes=1)
    # scheduler.add_job(prism_monitor, 'interval', minutes=30)
    scheduler.start()
