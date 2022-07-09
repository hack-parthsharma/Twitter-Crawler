import pika
import sys
import json
import argparse

from task_creator import create_tasks


class Producer:
    def __init__(self, host, port=5672, login='serv', password='1234'):
        self.login = login
        self.password = password
        self.host = host
        self.port = port

    def run(self, clear_queue):

        credentials = pika.PlainCredentials(self.login, self.password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host, credentials=credentials, heartbeat=60 * 60))
        self.channel = self.connection.channel()
        if clear_queue:
            self.channel.queue_delete(queue='task_queue')

        self.channel.queue_declare(queue='task_queue', durable=True)

    def stop(self):
        self.connection.close()

    def send_tasks(self, tasks):
        for task in tasks:
            self.channel \
                .basic_publish(exchange='',
                               routing_key='task_queue',
                               body=task)
            print(" [x] Sent %r" % (task, ))


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Crawler')
        parser.add_argument(
            '-q',
            '--query',
            help='Search query json file',
            default='query.json',
            dest="query_file",
            type=str)

        parser.add_argument(
            '-s',
            '--save_set',
            help='Save settings json file',
            default='save_settings.json',
            dest="save_set_file",
            type=str)

        parser.add_argument(
            '-cq',
            '--clear_queue',
            help='Clear queue',
            default=True,
            dest="cq",
            type=bool)
        args_c = parser.parse_args()

        query = json.load(open(args_c.query_file))
        saveSet = json.load(open(args_c.save_set_file))
        tasks = create_tasks(query, saveSet)

        p = Producer('localhost')
        p.run(args_c.cq)
        p.send_tasks(tasks)
        p.stop()

    except Exception:
        print(sys.exc_info())
