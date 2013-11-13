__author__ = 'fun'
__author__ = 'fun'

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
import time

class QueueConsumer(object):
    def __init__(self,accessKey,secretKey,qName):
        self.conn = SQSConnection(accessKey,secretKey)
        self.conn.https_validate_certificates = False
        self.qName = qName
        self.getQueue()
        print ("init queue[%s] producer")%qName

    def getQueue(self):
        self.queue = self.conn.get_queue(self.qName)
        if self.queue == None:
            self.queue = self.conn.create_queue(self.qName)
            print ("create new queue %s")%self.qName

    def GetMessage(self):

        try:
            while True:
                messages = self.queue.get_messages(1)
                if len(messages) == 0:
                    print "no message receive,sleep 1 seconds.."
                    time.sleep(1)
                    continue
                return messages[0]
        except ValueError:
            print str(ValueError)
            return False

    def DeleteMessage(self,message):
        try:
            res = self.queue.delete_message(message)
            return res
        except ValueError:
            print str(ValueError)
            return False


if __name__ == "__main__":

    qName = "pythonQueue"
    accessKey = "andre"
    sercretKey = "sercretKey"
    consumer = QueueConsumer(accessKey,sercretKey,qName)
    testloop = 1000000
    preTime = time.time()
    successNum = 0
    for loop in range(0,testloop):
        message = consumer.GetMessage()
        if message == False:
            continue
        print ("Get message %d: %s" %(loop,message.get_body()))
        res = consumer.DeleteMessage(message)
        if res == False:
            print "Delete message failed ",message.get_body()
            continue
        print "Delete message success ",message.get_body()
        successNum += 1

    consumer.conn.close()
    endTime = time.time()

    print "start at ",time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(preTime))
    print "end at ",time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(endTime))
    print "total cost ",(endTime-preTime)
    print "consume ",successNum," message success , falied ",(testloop-successNum)
