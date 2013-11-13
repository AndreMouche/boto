__author__ = 'fun'

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
import time

class QueueProducer(object):
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

  def pushMessage(self,messageBody):
      message = Message()
      message.set_body(messageBody)
      try:
         self.queue.write(message)
         print "finished send message ",messageBody
         return True

      except ValueError:
         print str(ValueError)
         return False


if __name__ == "__main__":

    qName = "pythonQueue"
    accessKey = "andre"
    sercretKey = "sercretKey"
    producer = QueueProducer(accessKey,sercretKey,qName)
    testloop = 1000000
    preTime = time.time()
    successNum = 0
    for loop in range(0,testloop):
         messageBody = "message %s" % loop
         status = producer.pushMessage(messageBody)
         if status:
             successNum += 1

    producer.conn.close()
    endTime = time.time()

    print "start at ",time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(preTime))
    print "end at ",time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(endTime))
    print "total cost ",(endTime-preTime)
    print "consume ",successNum," message success , falied ",(testloop-successNum)
