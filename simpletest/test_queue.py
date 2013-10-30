#!/usr/bin/env python
import unittest
import time
from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message

ACCESSKEY = "accessKey"
SECRETKEY = "bbbbb"

class SQueueTestCase(unittest.TestCase):
   def setUp(self):
        status =  True
        try:
            self.conn = SQSConnection(ACCESSKEY,SECRETKEY)
            self.conn.https_validate_certificates = False
        except :
            status = False
        self.assertTrue(status)

   def tearDown(self):
       print "tearDown"
       self.conn.close()


   def testQueue(self):
       status = True
       qname = self.generateQueueName("testQueue")
       try:
           queue = self.conn.create_queue(qname)
           getQueue = self.conn.get_queue(qname)
           self.assertEqual(queue.url,getQueue.url,"Url should be equal")

           self.conn.delete_queue(queue)
           getQueue = self.conn.get_queue(qname)
           self.assertIsNone(getQueue)

       except ValueError:
           print str(ValueError)
           status = False

       self.assertTrue(status)

   def testQueueAttribute(self):
       status = True
       qName = self.generateQueueName("testQueueAttribute")
       queue = self.conn.create_queue(qName)
       try:
          attribute = "DelaySeconds"
          value = "100"
          self.conn.set_queue_attribute(queue,attribute,value)
          getValue = self.conn.get_queue_attributes(queue,attribute)
          self.assertTrue(getValue.has_key(attribute))
          print "I get",getValue
          self.assertEqual(value,getValue[attribute],"attribute mismatch")
       except ValueError:
           print str(ValueError)
           status = False
       finally:
           self.conn.delete_queue(queue)
       self.assertTrue(status)


   def testMessage(self):
       status = True
       qName = self.generateQueueName("testQueueAttribute")
       queue = self.conn.create_queue(qName)
       try:
          m1 = Message()
          m1.set_body("Hello Ketty")
          queue.write(m1)


          getM1 = queue.get_messages(1,10)
          self.assertEqual(len(getM1),1)
          self.assertEqual(getM1[0].get_body(),m1.get_body())

          testNum = 10
          testloop = 10
          for loop in range(0,testloop):
              for id in range(0,testNum):
                 message = Message()
                 body = "%d_%d"%(loop,id)
                 message.set_body(body)
                 queue.write(message)




          for loop in range(0,testloop):
              mess = queue.get_messages(testNum)
              queue.delete_message_batch(mess)
              print mess


       except ValueError:
           print str(ValueError)
           status = False
       finally:
           self.conn.delete_queue(queue)
           self.assertTrue(status)

   def testBatch(self):
       status = True
       qName = self.generateQueueName("testBatch")
       queue = self.conn.create_queue(qName)
       #queue = self.conn.get_queue(qName)
       try:
           testNum = 10
           testloop = 10
           for loop in range(0,testloop):
                messages = []
                for id in range(0,testNum):
                    body = "%d_%d"%(loop,id)
                    messageId = body
                    delaySecond = 0
                    messages.append([messageId,body,delaySecond])
                queue.write_batch(messages)



           # #time.sleep(1)
           for loop in range(0,testloop):
               mess = queue.get_messages(testNum)
               print len(mess)
               for one_mess in mess:
                   print one_mess.get_body()
               queue.delete_message_batch(mess)

       except ValueError:
           print str(ValueError)
           status = False
       finally:
           self.conn.delete_queue(queue)
           self.assertTrue(status)

   def generateQueueName(self,prefix):
       return prefix + time.strftime("%Y%m%d%H%M%S")


if __name__ == "__main__":
    unittest.main() 
