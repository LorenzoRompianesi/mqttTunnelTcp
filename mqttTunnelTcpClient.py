import socket,os,random,sys,time,queue
import paho.mqtt.client as mqtt
from paho.mqtt.client import Client
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from threading import Thread
import threading

class mqttItem:
  payload = b''
  idConnection = 0

start_time = time.time()
q = queue.Queue()
globalConnClient = None
globalCorrelation = 0
sem = threading.Semaphore()


def fromServerToDestination():
    global globalConnClient
    global globalCorrelation
    print(str(time.time() - start_time) + ' - receiving thread started\n')  
    while True:
        while (globalConnClient == None):
            time.sleep(0.2)
        data = globalConnClient.recv(4096)
        if not data:
            #s.close()
            #maybe connection close
            print(str(time.time() - start_time) + ' - received no data\n')
            sem.acquire()
            globalConnClient.close()
            globalConnClient = None
            sem.release()
            data = None
        print(data)
        properties = Properties(PacketTypes.PUBLISH)
        properties.CorrelationData=globalCorrelation
        client.publish(topic="fromClient", payload = data, properties=properties)    
    
def fromDestinationToServer():  
    global globalConnClient
    global globalCorrelation
    print(str(time.time() - start_time) + ' - thread send started\n')  
    while True:
        item = q.get()
        print(str(time.time() - start_time) + ' - send to server\n')
        if ( globalConnClient == None ):
            sem.acquire()
            s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('142.251.209.3', 443))
            globalConnClient = s
            sem.release()
            print(str(time.time() - start_time) + ' - sending socket ready\n')
        if (item.payload == None):
            print(str(time.time() - start_time) + ' - received void payload from mqtt, closing socket\n')
            sem.acquire()
            try:
                s.close()
            except:
                pass
            globalConnClient = None
            s = None
            sem.release()
        else:
            try:
                globalCorrelation = item.idConnection
                s.sendall(item.payload)
            except:
                print(str(time.time() - start_time) + ' - error on send data\n')
                pass



client = Client(client_id = "client_2", protocol=mqtt.MQTTv5)

def on_connect(client, userdata, flags, rc, properties=None):
    print("Succecfully connected to mqtt broker")

def on_message(client, userdata, message):
    item = mqttItem()
    item.idConnection = message.properties.CorrelationData
    item.payload = message.payload
    q.put(item)
    print(str(time.time() - start_time) + ' - received  via mqtt\n')

client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost")
client.subscribe("toClient")

client.loop_start()
new_thread = Thread(target=fromServerToDestination)
new_thread2 = Thread(target=fromDestinationToServer)
new_thread.start()
new_thread2.start()
new_thread.join()
client.loop_stop()

	
#@LorenzoRompianesi
