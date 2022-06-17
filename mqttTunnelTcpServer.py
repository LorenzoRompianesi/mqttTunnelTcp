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
globalConnServer = None
sem = threading.Semaphore()


def fromServerToDestination():
    global globalConnServer
    print(str(time.time() - start_time) + ' - receiving thread started\n')  
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(60)
        s.bind(('127.0.0.5', 443))
        s.listen()
        while True:
            conn, addr = s.accept()
    #        conn.timeout(60);
            with conn:
                sem.acquire()
                globalConnServer = conn
                sem.release()
                print(f"Connected by {addr}")
                idConn = random.randint(0, sys.maxsize)
                while True:
                    try:
                        print(str(time.time() - start_time) + ' - waiting server date\n')
                        data = conn.recv(4096)
                        if not data:
                            #maybe closet connection
                            print(str(time.time() - start_time) + ' - no data server\n')
                            properties = Properties(PacketTypes.PUBLISH)
                            properties.ResponseTopic = "fromClient"
                            properties.CorrelationData=bytes(idConn.to_bytes(8, 'big'))
                            client.publish(topic="toClient", payload = None, properties=properties)
                            globalConnServer = None
                            break
                    except:
                        break
                    print(str(time.time() - start_time) + ' - sending to mqtt\n')
                    properties = Properties(PacketTypes.PUBLISH)
                    properties.ResponseTopic = "fromClient"
                    properties.CorrelationData=bytes(idConn.to_bytes(8, 'big'))
                    client.publish(topic="toClient", payload = data, properties=properties)

        oldIdConnection = -1
    
    
def fromDestinationToServer():  
    global globalConnServer
    print(str(time.time() - start_time) + ' - sending thread started\n')  
    print(str(time.time() - start_time) + ' - sending socket started\n')
    while True:
        item = q.get()
        print(str(time.time() - start_time) + ' - sending to server\n')
        while (globalConnServer == None):
            time.sleep(0.2)
        if (item.payload == None):
            print(str(time.time() - start_time) + ' - received void payload from mqtt, closing socket\n')
            sem.acquire()
            try:
                globalConnServer.close()
            except:
                pass
            globalConnServer = None
            sem.release()
        else:
            try:
                globalConnServer.sendall(item.payload)
            except:
                print(str(time.time() - start_time) + ' - error on send data\n')
                pass



client = Client(client_id = "client_1", protocol=mqtt.MQTTv5)

def on_connect(client, userdata, flags, rc, properties=None):
    print("Succesfully connected to mqtt broker")

def on_message(client, userdata, message):
    item = mqttItem()
    item.idConnection = message.properties.CorrelationData
    item.payload = message.payload
    q.put(item)
    print(str(time.time() - start_time) + ' - received via mqtt\n')

client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost")
client.subscribe("fromClient")

client.loop_start()
new_thread = Thread(target=fromServerToDestination)
new_thread2 = Thread(target=fromDestinationToServer)
new_thread.start()
new_thread2.start()
new_thread.join()
client.loop_stop()

	
#@LorenzoRompianesi
