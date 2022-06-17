import socket,os,random,sys,time,queue,getopt
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
brokerAddress='localhost'
brokerPort=1883
deviceAddress='localhost'
devicePort='502'
toDeviceTopic='toDevice'


def toDeviceHandler():
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
    
def fromDeviceHandler():  
    global globalConnClient
    global globalCorrelation
    print(str(time.time() - start_time) + ' - thread send started\n')  
    while True:
        item = q.get()
        print(str(time.time() - start_time) + ' - send to server\n')
        if ( globalConnClient == None ):
            sem.acquire()
            s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((deviceAddress, devicePort))
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

try:
    opts, args = getopt.getopt(sys.argv[1:],"hb:p:d:o:t:",["broker=","port=","deviceAddress=","devicePort=","toDeviceTopic"])
except getopt.GetoptError:
    print('mattTunnelTCPClient.py -b <ip_broker> -p <port_of_broker> -d <device_ip_address> -o <device_port> -t <toDeviceTopic>\n')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print('mattTunnelTCPClient.py -b <ip_broker> -p <port_of_broker> -d <device_ip_address> -o <device_port> -t <toDeviceTopic>\n')
        sys.exit()
    elif opt in ("-b", "--broker"):
        brokerAddress = arg
    elif opt in ("-p", "--port"):
        brokerPort = arg
    elif opt in ("-d", "--deviceAddress"):
        deviceAddress = arg
    elif opt in ("-o", "--devicePort"):
        devicePort = arg
    elif opt in ("-t", "--toDeviceTopic"):
        toDeviceTopic = arg
print('broker address is "', brokerAddress, '"\n')
print('broker port is "', brokerPort, '"\n')
print('device address is "', deviceAddress, '"\n')
print('device port is "', devicePort, '"\n')
print('to device topic is "', toDeviceTopic, '"\n')

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

client.connect(brokerAddress, port=brokerPort)
client.subscribe(toDeviceTopic)

client.loop_start()
threadToDevice = Thread(target=toDeviceHandler)
threadFromDevice = Thread(target=fromDeviceHandler)
threadToDevice.start()
threadFromDevice.start()
threadToDevice.join()
client.loop_stop()

	
#@LorenzoRompianesi
