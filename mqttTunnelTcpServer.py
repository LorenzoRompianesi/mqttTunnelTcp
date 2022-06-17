import socket,os,random,sys,time,queue, getopt
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
brokerAddress='localhost'
brokerPort=1883
inputAddress='localhost'
inputPort='503'
toDeviceTopic='toDevice'
fromDeviceTopic='fromDevice'

def toDeviceHandler():
    global globalConnServer
    print(str(time.time() - start_time) + ' - receiving thread started\n')  
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(60)
        s.bind((inputAddress, inputPort))
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
                            properties.ResponseTopic = fromDeviceTopic
                            properties.CorrelationData=bytes(idConn.to_bytes(8, 'big'))
                            client.publish(topic=toDeviceTopic, payload = None, properties=properties)
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
    
    
def fromDeviceHandler():  
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

try:
    opts, args = getopt.getopt(sys.argv[1:],"hb:p:a:i:t:f:",["broker=","port=","imputAddress=","inputPort=","toDeviceTopic","fromDeviceTopic"])
except getopt.GetoptError:
    print('mattTunnelTCPServer.py -b <ip_broker> -p <port_of_broker> -a <input_ip_address> -i <input_port> -t <toDeviceTopic> -f <fromDeviceTopic>\n')
    sys.exit(2)
for opt, arg in opts:
    if opt == '-h':
        print('mattTunnelTCPServer.py -b <ip_broker> -p <port_of_broker> -a <input_ip_address> -i <input_port> -t <toDeviceTopic> -f <fromDeviceTopic>\n')
        sys.exit()
    elif opt in ("-b", "--broker"):
        brokerAddress = arg
    elif opt in ("-p", "--port"):
        brokerPort = arg
    elif opt in ("-a", "--inputAddress"):
        inputAddress = arg
    elif opt in ("-d", "--inputPort"):
        inputPort = arg
    elif opt in ("-t", "--toDeviceTopic"):
        toDeviceTopic = arg
    elif opt in ("-t", "--fromDeviceTopic"):
        fromDeviceTopic = arg
print('broker address is "', brokerAddress, '"\n')
print('broker port is "', brokerPort, '"\n')
print('input address is "', inputAddress, '"\n')
print('input port is "', inputPort, '"\n')
print('to device topic is "', toDeviceTopic, '"\n')
print('from device topic is "', fromDeviceTopic, '"\n')

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

client.connect(brokerAddress, port=brokerPort)
client.subscribe(fromDeviceTopic)

client.loop_start()
threadToDevice = Thread(target=toDeviceHandler)
threadFromDevice = Thread(target=fromDeviceHandler)
threadToDevice.start()
threadFromDevice.start()
threadToDevice.join()
client.loop_stop()

	
#@LorenzoRompianesi
