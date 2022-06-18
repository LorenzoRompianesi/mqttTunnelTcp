# mqttTunnelTcp
Tunnel any TCP connection thru MQTT
Disclaimer
This code isn't production ready, as this isn't the scope of this project and for lack of time. I have tested it with modbus and HTTPS protocols.

Description
This is an early stage python code to test an idea, encapsulate any type of TCP connection inside an MQTT message exchange. 
Why?
There is planty of legacy software that comunicate thru old/insecure/proprietary protocol, is not always faisable to switch to a complete modern infrastructure, e.g. because there is the need compatibility with many actor.
![oldCommunication](doc/mqttTunnelTcp.svg)
Non the less those protocols lack of some usefull features: security, multi-tenant, isolation, etc.
We already have a powerfull tool: ssh tcp tunnels but. IMHO, even if this [ssh tunnel] is perfect from a systemystic point of view, in the IOT era, with thousand of devices and many clients needing data/intercation with them, maybe this does not scale very well.
furthermore the solution of an MQTT TCP Tunnel could be an intermediate step, waiting for everyone to leave the old protocols.
![newCommunication](doc/mqttTunnelTcpAfter.drwaio.svg)