import socket, random

brokerPorts = [5000, 5001, 5002]

c = socket.socket()
isProducer = False
c.connect(('localhost', brokerPorts[random.randint(0, len(brokerPorts)-1)]))

print(c.recv(1024).decode())