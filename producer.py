import socket, random, json

brokerPorts = [5000, 5001, 5002]

c = socket.socket()
isProducer = True
c.connect(('localhost', brokerPorts[random.randint(0, len(brokerPorts)-1)]))
topicName = input("Enter topic name: ")

producerData = {
    "topicName": topicName,
    "isProducer": isProducer
}

c.send(json.dumps(producerData).encode())

print(c.recv(1024).decode())