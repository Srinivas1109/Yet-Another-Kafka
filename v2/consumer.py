import socket, random, json

brokerPorts = [5000, 5001, 5002]

c = socket.socket()
isProducer = False
c.connect(('localhost', brokerPorts[random.randint(0, len(brokerPorts)-1)]))
topicName = input("Enter topicName: ")
producerData = {
    "topicName": topicName,
    "isProducer": isProducer,
}
c.send(json.dumps(producerData).encode())

while True:
    data = c.recv(1024).decode()
    if data.replace('\n', '') == "agsv11":
        break
    elif data:
        print(data)

    