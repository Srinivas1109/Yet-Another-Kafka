import socket, random, json

brokerPorts = [5000, 5001, 5002]

c = socket.socket()
isProducer = True
c.connect(('localhost', brokerPorts[random.randint(0, len(brokerPorts)-1)]))
topicName = input("Enter topic name: ")
noOfPartitions = int(input("Enter no. of partitions: "))

producerData = {
    "topicName": topicName,
    "isProducer": isProducer,
    "noOfPartitions": noOfPartitions
}

c.send(json.dumps(producerData).encode())

res = c.recv(1024).decode()

# /home/pes1ug20cs517/BD-project/producerData.txt
# filePath = input("Enter absolute filepath: ")
# file = open(filePath, "r")

# for line in file.readlines():
#     c.send(line.encode())
# c.send(file.readlines()[0].encode())
print(res)
if res not in "Topic already exists...":
    data = input("Enter data: ")

    while data != "stop":
        c.send(data.encode())
        data = input("Enter data: ")


    c.send("agsv11".encode())