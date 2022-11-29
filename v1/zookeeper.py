import threading, socket, json, time
from utils import checkTopics, createTopic, writeMessages, createPartitions

# on failure of existing leader change leader name
leader = "Broker1"

def Broker(client):
    clientRes = json.loads(client.recv(1024).decode())
        # print(clientRes)
    if clientRes["isProducer"]:
        if clientRes["topicName"] not in checkTopics()[0] and clientRes["topicName"] not in checkTopics()[1] and clientRes["topicName"] not in checkTopics()[2]:
            # create new topic
            createTopic(clientRes["topicName"], leader)
            # print("Topic Created Successfully....")

            for i in range(clientRes["noOfPartitions"]):
                # print(i)
                createPartitions(clientRes["topicName"], str(i))
                # create subfolders as partions

            client.send(f"Topic and Partitions created succcessfully...".encode())

        # append data to existing topic
    clientData = client.recv(1024).decode()
    if clientData.lower() == "stop":
        client.close()

# With thread1
def Broker1():
    broker1Socket = socket.socket()
    print("Broker 1 socket created...")
    broker1Socket.bind(("localhost", 5000))
    broker1Socket.listen()
    print("Broker 1 waiting for connections....")

    while True:
        client, addr = broker1Socket.accept()
        print("Connected With ", addr)
        Broker(client)
        



# With thread2
def Broker2():
    broker1Socket = socket.socket()
    print("Broker 2 socket created...")
    broker1Socket.bind(("localhost", 5001))
    broker1Socket.listen()
    print("Broker 2 waiting for connections....")

    while True:
        client, addr = broker1Socket.accept()
        print("Connected With ", addr)
        Broker(client)

# With thread3
def Broker3():
    broker1Socket = socket.socket()
    print("Broker 3 socket created...")
    broker1Socket.bind(("localhost", 5002))
    broker1Socket.listen()
    print("Broker 3 waiting for connections....")

    while True:
        client, addr = broker1Socket.accept()
        print("Connected With ", addr)
        Broker(client)

print("Creating Brokers....")

broker1 = threading.Thread(target= Broker1)
broker1.start()
print("Broker1 Started")

broker2 = threading.Thread(target= Broker2)
broker2.start()
print("Broker2 Started")

broker3 = threading.Thread(target= Broker3)
broker3.start()
print("Broker3 Started")

while True:
    if not broker1.is_alive():
        print("Broker1 is Dead")

    if not broker2.is_alive():
        print("Broker2 is Dead")

    if not broker3.is_alive():
        print("Broker3 is Dead")
        
    time.sleep(1)


# broker1.join()
# broker2.join()
# broker3.join()

# print("Zookepper Stopping....")