import threading, socket, json, time
from utils import *

BROKER_PORTS = [5000, 5001, 5002]

def Broker(client, broker, clientPort):
    clientRes = json.loads(client.recv(1024).decode())
    if clientRes["isProducer"]:
        res = createPartitions(clientRes["topicName"], clientRes["noOfPartitions"])
        if res:
            client.send(f"Topic and Partitions created succcessfully...".encode())
            clientData = client.recv(1024).decode()
        
            while clientData and clientData.lower() != "agsv11":
                # print(clientData)
                # Write message to the partition
                writeMessages(clientRes["topicName"], broker, clientData)
                time.sleep(1)
                clientData = client.recv(1024*5).decode()
        else:
            client.send(f"Topic already exists...".encode())

        client.close()
    elif not clientRes["isProducer"]:
        # read topic
        # read from beginning
        readFromBeginning(clientRes["topicName"], clientPort, client)
        # client.close()

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
        Broker(client, "Broker1", addr[1])

        



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

        Broker(client, "Broker2", addr[1])

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
        
        Broker(client, "Broker3", addr[1])

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