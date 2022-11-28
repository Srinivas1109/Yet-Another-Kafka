import threading, socket, json
from utils import checkTopics

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
        clientRes = json.loads(client.recv(1024).decode())
        print(clientRes, type(clientRes))
        client.send("Hello from broker1".encode())
        client.close()

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
        clientRes = json.loads(client.recv(1024).decode())
        print(clientRes, type(clientRes))
        client.send("Hello from broker2".encode())
        client.close()

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
        clientRes = json.loads(client.recv(1024).decode())
        print(clientRes, type(clientRes))
        client.send("Hello from broker3".encode())
        client.close()

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

broker1.join()
broker2.join()
broker3.join()

print("Zookepper Stopping....")