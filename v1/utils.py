import subprocess, shutil
from os import path

def checkTopics():
    # cmd = ["ls"]
    existingTopics = []
    for directory in ["Broker1", "Broker2", "Broker3"]:
        sp = subprocess.Popen(["ls", directory], shell= False, stdout= subprocess.PIPE, stderr= subprocess.PIPE, universal_newlines= True)
        topics, err = sp.communicate()
        # print(topics.splitlines())
        existingTopics.append(topics.splitlines())
        print("ERROR in CheckTopics",err)
    # print(existingTopics)
    return existingTopics

def createTopic(topicName, leader):
    topicName = topicName.replace(" ", "_")
    p = subprocess.Popen(f"mkdir {leader}/{topicName}", shell= True)
    p.communicate()

def deleteTopic(source):
    shutil.rmtree(source)

# checkTopics()
# createTopic("TestTpoic")
# /home/pes1ug20cs517/BD-project/v1/producerData.txt

def hash():
    pass

def replicatePartitions(broker, topicName):
    BASE_DIR = path.dirname(__file__)
    existingTopics = checkTopics()
    if topicName in existingTopics[broker-1]:
        source = BASE_DIR + f"/Broker{broker}/" + topicName
        deleteTopic(source)
    SOURCE = BASE_DIR + "/Broker1/" + topicName
    DESTINATION = BASE_DIR + f"/Broker{broker}/" + topicName
    shutil.copytree(SOURCE, DESTINATION)

def replicate(topicName):
    # print(BASE_DIR)
    for broker in [2, 3]:
        replicatePartitions(broker, topicName)

def createPartitions(topicName, partitionName):
    print(topicName, partitionName)
    p = subprocess.Popen(f"mkdir Broker1/{topicName}/{partitionName}", shell= True)
    p.communicate()

# replicate("Cricket")