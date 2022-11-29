import subprocess

def checkTopics():
    # cmd = ["ls"]
    existingTopics = []
    for directory in ["Broker1", "Broker2", "Broker3"]:
        sp = subprocess.Popen(["ls", directory], shell= False, stdout= subprocess.PIPE, stderr= subprocess.PIPE, universal_newlines= True)
        topics, err = sp.communicate()
        # print(topics.splitlines())
        existingTopics.append(topics.splitlines())
        print(err)
    print(existingTopics)
    return existingTopics

def createTopic(topicName, leader):
    topicName = topicName.replace(" ", "_")
    p = subprocess.Popen(f"mkdir {leader}/{topicName}", shell= True)
    p.communicate()

# checkTopics()
# createTopic("TestTpoic")
# /home/pes1ug20cs517/BD-project/producerData.txt

def writeMessages(broker, filepath):
    filedata = open(filepath, "r")
    lines = filedata.readlines()
    print(lines)

def createPartitions(topicName, partitionName):
    print(topicName, partitionName)
    p = subprocess.Popen(f"mkdir Broker1/{topicName}/{partitionName}", shell= True)
    p.communicate()