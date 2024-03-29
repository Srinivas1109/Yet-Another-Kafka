# from zookeeper import BROKER_PORTS
import json, subprocess, shutil
from os import path
import os
import glob

BROKER_PORTS = [5000, 5001, 5002]

BASE_DIR = path.dirname(__file__)

"""

log = {

    topicName1:{
        broker1: [],
        broker2: [],
        broker3: [],

    },
    topicName2:{
        broker1: [],
        broker2: [],
        broker3: [],

    }
}

"""
def deleteTempFiles(portNo):
    pattern = BASE_DIR + f"/**/{portNo}.temp"
    # pattern = BASE_DIR + f"/**/*.temp"

    # print(pattern)
    files = list(glob.iglob(pattern, recursive= True))
    # print(files)
    for i in files:
        # print(i)
        os.remove(i)

def getBrokerWithPartitionNum(selectedTopic, partitionNo):
    if partitionNo in selectedTopic["Broker1"]:
        return "Broker1"
    elif partitionNo in selectedTopic["Broker2"]:
        return "Broker2"
    elif partitionNo in selectedTopic["Broker3"]:
        return "Broker3"

def createFile(filepath, data):
    with open( filepath + str(getPreviousOffset(filepath)), "w") as file:
        file.write(data)

def getPreviousOffset(directory):
    sp = subprocess.Popen(["ls", directory], shell= False, stdout= subprocess.PIPE, stderr= subprocess.PIPE)
    noOfFiles, err = sp.communicate()
    # print(noOfFiles.decode().splitlines())
    return len(noOfFiles.decode().splitlines())

def checkTopics(topicName):
    # cmd = ["ls"]
    for directory in ["Broker1", "Broker2", "Broker3"]:
        try:
            sp = subprocess.Popen(["ls", directory], shell= False, stdout= subprocess.PIPE, stderr= subprocess.PIPE, universal_newlines= True)
            topics, err = sp.communicate()
            # print(topics.splitlines())
            if topicName in topics.splitlines():
                return True
        except:
            print("ERROR in CheckTopics",err)
    # print(existingTopics)
    return False

def createFolders(broker, topicName, partitionName):
    topicName = topicName.replace(" ", "_")
    p = subprocess.Popen(f"mkdir Broker{broker}/{topicName}/{partitionName}", shell= True)
    p.communicate()

def createTopics(topicName):
    topicName = topicName.replace(" ", "_")
    for i in range(len(BROKER_PORTS)):
        p = subprocess.Popen(f"mkdir Broker{i+1}/{topicName}", shell= True)
        p.communicate()

def createPartitions(topicName, nPartitions):
    
    log = {
        "Broker1": [],
        "Broker2": [],
        "Broker3": [],
        "start": -1,
        "end": -1, 
        "nPartitions": nPartitions 
        }

    with open("zoolog.json", "r+") as file:
        logdata = json.load(file)
        # print(logdata, type(logdata))
        if not checkTopics(topicName):
            createTopics(topicName)
            for i in range(nPartitions):
                
                leader = i % len(BROKER_PORTS)
                # create folder
                createFolders(leader+1, topicName, i)
                log[f"Broker{leader+1}"].append(i)
            logdata.append({topicName: log})
            file.seek(0)
            json.dump(logdata, file, indent= 4)
            # print(logdata)
        else:
            return False
        return True

def writeMessages(topicName, broker, data):
    logdata = None
    with open("zoolog.json", "r+") as file:
        logdata = json.load(file)

    selectedTopic = list(filter(lambda topics: list(topics.keys())[0] == topicName, logdata))[0]

    # print(selectedTopic)

    if selectedTopic[topicName]["start"] < 0:
        # 
        # print("Partition", selectedTopic[topicName][broker][0])
        partitionNo = selectedTopic[topicName][broker][0]
        
        filepath = BASE_DIR + f"/{broker}" + f"/{topicName}" + f"/{partitionNo}/"
        # print(filepath)
        # print(partitionNo, data)
        # print()
        createFile(filepath, data)
        selectedTopic[topicName]["start"] = partitionNo
        selectedTopic[topicName]["end"] = partitionNo

        for topic in logdata:
            if list(topic.keys())[0] == topicName:
                # logdata[topic] = selectedTopic
                index = logdata.index(topic)
                logdata[index][topicName] = selectedTopic[topicName]
                break
    else:
        nextPartition = (selectedTopic[topicName]["end"] + 1) % selectedTopic[topicName]["nPartitions"]
        
        newBroker = None

        if nextPartition in selectedTopic[topicName]["Broker1"]:
            newBroker = "Broker1"
        elif nextPartition in selectedTopic[topicName]["Broker2"]:
            newBroker = "Broker2"
        elif nextPartition in selectedTopic[topicName]["Broker3"]:
            newBroker = "Broker3"
        
        filepath = BASE_DIR + f"/{newBroker}" + f"/{topicName}" + f"/{nextPartition}/"
        # print(filepath)
        # print(nextPartition, data)
        # print()
        createFile(filepath, data)
        selectedTopic[topicName]["end"] = nextPartition

        for topic in logdata:
            if list(topic.keys())[0] == topicName:
                # logdata[topic] = selectedTopic
                index = logdata.index(topic)
                logdata[index][topicName] = selectedTopic[topicName]
                break
        
    with open("zoolog.json", "w") as newfile:
        json.dump(logdata, newfile, indent= 4)
        
def fileExists(dir, filename):
    sp = subprocess.Popen(["ls", dir], shell= False, stdout= subprocess.PIPE, stderr= subprocess.PIPE, universal_newlines= True)
    files, err = sp.communicate()
    # print(files)
    if filename in files.splitlines():
        return True
    return False

def readFromBeginning(topicName, consumerPort, client):
    logdata = None
    with open("zoolog.json", "r") as file:
        logdata = json.load(file)

    selectedTopic = list(filter(lambda topics: list(topics.keys())[0] == topicName, logdata))[0]
    # print(selectedTopic)
    startPartition = selectedTopic[topicName]["start"]
    endPartition = selectedTopic[topicName]["end"]
    nPartitions = selectedTopic[topicName]["nPartitions"]
    # print(stratPartition, nPartitions)

    # loop
    # dataFile = 0
    while True:
        checkFilePath = BASE_DIR + "/" + getBrokerWithPartitionNum(selectedTopic[topicName], startPartition)+ f"/{topicName}"  + f"/{startPartition}/"

        if not fileExists(checkFilePath, str(consumerPort) + ".temp"):
            # print("Creating new file")
            with open( checkFilePath + str(consumerPort) + ".temp", "w") as file:
                file.write(json.dumps({"lastRead": -1}))
                file.close()
        
        lastReadFile = open(checkFilePath + str(consumerPort) + ".temp", "r")
        lastRead = json.loads(lastReadFile.readline())
        lastReadFile.close()
        # print(type(lastRead["lastRead"]))
        readpath = checkFilePath + f"{ lastRead['lastRead'] + 1}"
        # print(readpath)

        sp = subprocess.Popen(["ls", checkFilePath], shell= False, stdout= subprocess.PIPE, stderr= subprocess.PIPE, universal_newlines= True)
        files, err = sp.communicate()
        files = files.splitlines()
        reqFiles = list(filter(lambda x : ".temp" not in x, files))
        lastFile = reqFiles[-1]

        with open(readpath) as file:
            
            data = file.readline()
            # print(data)
            client.send(data.encode())
            file.close()
            # pass

        if startPartition == endPartition and str(lastRead["lastRead"] + 1) == lastFile:
            break

        lastReadFile = open(checkFilePath + str(consumerPort) + ".temp", "w")
        # print("lastRead: ", lastRead['lastRead'])
        lastReadFile.write(json.dumps({"lastRead": lastRead['lastRead'] + 1}))
        lastReadFile.close()
        
        startPartition = (startPartition + 1) % nPartitions
    client.send("agsv11".encode())
    client.close()
    deleteTempFiles(consumerPort)
    

# readFromBeginning("Football", 1234, "client")
# lastReadFile = open("/home/pes1ug20cs517/BD-project/v2/Broker2/Football/1/" + str(1234) + ".temp", "w")
# lastReadFile.write(json.dumps({"lastRead": 2 + 1}))
# lastReadFile.close()
# lastRead = open("/home/pes1ug20cs517/BD-project/v2/Broker2/Football/1/" + str(1234) + ".temp", "r").readline()
# print(lastRead, type(lastRead), type(json.loads(lastRead)))


# sp = subprocess.Popen(["ls", "/home/pes1ug20cs517/BD-project/v2/Broker2/Football/1/"], shell= False, stdout= subprocess.PIPE, stderr= subprocess.PIPE, universal_newlines= True)
# files, err = sp.communicate()
# files = files.splitlines()
# print(files)
# reqFiles = list(filter(lambda x : ".temp" not in x, files))
# print(reqFiles)
# lastFile = reqFiles[-1]
# print(lastFile)

















# createPartitions("Cricket", 10)
# writeMessages("Cricket", "Broker1", "Hello this is test")
# createFile("1", "Hello")
# print(getPreviousOffset("/home/pes1ug20cs517/BD-project/v2/Broker1"))

def test():
    topicName = "Cricket"
    with open("zoolog.json", "r+") as file:
        logdata = json.load(file)
        selectedTopic = list(filter(lambda topics: list(topics.keys())[0] == topicName, logdata))[0]
        # logdata = json.load(file)

        # for topic in logdata:
        #     print(topic)
        print(logdata)

        for topic in logdata:
            if list(topic.keys())[0] == topicName:
                # logdata[topic] = selectedTopic
                print(logdata.index(topic))
                break

def testData():
    # /home/pes1ug20cs517/BD-project/producerData.txt
    filePath = "/home/pes1ug20cs517/BD-project/v1/producerData.txt"
    file = open(filePath, "r")

    for lineNo, line in enumerate(file.readlines()):
        # c.send(line.encode())
        print(lineNo, line)
        print("*"*100)

# testData()

# test()

# /home/pes1ug20cs517/BD-project/v1/producerData.txt

deleteTempFiles(40058)