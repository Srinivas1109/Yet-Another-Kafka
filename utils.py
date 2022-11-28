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