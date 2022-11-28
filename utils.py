import subprocess

def checkTopics():
    cmd = ["ls"]
    sp = subprocess.Popen(cmd, shell= False, stdout= subprocess.PIPE, stderr= subprocess.PIPE, universal_newlines= True)
    topics, err = sp.communicate()
    print(topics.splitlines())
    print(err)
    return topics.splitlines()