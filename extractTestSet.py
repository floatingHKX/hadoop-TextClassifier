import sys
import os
import random

def findExist(n, res):
    for t in res:
        if n == t:
            return False
    return True

def generateRandom(need,sum):
    res = []
    cord = 0
    while(cord < need):
        tmp = random.randint(0,sum)
        if findExist(tmp, res):
            res.append(tmp)
            cord += 1
    return res

rootdir = "train"
aimdir = "test"

if os.path.exists(aimdir) == False:
    os.mkdir(aimdir)

categoryList = os.listdir(rootdir)
for i in range(0, len(categoryList)):
    path = os.path.join(rootdir, categoryList[i])
    testpath = os.path.join(aimdir, categoryList[i])

    if os.path.exists(testpath) == False:
        os.mkdir(testpath)
    if os.path.isdir(path):
        textList = os.listdir(path)
        size = textList.__len__()
        print size
        randsum = size * 0.3
        print randsum
        testIndexList = generateRandom(int(randsum), size-1)
        print testIndexList

        for index in testIndexList:
            filepath = os.path.join(path, textList[index])
            aimfilepath = os.path.join(aimdir, categoryList[i], textList[index])
            command = "move %s %s" % (filepath, aimfilepath)
            os.system(command)

