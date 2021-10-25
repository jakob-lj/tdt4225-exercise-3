import os
import datetime
import threading

relPath = "../dataset/dataset/Data"

files = sorted(os.listdir(relPath))

DATASET_STANDARD_LABEL_DATE_FORMAT = "%Y/%m/%d %H:%M:%S"

threads = []


def formatLine(line):
    lineElements = line.split(",")
    latitude = lineElements[0]
    longitude = lineElements[1]
    altitude = lineElements[3]
    timestamp = datetime.datetime.strptime(
        lineElements[5] + " " + lineElements[6], "%Y-%m-%d %H:%M:%S")
    return [latitude, longitude, altitude, timestamp]


def readActivity(activity, fileId, labels):
    currentLabel = None
    print(activity)

    with open(relPath + "/" + fileId + "/Trajectory/" + activity) as f:
        data = [x.strip() for x in f.readlines()][6:]
        results = []
        for line in data:
            results.append(formatLine(line))

        print(results)


def readActivities(fileId, hasLabels=False):
    labels = []
    activities = os.listdir(relPath + "/" + fileId + "/Trajectory")
    if (hasLabels):
        with open(relPath + "/" + fileId + "/labels.txt", "r") as f:
            labelData = [x.strip().replace("\t", " ").split()
                         for x in f.readlines()][1:]
            labels = [[datetime.datetime.strptime(x[0] + " " + x[1], DATASET_STANDARD_LABEL_DATE_FORMAT), datetime.datetime.strptime(
                x[2] + " " + x[3], DATASET_STANDARD_LABEL_DATE_FORMAT), x[4]] for x in labelData]

    for activity in activities:
        readActivity(activity, fileId, labels)
        break


for file in files:
    if (file == "010"):
        users = os.listdir(relPath + "/" + file)
        readActivities(file, "labels.txt" in users)
        print(users)
        print(file)
    else:
        continue
