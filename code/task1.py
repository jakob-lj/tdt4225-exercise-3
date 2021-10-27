import os
import datetime
import threading
import uuid
from DbConnector import DbConnector
from bson.objectid import ObjectId

relPath = "../dataset/dataset/Data"

files = sorted(os.listdir(relPath))

DATASET_STANDARD_LABEL_DATE_FORMAT = "%Y/%m/%d %H:%M:%S"

threads = []
threadsQue = []
total = 0
running = True

USER_COLLECTION = "User"
ACTIVITY_COLLECTION = "Activity"
TRACKING_POINT_COLLECTION = "TrackingPoint"


def formatLine(line, activity):
    lineElements = line.split(",")
    latitude = lineElements[0]
    longitude = lineElements[1]
    altitude = lineElements[3]
    timestamp = datetime.datetime.strptime(
        lineElements[5] + " " + lineElements[6], "%Y-%m-%d %H:%M:%S")

    id = ObjectId()
    return {'lat': latitude, 'long': longitude, 'altitude': altitude, 'timestamp': timestamp, '_id': id, 'activityId': activity['id']}


def insertActivity(col, activity, label, trackingPointIds, userId):

    col.insert_one({'_id': activity['id'], 'activity': activity['name'], 'label': label,
                   'trackingPoints': trackingPointIds, 'userId': userId})


def insertTrackingPoints(trackingPointCollection, results):
    trackingPointCollection.insert_many(results)


def insertUser(userCollection, userId, fileId, activities, hasLabels):
    userCollection.insert_one(
        {'_id': userId, 'activities': [x['id'] for x in activities], 'textIdentifier': fileId, 'hasLabels': hasLabels})


def readActivity(trackingPointCollection, activityCollection, activity, fileId, labels, userId):
    currentLabel = None

    with open(relPath + "/" + fileId + "/Trajectory/" + activity['name'] + ".plt") as f:
        data = [x.strip() for x in f.readlines()][6:]
        if len(data) > 2500:
            return

        results = []
        for line in data:
            formattedLine = (formatLine(line, activity))
            results.append(formattedLine)

        for l in labels:
            if (l[0] == results[0]['timestamp']):
                if (l[1] == results[-1]['timestamp']):
                    currentLabel = l[2]
                    break

        insertTrackingPoints(trackingPointCollection, results)
        insertActivity(activityCollection, activity,
                       currentLabel, [x['_id'] for x in results], userId)


def readActivities(fileId, hasLabels=False):
    connection = DbConnector()
    labels = []
    userId = ObjectId()
    activities = [{'name': str(x).split(".")[0], 'id':ObjectId()} for x in os.listdir(
        relPath + "/" + fileId + "/Trajectory")]
    if (hasLabels):
        with open(relPath + "/" + fileId + "/labels.txt", "r") as f:
            labelData = [x.strip().replace("\t", " ").split()
                         for x in f.readlines()][1:]
            labels = [[datetime.datetime.strptime(x[0] + " " + x[1], DATASET_STANDARD_LABEL_DATE_FORMAT), datetime.datetime.strptime(
                x[2] + " " + x[3], DATASET_STANDARD_LABEL_DATE_FORMAT), x[4]] for x in labelData]

    for activity in activities:
        readActivity(
            connection.db[TRACKING_POINT_COLLECTION], connection.db[ACTIVITY_COLLECTION], activity, fileId, labels, userId)

    insertUser(connection.db[USER_COLLECTION],
               userId, fileId, activities, hasLabels)


def worker():
    global threadsQue
    global running
    global total

    while len(threadsQue) > 0:
        current = threadsQue.pop(0)
        print("working with", current)
        readActivities(current['file'], current['hasLabels'])
        print("finished", current)
        print(len(threadsQue) / total * 100)


if __name__ == '__main__':

    rootConn = DbConnector()
    dryRun = False
    activated = True

    if (not dryRun):
        rootConn.db[USER_COLLECTION].drop()
        rootConn.db[ACTIVITY_COLLECTION].drop()
        rootConn.db[TRACKING_POINT_COLLECTION].drop()
        rootConn.db.create_collection(USER_COLLECTION)
        rootConn.db.create_collection(ACTIVITY_COLLECTION)
        rootConn.db.create_collection(TRACKING_POINT_COLLECTION)

    if (not dryRun):
        for file in files:
            if (file == "010" or file == "001" or activated):
                users = os.listdir(relPath + "/" + file)
                threadsQue.append(
                    {'file': file, 'hasLabels': "labels.txt" in users})
                print("adding", file)
            else:
                continue

        total = len(threadsQue)

        for i in range(10):
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    print("Done!")
