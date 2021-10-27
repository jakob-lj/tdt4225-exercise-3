from DbConnector import DbConnector

USER_COLLECTION = "User"
ACTIVITY_COLLECTION = "Activity"
TRACKING_POINT_COLLECTION = "TrackingPoint"


def task1(connector):
    users = connector.db[USER_COLLECTION].count_documents({})
    activites = connector.db[ACTIVITY_COLLECTION].count_documents({})
    trackingPoints = connector.db[TRACKING_POINT_COLLECTION].estimated_document_count(
    )

    print("Number of users:", users)
    print("Number of activities:", activites)
    print("Number of tracking points:", trackingPoints)
    return {'users': users, 'activities': activites, 'trackingPoints': trackingPoints}


def task2(connector, totalAcitivites=16048):
    users = connector.db[USER_COLLECTION].find({})
    highest = 0

    lowest = totalAcitivites
    totalUsers = 0
    for u in users:
        thisLenght = len(u['activities'])
        if (thisLenght > highest):
            highest = thisLenght
        if thisLenght < lowest:
            lowest = thisLenght
        totalUsers += 1

    print("min:", lowest)
    print("max:", highest)
    print("avg: %.2f" % (totalAcitivites / totalUsers))


def task3(connector):
    aggregation = connector.db[USER_COLLECTION].aggregate([
        {"$project": {"count": {"$size": "$activities"},
                      "textIdentifier": "$textIdentifier"}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    print("The top 10 most axtive users:")
    print("User, activity")
    for u in aggregation:
        print("%4s | %8s" % (u['textIdentifier'], u['count']))


def task4(connector):
    pipeline = [
        {"$project": {"activity": "$activity", "userId": "$userId", "start": {"$dateToString": {
            "format": "%Y-%m-%d", "date": "$start"}}, "end": {"$dateToString": {"format": "%Y-%m-%d", "date": "$end"}}}},
        {"$match": {"$expr": {"$ne": ["$start", "$end"]}}},
        {"$group": {"_id": "$userId"}},
        {"$group": {"_id": "null", "count": {"$count": {}}}}
    ]

    results = connector.db[ACTIVITY_COLLECTION].aggregate(pipeline)
    print("Number of users that have activites starting in one day and ending in another:", list(
        results)[0]['count'])


def task7(connector):

    def getUserById(id):
        res = connector.db[USER_COLLECTION].find({"_id": id})
        return res[0]

    pipeline = [
        {"$match": {"label": {"$ne": "taxi"}}}, {"$group": {"_id": "$userId"}}
    ]
    usersWhoHaveNeverTakenTaxi = connector.db[ACTIVITY_COLLECTION].aggregate(
        pipeline)

    print("Users who have never taken taxi:")
    sortedList = sorted(([getUserById(u['_id'])['textIdentifier']
                        for u in usersWhoHaveNeverTakenTaxi]))
    for s in sortedList:
        print(s)


if __name__ == '__main__':
    connector = DbConnector()

    # totalNumbers = task1(connector)

    # task2(connector, totalNumbers['activities'])

    # task3(connector)

    # task4(connector)

    task7(connector)
