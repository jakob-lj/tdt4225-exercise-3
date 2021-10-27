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
    pass


if __name__ == '__main__':
    connector = DbConnector()

    # totalNumbers = task1(connector)

    # task2(connector, totalNumbers['activities'])

    task3(connector)