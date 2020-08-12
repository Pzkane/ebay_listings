#!/usr/bin/env 
from functools import partial
from itertools import repeat
from dateutil.relativedelta import relativedelta
import datetime, pprint
import concurrent.futures

from database import DBConnection
from pymongo import UpdateOne
from sell.analytics.traffic_report import GetTrafficReport
from config.config import database, api

def process_listing_metric_data(listingsIds, dbInstance, urls_per_call):
    todayDate = datetime.date.today().strftime("%Y%m%d")
    prevMonthDate = (datetime.date.today() + relativedelta(months=-1)).strftime("%Y%m%d")

    ebayTraffic = GetTrafficReport(
        api['access_token'],
        False
    )

    ebayTraffic.setPayload({
        "dimension": "LISTING",
        "metric": "LISTING_VIEWS_TOTAL",
        "filter": "listing_ids:{"+'|'.join([listingId for listingId in listingsIds])+"},date_range:["+prevMonthDate+".."+todayDate+"]"
    })

    response = ebayTraffic.getTrafficReport()
    print(response[1])

    if response[0]: #is_success
        bulkUpdateQuery = []
        for record in response[2]["results"]["records"]:
            listingsIds.remove(record['dimensionValues'][0]['value'])
            bulkUpdateQuery.append(
                UpdateOne(
                    { "offer.listing.listingId": f"{record['dimensionValues'][0]['value']}" },
                    {
                        "$set":
                            {
                                "offer.listing.total_views": f"{record['metricValues'][0]['value']}",
                                "offer.listing.is_processed": True,
                            },
                    },
                    upsert=False
                ),
            )

        for listingId in listingsIds:
            bulkUpdateQuery.append(
                UpdateOne(
                    { "offer.listing.listingId": f"{listingId}" },
                    {
                        "$set":
                            {
                                "offer.listing.does_not_exist": True,
                            },
                    },
                    upsert=False
                ),
            )

        dbInstance.execute_bulk_write(bulkUpdateQuery)
    else:
        print(response[1])

    return

def main():
    max_api_calls = api["max_api_calls"]
    urls_per_call = api["urls_per_call"]

    isProcessed = False

    endCursor = urls_per_call
    listingIds = []
    queryListingsList = []

    apiCallCount = 0
    listingCount = 0
    listEndReached = True

    db = DBConnection(database["host"], database["port"])
    db.set_database(database["name"])
    db.set_collection(database["collection"])
    records = db.get_records()

    for index, record in enumerate(records):
        try:
            if record["offer"]["listing"]["is_processed"] == True:
                isProcessed = True
            else:
                isProcessed = False
        except KeyError:
            isProcessed = False

        if not isProcessed:
            listingIds.append(record["offer"]["listing"]["listingId"])
            listingCount += 1
            if listingCount == endCursor:
                queryListingsList.append(listingIds)
                listingIds = []
                endCursor += urls_per_call
                apiCallCount += 1

        if apiCallCount >= max_api_calls:
            listEndReached = False
            break

    
    if listEndReached:
        queryListingsList.append(listingIds)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_listing_metric_data, queryListingsList, repeat(db), repeat(urls_per_call))

    if listEndReached:
        db.check_listings()
    
if __name__ == "__main__":
    main()
