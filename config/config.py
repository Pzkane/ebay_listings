# Note:
#   max_api_calls: how many API requests you'd like to send
#   urls_per_call: max = 200, https://developer.ebay.com/api-docs/sell/analytics/resources/traffic_report/methods/getTrafficReport
api = {
    "access_token": "token",
    "max_api_calls": 10,
    "urls_per_call": 200,
}

database = {
    "host": "localhost",
    "port": 27017,
    "name": "ebayapi",
    "collection": "listings",
}
