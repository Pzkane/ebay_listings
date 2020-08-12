from .base import AnalyticsAPI
import requests

class TrafficReportAPI(AnalyticsAPI):
    resource = "traffic_report"

import pprint

class GetTrafficReport(TrafficReportAPI):
    """
    Docs:
    https://developer.ebay.com/api-docs/sell/analytics/resources/traffic_report/methods/getTrafficReport
    """

    method_type = "GET"
    required_query_params = ["dimension", "metric", "filter"]

    def setPayload(self, payload):
        self.payload = payload
        
    def getTrafficReport(self):
        response = self.make_request(
            None,
            {},
            self.payload
        )

        return response