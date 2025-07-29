"""
✅ Requirements
Install the required packages: `pip install google-analytics-data google-auth`
"""
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient, GetMetadataRequest
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, Filter, FilterExpression
from functools import partial

class GA4Client:

    def __init__(self, property_id: str, key_path: str):

        self.property_id = property_id
        self.credentials = service_account.Credentials.from_service_account_file(key_path)
        self.client = BetaAnalyticsDataClient(credentials=self.credentials)
    
    def get_counts(self, metric, start_date, end_date, event_names=None):
        """ Get total counts of metric for the date range (no dimensions, only event filtering). """

        dimension_filter = None
        if event_names:
            dimension_filter = FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    in_list_filter=Filter.InListFilter(values=event_names)
                )
            )
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[], 
            metrics=[Metric(name=metric)],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=dimension_filter
        )
        response = self.client.run_report(request)
        
        if response.rows:
            total_sessions = int(response.rows[0].metric_values[0].value)
            return total_sessions
        else:
            return 0


if __name__ == "__main__":

    event_names=["videostarts"]
    start_date = end_date = "2023-04-07"
    client = GA4Client("320198532", "./cru-ga4-prod-1-63a3434e5a2a.json" )

    get_total_events = partial(client.get_counts, 'eventCount')
    total_events = get_total_events(start_date, end_date, event_names=["videostarts"])
    print(f"Total events from {start_date} to {end_date}: {total_events}")

