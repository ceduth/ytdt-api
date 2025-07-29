"""
✅ Requirements
Install the required packages: `pip install google-analytics-data`

✅ Google Cloud Setup (One-time)
    1. Create a service account in Google Cloud Console.
    2. Enable "Google Analytics Data API".
    3. Download the service account key JSON.
    4. Grant the service account access to your GA4 property (read permissions) 
        in the GA Admin UI under Admin > Property > Property Access Management.

✅ Usage:

    # Get sessions count for a date range
    >>> client = GA4Client("320198532", "./cru-ga4-prod-1-63a3434e5a2a.json" )
    >>> total_sessions = client.get_total_sessions("2023-03-01", "2023-07-16", event_names=["videostarts"])

"""
import csv
from datetime import datetime, timedelta

from google.analytics.data_v1beta import BetaAnalyticsDataClient, GetMetadataRequest
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, \
    FilterExpression, Filter
from google.oauth2 import service_account



class GA4Client:

    def __init__(self, property_id: str, key_path: str):

        self.property_id = property_id
        self.credentials = service_account.Credentials.from_service_account_file(key_path)
        self.client = BetaAnalyticsDataClient(credentials=self.credentials)

    def get_all_fields(self, excluded_metrics=[]):
        metadata = self.client.get_metadata(
            request=GetMetadataRequest(name=f"properties/{self.property_id}/metadata")
        )
        dims = [d.api_name for d in metadata.dimensions]
        mets = [m.api_name for m in metadata.metrics]
        safe_metrics = [m for m in mets if m not in excluded_metrics]
        return dims, safe_metrics

    def get_report(self, dimensions, metrics, start_date, end_date, event_names=None, limit=10000):

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
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=dimension_filter,
            limit=limit
        )

        response = self.client.run_report(request)
        print(f"Fetched {response.row_count} rows.")
        return response

    def print_report(self, response, max_rows=10):
        for i, row in enumerate(response.rows):
            while i <= max_rows:
                print({dimension.name: value.value for dimension, value in zip(response.dimension_headers, row.dimension_values)},
                    {metric.name: value.value for metric, value in zip(response.metric_headers, row.metric_values)})

    # TODO: use save_to_csv from utils/csv.py
    def save_to_csv(self, response, output_path: str):
        headers = [d.name for d in response.dimension_headers] + [m.name for m in response.metric_headers]
        
        with open(output_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            
            for row in response.rows:
                row_data = [dim.value for dim in row.dimension_values] + \
                           [met.value for met in row.metric_values]
                writer.writerow(row_data)
        print(f"✅ CSV report saved to: {output_path}")


if __name__ == "__main__":

    property_id = "320198532"
    key_path = "./cru-ga4-prod-1-63a3434e5a2a.json"
    event_names = ['videostarts', 'session_start', 'first_visit']

    # GA4 limits: max 9 dimensions, 10 metrics per report
    dimensions = [
        "eventName",
        # "dateHourMinute",
        "customEvent:mediacomponentid",
        "customEvent:languageid",
        # "pageLocation",
        # "sessionSource",
        # "sessionMedium",
        # "countryId"
    ]
    metrics = [
        "eventCount", # "sessions", "engagedSessions", "activeUsers"
    ]

    client = GA4Client(property_id, key_path)

    start_date, end_date = "2023-03-01", "2023-07-16"

    
    report = client.get_report(
        dimensions=dimensions,
        metrics=metrics,
        start_date=start_date,
        end_date=end_date,
        event_names=event_names,
        limit=250000
    )
    
    client.save_to_csv(report, "data/ga4_totals.csv")
    
