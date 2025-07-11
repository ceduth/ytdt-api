"""
✅ Requirements
Install the required packages: `pip install google-analytics-data`

✅ Google Cloud Setup (One-time)
    1. Create a service account in Google Cloud Console.
    2. Enable "Google Analytics Data API".
    3. Download the service account key JSON.
    4. Grant the service account access to your GA4 property (read permissions) 
        in the GA Admin UI under Admin > Property > Property Access Management.

"""
import csv
from google.analytics.data_v1beta import BetaAnalyticsDataClient, GetMetadataRequest
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account


EXCLUDED_METRICS = {
    "advertiserAdCost",
    "advertiserAdCostPerClick",
    "advertiserAdCostPerKeyEvent",
    "advertiserAdClicks",
    "advertiserAdImpressions"
}

class GA4Client:

    def __init__(self, property_id: str, key_path: str):

        self.property_id = property_id
        self.credentials = service_account.Credentials.from_service_account_file(key_path)
        self.client = BetaAnalyticsDataClient(credentials=self.credentials)

    # def get_report(self, start_date: str, end_date: str):

    #     request = RunReportRequest(
    #         property=f"properties/{self.property_id}",
    #         dimensions=[Dimension(name="date")],
    #         metrics=[Metric(name="sessions")],
    #         date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    #     )

    #     response = self.client.run_report(request)
    #     return response

    # def list_available_metrics(self):
    #     """ Get All Available Metrics for GA4 Property """
    #     metadata = self.client.get_metadata(
    #         request=GetMetadataRequest(name=f"properties/{self.property_id}/metadata")
    #     )

    #     print("Available metrics:")
    #     for metric in metadata.metrics:
    #         print(f"{metric.api_name} — {metric.ui_name} ({metric.description})")

    def get_all_fields(self):
        metadata = self.client.get_metadata(
            request=GetMetadataRequest(name=f"properties/{self.property_id}/metadata")
        )
        dims = [d.api_name for d in metadata.dimensions]
        mets = [m.api_name for m in metadata.metrics]
        safe_metrics = [m for m in mets if m not in EXCLUDED_METRICS]

        return dims, safe_metrics

    def get_report(self, dimensions, metrics, start_date, end_date):
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=10000
        )
        return self.client.run_report(request)
    
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
    output_csv = "data/ga4_simple_report.csv"

    start_date, end_date = "2023-03-01", "2023-03-01"
    client = GA4Client(property_id, key_path)

    # report = client.get_report(start_date, end_date)
    # client.list_available_metrics()
    # all_dims, all_mets = client.get_all_fields()

    # all_dims = ['date', 'browser', 'campaignId']
    # all_mets = ['sessions']

    # all_dims = ['dateHourMinute', 'customEvent:jfid', 'customEvent:ssoguid', 'customEvent:global_id', 
    #             'customEvent:mediacomponentid', 'signedInWithUserId', 'audienceId']
    # all_mets = ['sessions']

    # all_dims = ['browser', 'deviceCategory', 'operatingSystem', 'screenResolution', 'city', ]
    all_dims = [
        'mobileDeviceMarketingName',  
        'mobileDeviceModel',         
        'mobileDeviceBranding',   
        'deviceModel',              
        'operatingSystemWithVersion', 
        'operatingSystemVersion',    
        'screenResolution',        
        'platformDeviceCategory',   
        'appVersion'                
        ]
    all_mets = ['sessions']


    # GA4 limits: max 9 dimensions, 10 metrics per report
    selected_dims = all_dims[:9]
    selected_mets = all_mets[:10]

    report = client.get_report(
        dimensions=selected_dims,
        metrics=selected_mets,
        start_date=start_date,
        end_date=end_date
    )
    
    # client.print_report(report)
    client.save_to_csv(report, output_csv)