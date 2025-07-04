"""
GA4 Data Extraction with date range chunking strategy
=====================================================

GA4 Data Extraction with 8 prioritized dimensions (+ eventName filtering) 
with intelligent metric batching and pagination support. 

Implements date range chunking to handle large datasets that cause 504 Deadline Exceeded errors:
- Splits large date ranges into manageable chunks (7-14 days)
- Progress tracking and recovery from failures
- Incremental saving of chunks
- Automatic retry logic
- Memory efficient processing

✅ Requirements:
pip install google-analytics-data google-auth pandas

🎯 Usage:

    python ga4.py

    -- 

    from lib.ga4 import GA4Client
    client = GA4Client(property_id, key_path)
    df = client.get_comprehensive_report(start_date, end_date, event_names, limit_rows=1000, get_all_data=False)
    client.save_to_csv(df, "output.csv")

📝 Doc:
    https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema
    https://ga-dev-tools.google/ga4/dimensions-metrics-explorer/

"""

import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, 
    FilterExpression, Filter
)
from google.oauth2 import service_account
import time
import os
import json
from datetime import datetime, timedelta


class GA4Client:
    """Enhanced GA4 client with date range chunking for handling large datasets"""

    def __init__(self, property_id: str, key_path: str):
        self.property_id = property_id
        self.credentials = service_account.Credentials.from_service_account_file(key_path)
        self.client = BetaAnalyticsDataClient(credentials=self.credentials)
        self.max_metrics_per_request = 10
        
        # Create progress tracking directory
        self.progress_dir = "data/ga4_chunks"
        os.makedirs(self.progress_dir, exist_ok=True)

    def _batch_metrics(self, metrics: List[Metric]) -> List[List[Metric]]:
        """Batch metrics into groups of 10 (API limit)"""
        batches = []
        for i in range(0, len(metrics), self.max_metrics_per_request):
            batches.append(metrics[i:i + self.max_metrics_per_request])
        return batches

    def _get_prioritized_dimensions(self) -> List[Dimension]:
        """Exact 8 core dimensions"""
        return [
            Dimension(name="dateHourMinute"),
            Dimension(name="customEvent:mediacomponentid"),
            Dimension(name="languageCode"),
            Dimension(name="countryId"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
            Dimension(name="pageLocation"),
            Dimension(name="eventName"),
        ]

    def _get_all_metrics(self) -> List[Metric]:
        """All metrics from SQL query"""
        return [
            # Standard GA4 metrics
            Metric(name="activeUsers"),
            Metric(name="sessions"),
            Metric(name="engagedSessions"),
            Metric(name="userEngagementDuration"),
            Metric(name="screenPageViews"),
            Metric(name="bounceRate"),
            Metric(name="engagementRate"),
            Metric(name="sessionsPerUser"),
            Metric(name="averageSessionDuration"),
            Metric(name="eventValue"),
            Metric(name="newUsers"),
            Metric(name="totalUsers"),
            
            # Custom event metrics
            Metric(name="customEvent:activity_completes"),
            Metric(name="customEvent:video_view_time"),
            Metric(name="customEvent:videostarts"),
            Metric(name="customEvent:videocomplete"),
            Metric(name="customEvent:button_click"),
            Metric(name="customEvent:link_click"),
            Metric(name="customEvent:download_media"),
            Metric(name="customEvent:download_queued"),
            Metric(name="customEvent:subscribe"),
            Metric(name="customEvent:value"),
            Metric(name="customEvent:processing_fee"),
            Metric(name="customEvent:video_percent_yt"),
            Metric(name="customEvent:video_view_time_yt"),
        ]

    # =============================================
    # DATE RANGE CHUNKING IMPLEMENTATION
    # =============================================

    def generate_date_chunks(self, start_date: str, end_date: str, chunk_days: int = 10) -> List[Tuple[str, str]]:
        """
        Split date range into smaller chunks to avoid API timeouts
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            chunk_days: Days per chunk (default 10)
        
        Returns:
            List of (start_date, end_date) tuples for each chunk
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        chunks = []
        current = start
        
        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            chunks.append((
                current.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d")
            ))
            current = chunk_end + timedelta(days=1)
        
        return chunks

    def _get_chunk_filename(self, chunk_start: str, chunk_end: str, chunk_num: int) -> str:
        """Generate filename for chunk data"""
        return f"{self.progress_dir}/chunk_{chunk_num:02d}_{chunk_start}_to_{chunk_end}.csv"

    def _save_chunk_progress(self, chunk_num: int, total_chunks: int, chunk_start: str, chunk_end: str, 
                           rows: int, status: str):
        """Save progress information for recovery"""
        progress_file = f"{self.progress_dir}/progress.json"
        
        progress_data = {
            "current_chunk": chunk_num,
            "total_chunks": total_chunks,
            "chunk_start": chunk_start,
            "chunk_end": chunk_end,
            "rows_processed": rows,
            "status": status,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(progress_file, 'w') as f:
            json.dump(progress_data, f, indent=2)

    def _load_existing_chunks(self) -> List[str]:
        """Load list of already completed chunk files"""
        if not os.path.exists(self.progress_dir):
            return []
        
        chunk_files = []
        for file in os.listdir(self.progress_dir):
            if file.startswith("chunk_") and file.endswith(".csv"):
                chunk_files.append(os.path.join(self.progress_dir, file))
        
        return sorted(chunk_files)

    def extract_single_chunk(self, start_date: str, end_date: str, 
                           event_names: List[str] = None,
                           limit_rows: int = 50000,
                           max_retries: int = 3) -> pd.DataFrame:
        """
        Extract data for a single date chunk with retry logic
        
        Args:
            start_date: Chunk start date
            end_date: Chunk end date
            event_names: Events to filter for
            limit_rows: Rows per API request (reduced from 100K)
            max_retries: Number of retry attempts
        """
        
        for attempt in range(max_retries):
            try:
                print(f"      🔄 Attempt {attempt + 1}/{max_retries} for chunk {start_date} to {end_date}")
                
                # Use the existing comprehensive report method with smaller row limit
                chunk_df = self._execute_metric_batched_requests(
                    start_date=start_date,
                    end_date=end_date,
                    event_names=event_names,
                    limit_rows=limit_rows,  # Smaller chunks for reliability
                    get_all_data=True
                )
                
                print(f"      ✅ Chunk successful: {len(chunk_df):,} rows")
                return chunk_df
                
            except Exception as e:
                print(f"      ❌ Attempt {attempt + 1} failed: {str(e)[:100]}...")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 10  # Exponential backoff
                    print(f"      ⏳ Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                else:
                    print(f"      💥 All attempts failed for chunk {start_date} to {end_date}")
                    raise e

    def get_report(self, start_date: str, end_date: str,
                                       event_names: List[str] = None,
                                       chunk_days: int = 10,
                                       limit_rows: int = 50000,
                                       save_chunks: bool = True,
                                       resume_from_chunks: bool = True) -> pd.DataFrame:
        """
        Main method: Extract comprehensive GA4 report using date range chunking
        
        Args:
            start_date: Overall start date (YYYY-MM-DD)
            end_date: Overall end date (YYYY-MM-DD)
            event_names: List of event names to filter for
            chunk_days: Days per chunk (default 10)
            limit_rows: Rows per API request (reduced for reliability)
            save_chunks: Save individual chunks to disk
            resume_from_chunks: Resume from existing chunk files
        
        Returns:
            Combined DataFrame with all data
        """
        
        print(f"🚀 GA4 Chunked Extraction Strategy")
        print(f"=" * 60)
        print(f"📅 Overall date range: {start_date} to {end_date}")
        print(f"🎯 Events: {event_names}")
        print(f"📊 Chunk size: {chunk_days} days")
        print(f"🔄 Rows per request: {limit_rows:,}")
        print(f"💾 Save chunks: {save_chunks}")
        print(f"🔄 Resume mode: {resume_from_chunks}")
        
        # Generate date chunks
        chunks = self.generate_date_chunks(start_date, end_date, chunk_days)
        print(f"📦 Generated {len(chunks)} chunks:")
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            print(f"   Chunk {i:2d}: {chunk_start} to {chunk_end}")
        
        # Check for existing chunks if resume mode is enabled
        existing_chunks = []
        if resume_from_chunks:
            existing_chunk_files = self._load_existing_chunks()
            if existing_chunk_files:
                print(f"\n🔄 Found {len(existing_chunk_files)} existing chunk files:")
                for chunk_file in existing_chunk_files:
                    print(f"   📁 {os.path.basename(chunk_file)}")
                    try:
                        chunk_df = pd.read_csv(chunk_file)
                        existing_chunks.append(chunk_df)
                        print(f"      ✅ Loaded: {len(chunk_df):,} rows")
                    except Exception as e:
                        print(f"      ❌ Error loading {chunk_file}: {e}")
        
        # Process remaining chunks
        new_chunks = []
        completed_chunk_count = len(existing_chunks)
        
        for i, (chunk_start, chunk_end) in enumerate(chunks[completed_chunk_count:], completed_chunk_count + 1):
            print(f"\n📦 Processing Chunk {i}/{len(chunks)}: {chunk_start} to {chunk_end}")
            
            try:
                # Extract chunk data
                chunk_df = self.extract_single_chunk(
                    start_date=chunk_start,
                    end_date=chunk_end,
                    event_names=event_names,
                    limit_rows=limit_rows
                )
                
                # Save chunk if requested
                if save_chunks:
                    chunk_filename = self._get_chunk_filename(chunk_start, chunk_end, i)
                    chunk_df.to_csv(chunk_filename, index=False, encoding='utf-8')
                    print(f"      💾 Saved chunk to: {os.path.basename(chunk_filename)}")
                
                new_chunks.append(chunk_df)
                
                # Save progress
                self._save_chunk_progress(i, len(chunks), chunk_start, chunk_end, len(chunk_df), "completed")
                
                print(f"      ✅ Chunk {i} completed: {len(chunk_df):,} rows")
                
                # Rate limiting between chunks
                time.sleep(2)
                
            except Exception as e:
                print(f"      💥 Chunk {i} failed: {e}")
                self._save_chunk_progress(i, len(chunks), chunk_start, chunk_end, 0, "failed")
                
                # Continue with next chunk instead of failing completely
                print(f"      🔄 Continuing with next chunk...")
                continue
        
        # Combine all chunks
        all_chunks = existing_chunks + new_chunks
        
        if not all_chunks:
            print(f"\n❌ No data chunks available!")
            return pd.DataFrame()
        
        print(f"\n🔄 Combining {len(all_chunks)} chunks...")
        combined_df = pd.concat(all_chunks, ignore_index=True, sort=False)
        
        # Remove duplicates that might occur at chunk boundaries
        print(f"   📊 Before deduplication: {len(combined_df):,} rows")
        
        # Deduplicate based on all dimension columns
        dimension_cols = [col for col in combined_df.columns[:8]]  # First 8 are dimensions
        combined_df = combined_df.drop_duplicates(subset=dimension_cols, keep='first')
        
        print(f"   📊 After deduplication: {len(combined_df):,} rows")
        print(f"   📋 Final shape: {combined_df.shape}")
        
        print(f"\n✅ Chunked extraction completed successfully!")
        print(f"   📦 Total chunks processed: {len(all_chunks)}")
        print(f"   📊 Final dataset: {len(combined_df):,} rows × {len(combined_df.columns)} columns")
        
        return combined_df

    def _execute_metric_batched_requests(self, start_date: str, end_date: str, 
                                       event_names: List[str] = None, 
                                       limit_rows: int = 50000,
                                       get_all_data: bool = False) -> pd.DataFrame:
        """Execute multiple requests with SAME dimensions, different metric batches"""
        
        dimensions = self._get_prioritized_dimensions()
        all_metrics = self._get_all_metrics()
        metric_batches = self._batch_metrics(all_metrics)
        
        # Set up event filter for specific events
        dimension_filter = None
        if event_names:
            dimension_filter = FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    in_list_filter=Filter.InListFilter(values=event_names)
                )
            )
        
        # Execute requests and collect all dataframes
        all_dataframes = []
        
        for batch_num, metric_batch in enumerate(metric_batches, 1):
            if get_all_data:
                # Use pagination to get all data for this metric batch
                batch_dataframes = self._get_all_data_with_pagination(
                    dimensions, metric_batch, start_date, end_date, 
                    dimension_filter, limit_rows
                )
                # Combine all pages for this batch into single dataframe
                if batch_dataframes:
                    combined_batch_df = pd.concat(batch_dataframes, ignore_index=True)
                    all_dataframes.append(combined_batch_df)
            else:
                # Single request
                df = self._execute_single_request(
                    dimensions, metric_batch, start_date, end_date,
                    dimension_filter, limit_rows, batch_num
                )
                all_dataframes.append(df)
        
        # Merge all dataframes properly
        return self._merge_same_dimension_dataframes(all_dataframes)

    def _get_all_data_with_pagination(self, dimensions: List[Dimension], 
                                    metrics: List[Metric], start_date: str, 
                                    end_date: str, dimension_filter, 
                                    limit_per_request: int = 50000) -> List[pd.DataFrame]:
        """Get ALL data for a single metric batch using pagination"""
        paginated_dataframes = []
        offset = 0
        total_rows_retrieved = 0
        page_num = 1
        
        while True:
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=dimensions,
                metrics=metrics,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimension_filter=dimension_filter,
                order_bys=[
                    {"dimension": {"dimension_name": "dateHourMinute"}},
                    {"dimension": {"dimension_name": "eventName"}},
                    {"dimension": {"dimension_name": "countryId"}},
                ],
                offset=offset,
                limit=limit_per_request,
            )
            
            response = self.client.run_report(request)
            df = self._response_to_dataframe(response)
            
            if len(df) == 0:
                break
            
            paginated_dataframes.append(df)
            total_rows_retrieved += len(df)
            
            # If we got fewer rows than the limit, we've reached the end
            if len(df) < limit_per_request:
                break
            
            offset += limit_per_request
            page_num += 1
            
            # Rate limiting between pagination requests
            time.sleep(0.3)  # Slightly longer delay for chunked approach
        
        return paginated_dataframes

    def _execute_single_request(self, dimensions: List[Dimension], 
                              metrics: List[Metric], start_date: str, 
                              end_date: str, dimension_filter, 
                              limit_rows: int, batch_num: int) -> pd.DataFrame:
        """Execute a single request"""
        
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=dimension_filter,
            order_bys=[
                {"dimension": {"dimension_name": "dateHourMinute"}},
                {"dimension": {"dimension_name": "eventName"}},
                {"dimension": {"dimension_name": "countryId"}},
            ],
            limit=limit_rows,
        )
        
        response = self.client.run_report(request)
        df = self._response_to_dataframe(response)
        
        # Rate limiting
        time.sleep(0.2)
        
        return df

    def _merge_same_dimension_dataframes(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """Merge DataFrames with IDENTICAL dimensions but different metrics"""
        if not dataframes:
            return pd.DataFrame()
        
        if len(dataframes) == 1:
            return dataframes[0]
        
        # Get dimension columns from the first DataFrame
        first_df = dataframes[0]
        num_dimensions = 8  
        dimension_columns = first_df.columns[:num_dimensions].tolist()
        
        # Start with first DataFrame
        merged_df = dataframes[0].copy()
        
        # Merge additional DataFrames
        for i, df in enumerate(dataframes[1:], 1):
            # Ensure the DataFrame has the expected number of dimensions
            if len(df.columns) < num_dimensions:
                continue
            
            # Get dimension columns from this dataframe
            df_dimension_cols = df.columns[:num_dimensions].tolist()
            
            if dimension_columns != df_dimension_cols:
                continue
            
            # Get metric columns (everything after dimensions)
            df_metric_cols = df.columns[num_dimensions:].tolist()
            
            # Check for column conflicts
            existing_metric_cols = merged_df.columns[num_dimensions:].tolist()
            conflicting_cols = set(df_metric_cols) & set(existing_metric_cols)
            
            if conflicting_cols:
                # Only keep non-conflicting metric columns
                non_conflicting_metrics = [col for col in df_metric_cols if col not in conflicting_cols]
                if non_conflicting_metrics:
                    cols_to_merge = dimension_columns + non_conflicting_metrics
                    df_to_merge = df[cols_to_merge]
                else:
                    continue
            else:
                df_to_merge = df
            
            # Merge on dimension columns only
            try:
                merged_df = pd.merge(
                    merged_df, 
                    df_to_merge, 
                    on=dimension_columns, 
                    how='outer'
                )
            except Exception as e:
                merged_df = pd.concat([merged_df, df], ignore_index=True, sort=False)
        
        return merged_df

    def _response_to_dataframe(self, response) -> pd.DataFrame:
        """Convert GA4 response to pandas DataFrame"""
        if not response.rows:
            return pd.DataFrame()
        
        # Extract headers
        dim_headers = [d.name for d in response.dimension_headers]
        metric_headers = [m.name for m in response.metric_headers]
        all_headers = dim_headers + metric_headers
        
        # Extract data
        data = []
        for row in response.rows:
            row_data = []
            
            # Add dimension values
            for dim_value in row.dimension_values:
                row_data.append(dim_value.value)
            
            # Add metric values
            for metric_value in row.metric_values:
                row_data.append(metric_value.value)
            
            data.append(row_data)
        
        return pd.DataFrame(data, columns=all_headers)

    def save_to_csv(self, df: pd.DataFrame, filepath: str):
        """Save DataFrame to CSV with error handling"""
        if df is None or len(df) == 0:
            print(f"⚠️  Skipping CSV save - no data to save")
            return
            
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"💾 Saved to {filepath}")
        print(f"   Rows: {len(df):,}, Columns: {len(df.columns)}")

    def print_sample(self, df: pd.DataFrame, rows: int = 5):
        """Print sample of DataFrame"""
        print(f"\n📊 Sample Data (showing {rows} rows):")
        
        if df is None:
            print("❌ DataFrame is None - no data returned")
            return
            
        if len(df) == 0:
            print("⚠️  DataFrame is empty - no data found")
            return
        
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns[:10])}{'...' if len(df.columns) > 10 else ''}")
        
        print("\nSample rows:")
        print(df.head(rows).to_string())

    def cleanup_chunks(self):
        """Clean up chunk files and progress tracking"""
        if os.path.exists(self.progress_dir):
            import shutil
            shutil.rmtree(self.progress_dir)
            print(f"🧹 Cleaned up chunk directory: {self.progress_dir}")

    def get_event_counts_summary(self, start_date: str, end_date: str, 
                            event_names: List[str] = None) -> pd.DataFrame:
        """
        Simple aggregation method: Get event counts by eventName for the given period.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            event_names: Optional list of specific events to filter for
        
        Returns:
            DataFrame with eventName and count aggregations
        """
        
        print(f"📊 Getting event counts summary for {start_date} to {end_date}")
        
        # Simple dimensions - just eventName for aggregation
        dimensions = [
            Dimension(name="eventName")
        ]
        
        # Simple count metrics
        metrics = [
            Metric(name="eventCount"),          # Total event occurrences
            Metric(name="activeUsers"),         # Users who triggered events
            Metric(name="sessions"),            # Sessions with events
            Metric(name="totalUsers")           # Total users
        ]
        
        # Set up event filter if specific events requested
        dimension_filter = None
        if event_names:
            dimension_filter = FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    in_list_filter=Filter.InListFilter(values=event_names)
                )
            )
            print(f"   🎯 Filtering for events: {event_names}")
        
        # Single request for event summary
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=dimension_filter,
            order_bys=[
                {"metric": {"metric_name": "eventCount"}, "desc": True}  # Order by event count desc
            ],
            limit=1000  # Should be plenty for event name summary
        )
        
        response = self.client.run_report(request)
        df = self._response_to_dataframe(response)
        
        if len(df) > 0:
            # Convert string numbers to integers for better display
            for col in ['eventCount', 'activeUsers', 'sessions', 'totalUsers']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
            print(f"   ✅ Found {len(df)} event types with {df['eventCount'].sum():,} total events")
        else:
            print(f"   ⚠️  No events found for the specified period")
        
        return df


if __name__ == "__main__":
    
    # Configuration
    property_id = "320198532"
    key_path = "./cru-ga4-prod-1-63a3434e5a2a.json"
    start_date = "2023-03-01"
    end_date = "2023-07-16"  
    chunk_days = 10               # Chunk size in days (adjust based on data volume)
    limit_rows = 50000            # Reduced from max. 100K for better reliability
    
    # Specific events we're interested in
    # event_names = ['videostarts', 'session_start', 'first_visit']
    event_names = []
    
    # Initialize client
    client = GA4Client(property_id, key_path)

    # Get event counts summary for the specified period
    event_summary = client.get_event_counts_summary("2023-03-01", "2023-07-16")
    print(event_summary)
    
    print("🚀 GA4 Data Extraction - Date Range Chunking Strategy")
    print("=" * 75)
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"🎯 Events: {event_names}")
    print(f"📦 Chunk Size: {chunk_days} days")
    print(f"🔄 Rows per request: {limit_rows:,}")
    print("=" * 75)
    
    try:
        # Extract data using chunking strategy
        comprehensive_df = client.get_report(
            start_date=start_date,
            end_date=end_date,
            event_names=event_names,
            chunk_days=chunk_days,
            limit_rows=limit_rows,
            save_chunks=True,      # Save individual chunks for recovery
            resume_from_chunks=True  # Resume from existing chunks
        )
        
        # Display results
        client.print_sample(comprehensive_df)
        
        # Save final combined file
        client.save_to_csv(comprehensive_df, "data/ga4_comprehensive_chunked_final.csv")
        
    except Exception as e:
        print(f"❌ Error in chunked extraction: {e}")
        print(f"💡 Check individual chunk files in '{client.progress_dir}' directory for partial results")
    
    print("\n✅ Chunked extraction completed!")
    print("\n📁 Files generated:")
    print("   • Individual chunk files in 'ga4_chunks/' directory")
    print("   • ga4_comprehensive_chunked_final.csv - Combined final dataset")

    # Optional: Clean up chunk files after successful completion
    # client.cleanup_chunks()