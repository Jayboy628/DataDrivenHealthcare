import unittest
from unittest.mock import patch

import sys
sys.path.append('/opt/airflow/dags')
from main_dag import airflow_task_load_data

class TestAirflowTaskLoadData(unittest.TestCase):

    @patch('main_dag.load_data_into_snowflake')  # Mock the load_data_into_snowflake function
    @patch('main_dag.fetch_files_from_s3')  # Mock the fetch_files_from_s3 function
    def test_matching_files(self, mock_fetch_files, mock_load_data):
        # Mock the return value of fetch_files_from_s3 to simulate S3 file paths
        mock_fetch_files.return_value = [
            'raw_files/users_20230921154714-1.csv',
            'raw_files/orders_20230901165789-1.csv'
        ]

        # Call the function you want to test
        airflow_task_load_data()

        # Assert that the function correctly matched the files
        self.assertEqual(mock_fetch_files.call_count, 1)  # Ensure fetch_files_from_s3 was called once
        self.assertEqual(mock_load_data.call_count, 2)  # Ensure load_data_into_snowflake was called twice

    @patch('main_dag.fetch_files_from_s3')  # Mock the fetch_files_from_s3 function
    def test_no_matching_files(self, mock_fetch_files):
        # Mock the return value of fetch_files_from_s3 to simulate no files in S3
        mock_fetch_files.return_value = []

        # Call the function you want to test
        airflow_task_load_data()

        # Assert that the function handled no matching files correctly
        self.assertEqual(mock_fetch_files.call_count, 1)  # Ensure fetch_files_from_s3 was called once
        # Add more assertions based on your specific handling of no files

if __name__ == '__main__':
    unittest.main()
