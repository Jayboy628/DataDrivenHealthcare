import sys
import logging
import unittest
from unittest.mock import patch, MagicMock

# Correcting the import paths
from scripts.load_data_to_snowflake import airflow_task_load_data
from scripts.s3_utils import fetch_files_from_s3

logging.basicConfig(level=logging.INFO)
print(sys.path)

class TestAirflowTaskLoadData(unittest.TestCase):

    @patch('logging.info')
    @patch('logging.error')
    @patch('logging.warning')
    @patch('scripts.load_data_to_snowflake.load_data_into_snowflake')  # Corrected patch path
    @patch('scripts.s3_utils.fetch_files_from_s3')  # Corrected patch path
    def test_matching_files(self, mock_fetch_files, mock_load_data, mock_warning, mock_error, mock_info):
        # The rest of the code remains unchanged
        ...

    @patch('logging.info')
    @patch('logging.error')
    @patch('logging.warning')
    @patch('scripts.s3_utils.fetch_files_from_s3')  # Corrected patch path
    def test_no_matching_files(self, mock_fetch_files, mock_warning, mock_error, mock_info):
        """
        Test that function handles no matching files correctly and logs a warning.
        """
        # Mock the return value of fetch_files_from_s3 to simulate no files in S3
        mock_fetch_files.return_value = []

        mock_context = {'task_instance': MagicMock()}

        # Call the function you want to test
        print("Calling airflow_task_load_data...") 
        airflow_task_load_data(context=mock_context)

        # Assert that the function handled no matching files correctly
        mock_fetch_files.assert_called_once_with()
        mock_warning.assert_called_once()  # If possible, assert the specific warning message

        # If the task_instance in the context is modified, add an assertion to check that
        # Example: 
        # mock_context['task_instance'].xcom_push.assert_not_called()

if __name__ == '__main__':
    unittest.main()
