import random
import pandas as pd
from datetime import datetime, timedelta

# Load the CSV file into memory
customer_survey_data = pd.read_csv('./Customer_support_data.csv')

# Define the mapping of old column names to new column names
column_name_mapping = {
    'Unique id': 'unique_id',
    'Sub-category': 'sub_category',
    'Issue_reported at': 'issue_reported_at',
    'Order_id': 'order_id',
    'Agent_name': 'agent_name',
    'CSAT Score': 'csat_score'
    # Add more mappings as needed
}

# Rename columns using the mapping
customer_survey_data.rename(columns=column_name_mapping, inplace=True)

def get_simulated_data_event() -> dict:

    # Generate a random index to select a row
    random_index = random.randint(0, len(customer_survey_data) - 1)
    # Get the row at the random index
    random_row = customer_survey_data.iloc[random_index]
    # Convert the random row into a dictionary
    random_row_dict = random_row.to_dict()
    # Extract values of specific columns
    columns_to_send = [
    'unique_id', 
    'channel_name', 
    'category', 
    'sub_category', 
    'order_id', 
    'issue_reported_at', 
    'issue_responded', 
    'agent_name',
    'csat_score'
    ]
    simulated_message = {col: random_row_dict[col] for col in columns_to_send}
    
    # Parse the dates from the dictionary
    reported_at = datetime.strptime(simulated_message['issue_reported_at'], '%d/%m/%Y %H:%M')
    responded_at = datetime.strptime(simulated_message['issue_responded'], '%d/%m/%Y %H:%M')
    
    # Get the current date and time
    current_datetime = datetime.now()
    
    # Replace the value of 'issue_responded' with current date and time
    simulated_message['issue_responded'] = current_datetime.strftime('%d/%m/%Y %H:%M')
    
    # Calculate the difference between old 'issue_responded' and old 'Issue_reported_at'
    time_difference = responded_at - reported_at
    
    # Subtract this difference from the new 'issue_responded' to get the new 'Issue_reported_at'
    new_reported_at = current_datetime - time_difference
    simulated_message['issue_reported_at'] = new_reported_at.strftime('%d/%m/%Y %H:%M')
    
    # Simulate sending the values to event streaming
    return simulated_message

