import pandas as pd
import os
from datetime import datetime


class SymbolsUpdate(object):
    def __init__(self):
        self.database_file = 'database.csv'

    def load_new_data_from_file(self, file_path: str) -> pd.DataFrame:
        new_data = pd.read_csv(file_path)

        # Process the 'isin' column to derive 'country_id'
        new_data['country_id'] = new_data['isin'].apply(lambda x: x[:2])

        # Add current datetime as 'updatetime'
        new_data['updatetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Reshape the dataframe to have item/item_value pairs
        data_melted = pd.melt(new_data, id_vars=['updatetime', 'country_id', 'symbol', 'hold'],
                              value_vars=['isin', 'cusip'],
                              var_name='item',
                              value_name='item_value')

        return data_melted

    def save_new_data(self, input_data: pd.DataFrame):
        if os.path.exists(self.database_file):
            db_data = pd.read_csv(self.database_file)
            # Append new data
            combined_data = pd.concat([db_data, input_data])
        else:
            # If database doesn't exist, use the input data as database
            combined_data = input_data

        # Save the combined data back to the database
        combined_data.to_csv(self.database_file, index=False)

    def get_data_from_database(self) -> pd.DataFrame:
        db_data = pd.read_csv(self.database_file)

        # Sort by item_value and updatetime to get the most recent entries
        db_data.sort_values(by=['item_value', 'updatetime'], ascending=[True, False], inplace=True)

        # Drop duplicates to keep only the most recent entries for each item_value
        most_recent_data = db_data.drop_duplicates(subset=['item_value'], keep='first')

        return most_recent_data


if __name__ == "__main__":
    su = SymbolsUpdate()

    new_data = su.load_new_data_from_file('symbols_update_1.csv')
    su.save_new_data(new_data)

    new_data = su.load_new_data_from_file('symbols_update_2.csv')
    su.save_new_data(new_data)

    new_data = su.load_new_data_from_file('symbols_update_3.csv')
    su.save_new_data(new_data)

    # Fetch and print the most recent data from the database
    recent_data = su.get_data_from_database()
    print(recent_data)
