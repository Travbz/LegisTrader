import re
import pandas as pd
import requests
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import os

class LegislatorsProcessor:
    def __init__(self, existing_members_url, db_params):
    def __init__(self, existing_members_url):
        self.existing_members_url = existing_members_url
        self.db_params = self.load_secrets()

    def load_secrets(self):
        db_params = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT")
        }

        if all(value is not None for value in db_params.values()):
            return db_params
        else:
            raise ValueError("Missing one or more database parameters in Kubernetes Secrets.")


    def fetch_legislators_data(self):
        existing_member_req = requests.get(self.existing_members_url)
        existing_members = existing_member_req.json()
        return existing_members

    def process_legislators(self, data):
        regex = re.compile('[^a-zA-Z\s]')
        senate_dicts = []

        for member in data:
            terms = member.get('terms', [])
            if not terms:
                continue

            terms.sort(key=lambda x: x.get('end', ''), reverse=True)
            latest_term = terms[0]

            senate_dict = {
                'fullname': regex.sub('', member['name'].get('official_full', f"{member['name']['first']} {member['name']['last']}")),
                'firstname': regex.sub('', member['name']['first']),
                'lastname': regex.sub('', member['name']['last']),
                'id': member['id']['bioguide'],
                'party': latest_term.get('party'),
                'state': latest_term.get('state'),
                'position': latest_term.get('type'),
                'start_term': latest_term.get('start'),
                'end_term': latest_term.get('end')
            }

            senate_dicts.append(senate_dict)

        return pd.DataFrame(senate_dicts)

    def create_legislators_table(self, conn):
        with conn.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS legislators (
                    fullname TEXT,
                    firstname TEXT,
                    lastname TEXT,
                    id TEXT,
                    party TEXT,
                    state TEXT,
                    position TEXT,
                    start_term TEXT,
                    end_term TEXT
                )
            ''')

    def fetch_existing_legislators(self, conn):
        query = "SELECT * FROM legislators"
        existing_legislators = pd.read_sql_query(query, conn)
        return existing_legislators

    def update_database(self, conn, new_data):
        existing_data = self.fetch_existing_legislators(conn)
        new_rows = pd.concat([existing_data, new_data]).drop_duplicates(keep=False)

        if not new_rows.empty:
            with conn.cursor() as cursor:
                insert_query = sql.SQL('INSERT INTO legislators VALUES {}').format(
                    sql.SQL(',').join(map(sql.Literal, new_rows.to_records(index=False)))
                )
                cursor.execute(insert_query)

    def run_daily_update(self):
        # PostgreSQL Database Connection
        conn = psycopg2.connect(**self.db_params)
        self.create_legislators_table(conn)

        # Fetch existing legislators data from the database
        existing_data = self.fetch_existing_legislators(conn)

        # Fetch the latest data from the API
        latest_data = self.process_legislators(self.fetch_legislators_data())

        # Identify new rows and update the database
        self.update_database(conn, latest_data)

        # Commit changes and close the connection
        conn.commit()
        conn.close()

if __name__ == "__main__":
    # Define the existing members URL and PostgreSQL connection parameters
    existing_members_url = "your_existing_members_url"
    db_params = {
        "dbname": "officiallist",
        "user": "travbz",
        "password": "lol-lmao-69-420-24-7",
        "host": "your_postgresql_service",
        "port": "your_postgresql_port"
    }

    # Instantiate the processor
    processor = LegislatorsProcessor(existing_members_url, db_params)

    # Run the daily update
    processor.run_daily_update()
