from datetime import datetime
import time
import httpx
import json
from flask import Flask

from firebase_admin import credentials, messaging
import firebase_admin
import supabase
import uuid

app = Flask(__name__)

# Initialize Firebase
cred = credentials.Certificate('D:\\supabase_mqtt\\ies-67563-firebase-adminsdk-6epw7-4ebad2bfc3.json')
firebase_admin.initialize_app(cred)

# Initialize Supabase
supabase_url = 'http://13.51.198.7:8000'
supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJhbm9uIiwKICAgICJpc3MiOiAic3VwYWJhc2UtZGVtbyIsCiAgICAiaWF0IjogMTY0MTc2OTIwMCwKICAgICJleHAiOiAxNzk5NTM1NjAwCn0.dc_X5iR_VP_qT0zsiyj_I_OZ2T9FtRU2BBNWN8Bu4GE'

supabase_client = supabase.Client(supabase_url, supabase_key)

notifications_table_name = 'notifications_table_name'


def send_firebase_notification(device_ids, notification_message):
    messages = []

    for device_id in device_ids:
        message = messaging.Message(
            notification=messaging.Notification(
                title='Threshold Notification',
                body=notification_message
            ),
            token=device_id
        )
        messages.append(message)

    response = messaging.send_all(messages)
    return response


def store_notification(device_id, payload, topic, sensor_id, customer_id, operator_id):
    threshold = 5.0
    message = "Resistance crossed Threshold level" if payload > threshold else "Threshold not crossed"

    id = str(uuid.uuid4())
    current_date = datetime.now().date().isoformat()
    current_time = datetime.now().time().isoformat()

    try:

        response = supabase_client.table(notifications_table_name).select('sequence').limit(1).execute()

        if isinstance(response, dict) and 'data' in response:
            data_list = response['data']
            max_sequence = max(data_list, key=lambda x: x.get('sequence', 0)).get('sequence', 0)
        else:
            print('Error fetching max sequence value. Setting sequence to 1.')
            max_sequence = 0

        # Increment the sequence for the new entry
        new_sequence = max_sequence + 1

        data = {
            'id': id,
            'device_id': device_id,
            'payload': payload,
            'topic': topic,
            'date_time': current_date,
            'time': current_time,
            'sensor_id': sensor_id,
            'message': message,
            'customer_id': customer_id,
            'operator_id': operator_id,
            'flag': 1,
            'sequence': new_sequence

        }
        print("Inserting data with new sequence:", data)

        # Insert the data with the new sequence value
        response = supabase_client.table(notifications_table_name).insert([data]).execute()

        if isinstance(response, dict) and 'status_code' in response and response['status_code'] == 201:
            print("Data inserted successfully!")
        else:
            error_response = response.json() if hasattr(response, 'json') else str(response)
            print(f'Error inserting data. Response: {error_response}')

    except Exception as e:
        print('Error inserting data:', str(e))


def update_flag(notification_id, flag_value):
    try:
        response = supabase_client.table(notifications_table_name).update(
            {'flag': flag_value}).eq('id', notification_id).execute()
        print(response)
        if isinstance(response, dict) and 'status_code' in response and response['status_code'] == 200:
            print("Flag updated successfully!")
        else:
            error_response = response.json() if hasattr(response, 'json') else str(response)
            print(f'Error updating flag. Response: {error_response}')

    except Exception as e:
        print('Error updating flag:', str(e))


def get_device_ids():
    try:
        headers = {
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json',
            'apikey': supabase_key
        }

        response = httpx.get(f'{supabase_url}/rest/v1/rpc/get_register_info', headers=headers)

        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, dict) and 'data' in data:
                device_data = data['data']
                if isinstance(device_data, list) and len(device_data) > 0:
                    return [device.get('device_id') for device in device_data]
                else:
                    print('Error: No device_id found in the API response')
                    return []
            else:
                print('Error: Invalid API response format')
                return []
        else:
            print('Error fetching device_ids:', response.text)
            return []

    except Exception as e:
        print('Error:', e)
        return []


def fetch_live_data():
    try:
        headers = {
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json',
            'apikey': supabase_key
        }

        response = httpx.get(f'{supabase_url}/rest/v1/rpc/get_all_mqtt_data1', headers=headers)

        if response.status_code == 200:
            live_data = response.json()
            return live_data
        else:
            print('Error fetching live data:', response.text)
            return []

    except Exception as e:
        print('Error:', e)
        return []

@app.route('/')
def home():
    return "Flask MQTT Data Processing"

@app.route('/test_process', methods=['POST'])
def test_process():
    device_ids = get_device_ids()

    if not device_ids:
        return "Error fetching device IDs", 400

    processed_notification_ids = set()

    try:
        while True:
            live_data = fetch_live_data()

            if not live_data:
                print("No live data available")
                time.sleep(10)
                continue

            for data in live_data:
                notification_id = data.get('id')
                topic = data.get('topic')
                payload = float(data.get('payload', 0))
                sensor_id = data.get('sensor_id')
                customer_id = data.get('customer_id')
                operator_id = data.get('operator_id')

                print(f"Received data - Topic: {topic}, Payload: {payload}, Sensor ID: {sensor_id}, Customer ID: {customer_id}")

                # Check if the notification has already been processed
                if notification_id in processed_notification_ids:
                    continue

                # Process the notification
                if "R" in topic and payload > 5:
                    message = f"Topic: {topic}, Payload: {payload}"
                    send_firebase_notification(device_ids, message)
                    store_notification(device_ids, payload, topic, sensor_id, customer_id, operator_id)
                    processed_notification_ids.add(notification_id)

            time.sleep(5)

    except KeyboardInterrupt:
        print("Process interrupted")

    return json.dumps({"status": "Notifications continuously monitored"})

if __name__ == "__main__":
    app.run(debug=True)