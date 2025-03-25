import requests
import google.auth
import google.auth.transport.requests

date = '2025-03-19'
url = f'https://ecommerce-data-generator-api-crvrynnlfq-nw.a.run.app/products?date_updated={date}'

credentials, project_id = google.auth.default()
credentials.refresh(google.auth.transport.requests.Request())
identity_token = credentials.id_token
auth_header = {"Authorization": "Bearer " + identity_token}

response = requests.get(url,headers=auth_header)
parsed_response = response.json()
print(parsed_response)

