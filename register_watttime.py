import requests

register_url = "https://api.watttime.org/register"

payload = {
    "username": "FinnCaseUtil",
    "password": "Doughnut12!",
    "email": "finnrcase@gmail.com",
    "org": "Util"
}

response = requests.post(register_url, json=payload, timeout=30)

print("Status code:", response.status_code)
print("Response text:", response.text)
