import requests

url = "http://127.0.0.1:5000/get-ads"

headers = {
    'TOP': '5'
}

response = requests.request("GET", url, headers=headers)

print(response.text)
