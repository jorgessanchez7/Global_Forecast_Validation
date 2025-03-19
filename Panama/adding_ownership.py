import getpass
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

stations_df = pd.read_csv('/Users/grad/Library/CloudStorage/Box-Box/Post_Doc/GEOGLOWS_Applications/Central_America/Panama_Selected_Stations.csv', index_col=2)

uids = stations_df.index.to_list()

emails = [
    "allyblackhurst@gmail.com",
    "haidee.armstrong@gmail.com",
    "mark.payne749@gmail.com"
]

# Encabezados (si necesitas autenticación, agrega el token)
headers = {
    "accept": "application/json",
    "Content-Type": "application/json"
}

username = input("Enter your email: ")
password = getpass.getpass("Enter your password: ")

for uid in uids:

    print(uid)

    url = "https://imhpa-hydroserver.geoglows.org/api/data/things/{}/ownership".format(uid)

    for email in emails:

        data = {
            "email": email,
            "makeOwner": True,
            "removeOwner": False,
            "transferPrimary": False
        }

        # Realizar la solicitud PATCH
        #response = requests.patch(url, headers=headers, json=data)
        response = requests.patch(url, headers=headers, json=data, auth=HTTPBasicAuth(username, password))

        # Print results
        print(f"Request to {url} for {email}:")
        print("Status Code:", response.status_code)
        try:
            print("Response:", response.json())
        except Exception:
            print("Response could not be parsed as JSON.")
        print("-" * 50)

    
    