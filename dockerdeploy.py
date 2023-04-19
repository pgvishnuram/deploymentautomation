import json
from os import environ as env
import docker
client = docker.from_env()
from dotenv import load_dotenv
load_dotenv()


SERVER_URL=env['SERVER_URL']
docker_tag="deploy-1"
filename="deploy.json"

def load_data(file_name):
    try:
        with open(file_name, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        # If file does not exist, initialize with an empty list
        data = []
        with open(file_name, 'w') as file:
            json.dump(data, file)
    return data

def save_data(data, file_name):
    with open(file_name, 'w') as file:
        json.dump(data, file, indent=4)

data = load_data('deploy.json')


for deployment in data:
    if "tag" in deployment and deployment["tag"] == docker_tag :
        print(f'Key "tag" exists in deployment {deployment["releaseName"]} in the loaded data.')
    else:
       print(deployment)
       RELEASE_NAME = deployment["releaseName"]
       DEPLOYMENT_URL=f"registry.{SERVER_URL}/{RELEASE_NAME}/airflow:{docker_tag}"
       print(f"Build to release {RELEASE_NAME} with URL {DEPLOYMENT_URL} started")
       client.images.build(path='./qa-scenario-dags', tag=DEPLOYMENT_URL,buildargs={"platform": "linux/amd64"})
       client.api.tag(DEPLOYMENT_URL,DEPLOYMENT_URL)
       client.images.push(DEPLOYMENT_URL)
       print(f"Image Pushed to release {RELEASE_NAME} with URL {DEPLOYMENT_URL} completed")
       deployment["tag"] = docker_tag

save_data(data, filename)