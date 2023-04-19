from os import environ as env
import requests
import urllib3
from typing import Union
from requests import Response
import json
import time
from dotenv import load_dotenv

load_dotenv()


SERVER_URL = env["SERVER_URL"]
TOKEN = env["TOKEN"]
WORKSPACE_ID = env["WORKSPACE_ID"]

urllib3.disable_warnings()


def load_data(file_name):
    try:
        with open(file_name, "r") as file:
            data = json.load(file)
    except FileNotFoundError:
        # If file does not exist, initialize with an empty list
        data = []
        with open(file_name, "w") as file:
            json.dump(data, file)
    return data


def save_data(data, file_name):
    with open(file_name, "w") as file:
        json.dump(data, file, indent=4)


def graphql_request(
    operation: str, variables: dict = None, headers: dict = None, special: bool = False
) -> Union[dict, Response]:
    """Send an operation to a graphql server.
    Operation is a string of the graphQL operation
    Variables are the data to the query.
    headers is the request headers
    special if true, will return just the response object
    """
    json_payload = {"query": operation}
    if not headers:
        headers = {"Authorization": TOKEN}

    if variables:
        json_payload["variables"] = variables

    response: Response = requests.post(
        f"https://houston.{SERVER_URL}/v1", json=json_payload, headers=headers
    )
    # if errors key present in json response raise
    graphql_result = response.json()
    if "errors" in graphql_result:
        raise Exception(graphql_result["errors"])
    if special:
        return response
    if graphql_result["data"]:
        return graphql_result["data"]


def create_deployment(label: str, executor: str, airflowVersion: str):
    mutation = """
           mutation CreateDeployment(
            $workspaceUuid: Uuid!
            $type: String!
            $label: String!
            $releaseName: String
            $namespace: String
            $description: String
            $airflowVersion: String
            $runtimeVersion: String
            $executor: ExecutorType
            $workers: Workers
            $webserver: Webserver
            $scheduler: Scheduler
            $triggerer: Triggerer
            $dagDeployment: DagDeployment
            $properties: JSON) {
            createDeployment(workspaceUuid: $workspaceUuid
              type: $type, label: $label
              releaseName: $releaseName
              namespace: $namespace
              description: $description
              airflowVersion: $airflowVersion
              runtimeVersion: $runtimeVersion
              executor: $executor
              workers: $workers
              webserver: $webserver
              scheduler: $scheduler
              triggerer: $triggerer
              dagDeployment: $dagDeployment
              properties: $properties) {
               id
               label
               releaseName
               airflowVersion
               executor
               }
            }
    """
    deployment_args = {
        "workspaceUuid": WORKSPACE_ID,
        "type": "airflow",
        "label": label,
        "releaseName": "",
        "namespace": "",
        "description": "CreateApi_k8s_image_01",
        "airflowVersion": airflowVersion,
        "runtimeVersion": "",
        "executor": executor,
        "dagDeployment": {"type": "image"},
        "properties": {},
    }
    return graphql_request(operation=mutation, variables=deployment_args)[
        "createDeployment"
    ]


def test():
    for x in range(32):
        try:
            import random

            file_name = "deploy.json"
            executor_types = ["KubernetesExecutor", "CeleryExecutor"]

            random_executor = random.choice(executor_types)
            executor = str(random_executor)
            airflow = ["2.4.3"]
            random_airflow = random.choice(airflow)
            airflowVersion = str(random_airflow)
            label = f"Deployment-{x:0>3}"
            data = load_data(file_name)
            for deployment in data:
                if label in deployment:
                    print(f"Deployment with name '{label}' already exists.")
                    break
            else:
                print(
                    f"Deployment with name  '{label}' does not exist in the loaded data. Creating new deployment..."
                )
                result = create_deployment(label, executor, airflowVersion)
                print(f'{result["label"]} just created.')
                time.sleep(60)
                # Add a new user
                deployments_dict = {
                    label: result["id"],
                    "releaseName": result["releaseName"],
                    "executor": executor,
                    "airflowVersion": airflowVersion,
                    "tag": "",
                }
                loaded_users = load_data(file_name)
                loaded_users.append(deployments_dict)
                save_data(loaded_users, file_name)
                print("Finished sleeping. Next...")
        except Exception as e:
            print("Error in creating deployment", e)
            exit(1)


if __name__ == "__main__":
    test()
