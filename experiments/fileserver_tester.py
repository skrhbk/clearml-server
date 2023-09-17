import requests

fileserver_url = "http://localhost:8081"


def download(file_path):
    resp = requests.get(f"{fileserver_url}/{file_path}")
    print(f"{resp.status_code}, {resp.content}")


def upload(file_paths):
    files = {}
    for file_path in file_paths:
        # with open(file_path, 'rb') as file:
        files[file_path] = (file_path, open(file_path, 'rb'))
    resp = requests.post(fileserver_url, files=files)
    print(f"{resp.status_code}, {resp.content}")


def delete(file_paths):
    for file_path in file_paths:
        resp = requests.delete(f"{fileserver_url}/{file_path}")
        print(f"{resp.status_code}, {resp.content}")


def delete_many(file_paths):
    body = {"files": file_paths}
    resp = requests.post(f"{fileserver_url}/delete_many", json=body)
    print(f"{resp.status_code}, {resp.content}")


if __name__ == "__main__":

    file_paths = ["fileserver.py", "config/basic.py"]
    upload(file_paths)
    # download(file_paths[0])
    # delete(file_paths)
    delete_many(file_paths)

    # print(f"{resp.status_code}, {resp.content}")
