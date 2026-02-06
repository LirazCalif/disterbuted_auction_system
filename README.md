# Distributed Auction System with Multi Paxos

A fault-tolerant, linearizable distributed auction system built with **Go**, **Angular**, and **Docker**.

This project implements the Multi-Paxos consensus algorithm to ensure data consistency across a cluster of nodes, featuring dynamic membership, leader election via etcd, and a real-time observability dashboard.

Before running the system, ensure you have the following installed:

* **Docker Desktop** 
* **Node.js & npm** for the Frontend
* **Python 3.x** for running system tests
* **PowerShell** for the automation script

#before running
Make sure the Docker Desktop is up before running.

You should run the following command from the project root directory.

# building the cluster docker alone
Building Docker containers are built and running:
    ```powershell
        docker-compose -f src/docker/docker-compose.yaml up --build -d
        ```

# take down the containers
taking down Docker containers:
    ```powershell
        docker-compose -f src/docker/docker-compose.yaml down
        ```


# for running the test
running the tests after the containers are up:
    ```powershell
        python src/etc/test.py
        ```


# for running docker and the front
This script will open the logs in a new window, start the Angular frontend, and automatically open your browser when ready.
    ```powershell -ExecutionPolicy Bypass -File .\src\etc\run_demo.ps1
    ```
