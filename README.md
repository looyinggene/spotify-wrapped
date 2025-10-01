# 🎧 Spotify Data Pipeline & Wrapped Story Generator

[![GitHub Repository](https://img.shields.io/badge/GitHub-looyinggene%2Fspotify--wrapped-1DB954?style=for-the-badge)](https://github.com/looyinggene/spotify-wrapped)

## 🎯 Project Overview

This is a full-stack data engineering and visualization project that mimics the popular **Spotify Wrapped** feature. It demonstrates expertise in building a containerized, robust **Data Pipeline (ETL)**, scheduling tasks with **Apache Airflow**, serving data via an **API**, and presenting the results in a dynamic **React frontend**.

The project transforms raw Spotify user data into structured insights using a best-practice Bronze, Silver, and Gold data layering approach.

### Key Architectural Highlights
* **Data Orchestration:** Three distinct **Apache Airflow DAGs** manage the ETL flow, ensuring reliable data extraction, transformation, and loading into a Gold-layer database view.
* **Containerization:** The entire stack—including Airflow, the Python API, the Frontend, and the Postgres Database—is orchestrated using **Docker Compose** for immediate, portable local deployment.
* **Full-Stack Data Delivery:** A dedicated **Python API** serves the cleaned, transformed Gold-layer data to the **React Frontend**, which renders the data into the animated, story-based user experience.

---

## 🛠️ Technical Stack

| Category | Technologies | Focus Areas |
| :--- | :--- | :--- |
| **Data Engineering** | **Apache Airflow**, Python, ETL/ELT | DAG authoring, task dependency management, scheduling. |
| **Deployment/Infra** | **Docker**, **Docker Compose** | Containerization, service networking, environment setup. |
| **Backend/API** | Python (Flask/FastAPI structure), RESTful API Design | Data serving, final transformation logic. |
| **Frontend** | React, JavaScript, CSS3 | Data visualization, complex state management, custom UX/UI. |
| **Data Storage** | PostgreSQL | Database schema design, data layering (Bronze/Silver/Gold). |

---

## 💡 Technical Deep Dive (Key Selling Points)

This section highlights the specific technical problems solved, showcasing expertise beyond basic development.

### 1. Robust ETL Design (Bronze/Silver/Gold Layers)
The project strictly follows a layered Data Lakehouse methodology:
* **Bronze:** Raw data is extracted directly from the Spotify Web API (`etl/extract.py`).
* **Silver:** Data is cleaned, standardized, and enriched (e.g., fetching artist images and audio previews).
* **Gold:** Final aggregated and structured views are created for direct consumption by the API, providing calculated metrics like "Top Artists" and "Total Minutes Listened."

### 2. Airflow DAG Logic
Multiple DAGs (`dags/`) are utilized to manage different data flows:
* The primary DAG handles daily incremental data refreshes.
* Separate DAGs manage backfilling of historical data and specific extraction logic for saved songs versus recent listening history, guaranteeing comprehensive data coverage.

### 3. Frontend Audio Synchronization
The React application's core complexity lies in the timed story transitions:
* It implements custom logic to manage a single audio player, ensuring **seamless audio crossfading** between songs and synchronization of audio tracks with timed slide advances. This required careful state management and timing control to replicate a high-fidelity native application experience.

---

## 📸 Screenshots

To view the application and Airflow pipeline in action, see the following screenshots:

### 1. Airflow Pipeline Status
*Show a screenshot of your **Airflow UI** (DAGs view or a successful graph view) demonstrating the complexity of your ETL workflow.*

> **[Screenshot of Airflow DAG running successfully (showing task dependencies)]**
>
> *This screenshot shows the scheduled execution of the core ETL pipeline, validating the successful completion of data extraction, multiple transformation steps, and the final load to the Gold layer.*

### 2. Frontend Visualization
*Show a screenshot of the actual React application displaying a key metric slide (e.g., Top Songs, Top Genres, or Total Minutes).*

> **[Screenshot of the React App showing a 'Top Songs' slide]**
>
> *The resulting data powers a dynamic, modern React frontend. This view illustrates the complex visualization of ranked data within the story format.*

---

## 🚀 Local Setup and Installation

The entire environment is built using Docker Compose, allowing for a single-command setup.

### Prerequisites
* **Docker** and **Docker Compose** installed.
* A Spotify Developer Account is required to set up the necessary API credentials.

### 1. Environment Configuration

Create a file named **`.env`** in the project root (`spotify-wrapped/`) and include your secret credentials:

```bash
# .env file content (Do NOT commit this file to Git!)

# Airflow/Postgres setup variables
AIRFLOW_UID=50000 
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Spotify API Keys
SPOTIFY_CLIENT_ID="your_client_id_here"
SPOTIFY_CLIENT_SECRET="your_client_secret_here"
```

### 2. Run the Stack

Navigate to the project root and run the build and up command:
```bash
docker-compose up --build -d
```

### 3. Access Services

Wait a few minutes for all containers to initialize. The services will be accessible here:

| Service | Access URL | Default Credentials |
| :--- | :--- | :--- |
| **Airflow UI** | `http://localhost:8080` | `airflow` / `airflow` |
| **Frontend App** | `http://localhost:3000` | N/A |
| **API Backend** | `http://localhost:5000` | N/A |

### 4. Run the ETL Pipeline

1.  Navigate to the **Airflow UI** at `http://localhost:8080`.
2.  Log in using the default credentials: **`airflow` / `airflow`**.
3.  Find the relevant ETL DAG (e.g., `spotify_etl_pipeline`).
4.  **Unpause** the DAG and trigger a **manual run** to execute the data pipeline and populate your database.

Once the pipeline successfully completes, the **Frontend** at `http://localhost:3000` will begin displaying the processed data.
