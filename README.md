# OpenPurview Local

A free, local alternative to Microsoft Purview for Azure and Microsoft 365 governance.

## Prerequisites

1.  **Python 3.10+**
2.  **Azure CLI**: Install and run `az login` to authenticate.

## Installation

1.  Create a virtual environment:
    ```bash
    python -m venv venv
    .\venv\Scripts\Activate  # Windows
    # source venv/bin/activate # Linux/Mac
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

Run the Streamlit dashboard:

```bash
streamlit run src/ui/app.py
```

## Features

-   **Azure Collector**: Fetches subscriptions and resources using Azure Resource Graph.
-   **Local Frontend**: Streamlit-based dashboard.
-   **Secure**: Runs entirely on localhost.
