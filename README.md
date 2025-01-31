# Data Engineer Challenge

## Setup
1. Clone the repository.
2. Subscribe for free in order to get your API Token on this [LINK](https://www.football-data.org/client/register)
2. Create the file **.env** in the root directory with:
    ```bash
    API_KEY=<your_api_token>
    ```
    
3. Create a virtual environment and install dependencies:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```
4. Run the pipeline:
    ```bash
    python src/main.py
    ```

## Requirements
- Python 3.10 or higher (THIS PROJECT WAS BUILT WITH PYTHON 3.13.1)
