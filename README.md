## About Amazon Connect Contact Search Tool
This solution can be used to search contact with specified attributes in Amazon Connect.

### Installation

Clone the repo

```bash
git clone https://github.com/photosphere/contact-trace-record-search.git
```

cd into the project root folder

```bash
cd contact-trace-record-search
```

#### Create virtual environment

##### via python

Then you should create a virtual environment named .venv

```bash
python -m venv .venv
```

and activate the environment.

On Linux, or OsX 

```bash
source .venv/bin/activate
```
On Windows

```bash
source.bat
```

Then you should install the local requirements

```bash
pip install -r requirements.txt
```
### Build and run the Application Locally

```bash
streamlit run contact_trace_record_search.py
```

