# freshcart-pipelines

Data Engineering & Pipeline Orchestration: **From Git to Gold** (Version control, ELT, ML pipelines and event streaming, end to end).

## Project Structure
```text
freshcart-pipelines/
├── README.md                          # Project documentation and architectural overview
├── DEVELOPMENT_TRACKER.md             # Milestone tracking, commit history, and test records
├── .gitignore                         # Build and artifact exclusions
├── requirements.txt                   # Project dependencies
├── elt/                               # FreshCart Snowflake ELT pipeline (Part B)
├── iris_project/                      # Mage ML pipeline (Part C)
│   ├── __init__.py
│   └── transformers/
│       ├── __init__.py
│       └── prepare_data.py            # Data preparation block with 80/20 train/test split
├── stream/                            # RabbitMQ streaming dispatcher and drivers (Part D)
├── tests/                             # Pytest test suite
│   ├── __init__.py
│   └── test_prepare_data.py           # Automated unit test for Mage data prep block
└── .github/
    └── workflows/
        └── ci.yml                     # GitHub Actions CI workflow (Python 3.12 + Pytest)
```

## Getting Started

### Local Setup
1. Clone this repository:
   ```bash
   git clone <YOUR_REPO_URL>
   cd freshcart-pipelines
   ```
2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the automated tests:
   ```bash
   pytest
   ```
