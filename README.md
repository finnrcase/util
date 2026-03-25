# Util - Compute Cost & Carbon Optimization Engine
> Built as part of a broader research and product initiative exploring cost and carbon optimization in AI and data center infrastructure.
### Demo
https://utilplatformv01.streamlit.app/

Website for download (in progress):

https://utilcompute.com/

## Overview
Util is a decision-support tool that optimizes when and where computational workloads should run to minimize electricity cost, carbon emissions, or a weighted combination of both.

The system models compute as a constrained optimization problem under real-world conditions by using the variables:
- time-varying electricity prices
- carbon intensity of the grid at any given time
- Workload time requirements
- deadline constraints
- Historical data

Util produces an optimal execution schedule along with quantified cost and emissions outcomes.

## Why this matters
As compute demand grows (AI training, data centers, research modeling, GPU workloads), energy consumption is becoming a primary cost and sustainability issue.

## Key Features
- Optimization Engine
  - Minimizes cost, carbon, or a weighted objective
  - Built using constrained optimization
- Workload Scheduling
  - Allocates compute across time intervals
  - Respects users deadlines and compute requirements
- Forecast Integration
  - Uses time-series electricity price and carbon intensity data along with machine learning to determine an estimate for future costs and emissions beyond public provided information
- Balanced Optimization Mode
  - Allows trade-offs between cost and emissions
- Detailed Outputs
  - Optimal schedule (run vs pause)
  - Cost and emissions breakdown
  - Savings vs baseline
  - CSV output
- Interactive Interface
- Workload Shifting (in progress)
  - Determines which data centers have access to the cleanest or cheapest energy and shift workloads accordingly

## System Architecture
User Input
  ↓
Location Mapping
  ↓
Energy Data Fetcher
  ↓
Forecast Engine
  ↓
Optimization Engine
  ↓
Scheduler
  ↓
Dashboard Output

## Tech Stack
Python
pandas / numpy — data processing
scipy.optimize — optimization engine
Streamlit — frontend interface
Altair — visualization

## How It Works
Util solves:
Minimize:
Cost = Σ(power × price × time)
 or
 Carbon = Σ(power × carbon_intensity × time)
Subject to:
required compute hours
deadline constraint
machine capacity

## Installation
1. Clone the repository
git clone https://github.com/yourusername/util.git
cd util

2. Install dependencies
pip install -r requirements.txt

3. Create a `.env` file in the project root
The app expects `.env` at the root of the running workspace, next to `app.py`.
If you run Util in a mounted or containerized workspace, that workspace must also contain its own `.env` file.
Do not commit `.env` to GitHub.

Template:
```env
WATTTIME_USERNAME=your_watttime_username
WATTTIME_PASSWORD=your_watttime_password
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_REGION=your_aws_region
S3_BUCKET_NAME=your_private_s3_bucket_name
```

For Streamlit Community Cloud, add AWS credentials in the app `Secrets` settings instead of committing `.env`.
Expected secret names:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_BUCKET_NAME`

Example Streamlit secrets format:
```toml
AWS_ACCESS_KEY_ID = "your_aws_access_key_id"
AWS_SECRET_ACCESS_KEY = "your_aws_secret_access_key"
AWS_REGION = "your_aws_region"
S3_BUCKET_NAME = "your_private_s3_bucket_name"
```

4. Run the app
streamlit run app.py

## Use Cases
- AI model training optimization
- GPU workload scheduling
- data center cost reduction
- carbon-aware compute planning
- research and academic compute workloads

## About
Util is being developed as an early-stage product exploring the intersection of:
infrastructure economics
energy systems
cost optimization
AI compute

## Author
Finn Case
 Economics @ UC Santa Barbara
 Focus: infrastructure, energy systems, and cost optimization in technology environments
