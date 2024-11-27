# daily_metrics_calculation
This project is designed to calculate and analyze key performance metrics for a digital platform (e.g., a streaming service or cinema platform). It fetches data from PostgreSQL databases, processes it, and generates comprehensive insights that can be used for reporting or automation, such as sending daily updates to a Telegram bot.
# Key Features

### 1. Data Retrieval:
- Fetches data from PostgreSQL database tables:
`central_cinema_user_payments`: User payments data.
`central_cinema_partner_commission`: Commission details for partners.
`central_cinema_user_activity`: User activity and session data.
`central_cinema_title`: Metadata about titles.

### 2. Metrics Calculation:
#### - Payment Metrics:
- Number of trials and regular payments.
- Month-over-month (MoM) comparisons with visual indicators.
#### - Conversion Rates:
- Conversion from trials to first payments.
#### -Cash-In Analysis:
- Gross cash-in calculation and MoM comparison.
#### - Customer Acquisition Cost (CAC):
- Analysis of average partner commissions.
#### - Session Metrics:
- Average session duration.
- Completion rates of content.
#### - Unique Viewers:
- Number of unique viewers per day.
#### - Repeat Viewers:
- Number of repeat viewers over a rolling weekly period.
#### - Average Sessions Per Viewer:
- Average number of sessions per user over the last week.

### 3. Data Aggregation and Formatting:
Metrics are aggregated and formatted into text messages for further integration (e.g., sending via Telegram).
### 4. Extensibility:
Modular functions to calculate custom metrics or support new data sources.

## Technologies Used
- Python Libraries:
`pandas`: For data manipulation and analysis.
`psycopg2`: For PostgreSQL database connectivity.
`datetime` and `dateutil`: For date operations.
- Database:
PostgreSQL
- Data Formats:
Raw SQL queries are used to fetch data from the database.
All calculations are done in-memory with pandas.

## Project Structure

daily-metrics/
- `metrics_calculator.py`        # Core metrics calculation functions
- `main.py`                      # Main script to generate all metrics
- `requirements.txt`             # Python package dependencies
- `secrets.yaml.example`         # Example configuration file for database connection
- `README.md`                    # Project documentation


