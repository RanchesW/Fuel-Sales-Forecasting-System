# Fuel Sales Forecasting System

An advanced time series forecasting system for gas station fuel sales prediction using Facebook Prophet, designed for retail fuel management and inventory optimization.

## Overview

Prophet-NP is a comprehensive forecasting solution that predicts fuel sales at gas stations to help manage inventory levels and prevent stockouts. The system integrates with SQL Server and Oracle databases to collect historical sales data and current tank volumes, then uses Facebook Prophet to generate accurate hourly fuel consumption forecasts.

## Key Features

- **Intelligent Date Selection**: Interactive date selection with weekday-aware forecasting horizons
- **Multi-Database Integration**: Seamless connection to SQL Server and Oracle databases
- **Advanced Prophet Implementation**: Custom seasonality patterns (weekly, daily, hourly)
- **Deadstock Prevention**: Predicts when fuel levels will reach critical thresholds
- **Robust Error Handling**: Comprehensive retry mechanisms and fallback connections
- **Parallel Processing**: Multi-threaded execution for improved performance
- **Dynamic Forecast Horizons**: Weekend-aware forecasting (71 hours for Friday, 47 hours for other days)

## System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CSV Data      │    │   SQL Server     │    │     Oracle      │
│ (Deadstock Info)│    │ (Sales History)  │    │ (Current Volumes│
│                 │    │                  │    │  & Station Info)│
└─────────┬───────┘    └────────┬─────────┘    └─────────┬───────┘
          │                     │                        │
          └─────────────────────┼────────────────────────┘
                                │
                    ┌───────────▼────────────┐
                    │    Prophet-NP Engine   │
                    │  • Data Processing     │
                    │  • Prophet Modeling    │
                    │  • Forecast Generation │
                    └───────────┬────────────┘
                                │
                    ┌───────────▼────────────┐
                    │     ord_forecast       │
                    │   (Forecast Results)   │
                    └────────────────────────┘
```

## Installation

### Prerequisites

- Python 3.7+
- SQL Server ODBC Driver 18
- Oracle Instant Client
- Access to SQL Server and Oracle databases

### Required Python Packages

```bash
pip install pandas prophet sqlalchemy pyodbc oracledb numpy
```

### Oracle Client Setup

1. Download Oracle Instant Client
2. Extract to a directory (e.g., `C:\instantclient_23_7`)
3. Update the path in the code:
```python
oracledb.init_oracle_client(lib_dir=r"C:\path\to\instantclient")
```

## Configuration

### Database Connections

Update the connection parameters in the code:

```python
# SQL Server Configuration
host = 'your_sql_server_host'
port = ''
database = 'your_database'
username = 'your_username'
password = 'your_password'

# Oracle Configuration
oracle_host = "your_oracle_host"
oracle_port = ""
oracle_service_name = "ORCL"
oracle_username = "your_username"
oracle_password = "your_password"
```

### CSV Data Format

Ensure your `deadstock_info_new.csv` contains:

```csv
Gas_Station_Name,City,Branch,ObjectCode,Tank_Number,Deadstock_Level,Deadstock_Volume,Max_Level,Max_Volume
Station A,Астана,Main,12345,Резервуар 1 АИ-92,10,500,180,9000
```

## Usage

### Basic Execution

```bash
python prophet_forecasting.py
```

### Interactive Date Selection

The system provides three options for forecast date selection:

1. **Current Date**: Use today's date
2. **Manual Input**: Enter specific date (YYYY-MM-DD format)  
3. **Recent Dates**: Choose from last 7 days

### Forecast Horizons

- **Friday**: 71-hour forecast (covers weekend)
- **Other Days**: 47-hour forecast (covers next 2 days)

## Core Components

### 1. Data Processing Pipeline

```python
# Extract tank information and fuel types
def extract_tank_and_fuel(tank_value):
    # Extracts tank number and fuel type from description
    # Supports: АИ-80, АИ-92, АИ-95, АИ-98, ДТ variants, СУГ
```

### 2. Prophet Model Configuration

```python
model = Prophet(
    seasonality_mode='additive',
    yearly_seasonality=False,
    weekly_seasonality=False,
    daily_seasonality=False,
    changepoint_prior_scale=0.05,
    seasonality_prior_scale=10.0
)

# Custom seasonality patterns
model.add_seasonality(name='weekly', period=7, fourier_order=3)
model.add_seasonality(name='daily', period=1, fourier_order=5)
model.add_seasonality(name='hourly', period=24, fourier_order=15)
```

### 3. Hybrid Forecasting Approach

```python
# Combines Prophet predictions with rolling averages
forecast['combined_forecast'] = 0.3 * forecast['rolling_mean'] + 0.7 * forecast['yhat']
```

## Data Sources

### SQL Server Tables
- **ord_salesbyhour**: Historical hourly fuel sales data
- **ord_forecast**: Output table for forecast results

### Oracle Tables
- **GS.AZS**: Gas station status information
- **BI.tigmeasurements**: Current tank volume measurements

### CSV Files
- **deadstock_info_new.csv**: Tank specifications and deadstock levels

## Output Format

The system generates forecasts with the following structure:

| Column | Description |
|--------|-------------|
| objectcode | Gas station identifier |
| gasnum | Fuel type code |
| tank | Tank number |
| date_time | Forecast timestamp |
| forecast_volume_sales | Predicted sales volume |
| date_time_deadstock | Predicted deadstock time |
| forecast_current_volume | Predicted remaining volume |

## Fuel Type Mapping

```python
fuel_mapping = {
    'АИ-80': '3300000000',
    'АИ-92': '3300000002', 
    'АИ-95': '3300000005',
    'АИ-98': '3300000008',
    'ДТ': '3300000010',
    'СУГ': '3400000000',
    # ... additional mappings
}
```

## Error Handling & Reliability

### Robust Connection Management
- Multiple retry attempts for database connections
- Alternative connection methods as fallbacks
- Automatic connection recreation on failures

### Data Validation
- Historical data sufficiency checks (minimum 24 hours)
- Forecast quality validation
- Missing data interpolation

### Logging
Comprehensive logging at all stages:
```python
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
```

## Performance Features

- **Parallel Processing**: Multi-threaded execution for multiple gas stations
- **Efficient Queries**: Optimized SQL queries with proper indexing
- **Memory Management**: Chunked data processing for large datasets
- **Connection Pooling**: Reusable database connections

## Monitoring & Diagnostics

The system provides detailed diagnostics:

```
📊 Диагностика для OBJECTCODE=12345, tank=1, fuel=АИ-92:
   Количество исторических записей: 2160
📈 Прогноз Prophet:
   Min yhat: 45.23
   Max yhat: 234.56
   Mean yhat: 123.45
```

## Deployment Considerations

### System Requirements
- Minimum 4GB RAM
- Network access to SQL Server and Oracle
- Windows environment (for ODBC drivers)

### Security
- Database credentials should be stored in environment variables
- Use encrypted connections where possible
- Implement proper access controls

## Troubleshooting

### Common Issues

1. **Oracle Client Not Found**
   - Ensure Oracle Instant Client is properly installed
   - Verify the path in `oracledb.init_oracle_client()`

2. **SQL Server Connection Timeout**
   - Check network connectivity
   - Verify ODBC driver installation
   - Adjust timeout parameters

3. **Insufficient Historical Data**
   - Ensure at least 24 hours of historical data
   - Check data quality and completeness

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with proper logging
4. Test with sample data
5. Submit pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For technical support or questions:
- Create an issue in the repository
- Check logs for detailed error information
- Verify database connectivity and data availability

---

**Note**: This system is designed for production use in fuel retail environments. Ensure proper testing and validation before deployment in critical systems.
