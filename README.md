# Gold-Silver-ETF-and-USD-to-INR-tracker-for-MAC-OS
This tool tracks Gold and Silver ETFs and USD to INR using API and notifies

# Instructions 

1. Clone the repo

```bash
git clone https://github.com/Ankit-Gupta/Gold-Silver-ETF-and-USD-to-INR-tracker-for-MAC-OS.git
```

2. Install dependencies

```bash
pip install -r requirements.txt
```

3. Create a .env file

```bash
touch .env
```

4. Add your API keys to the .env file

```bash
GOLD_API_KEY=YOUR_GOLD_API_KEY
SILVER_API_KEY=YOUR_SILVER_API_KEY
```
5. Edit the config.yaml

```bash
SIVR_threshold: 10
GLDM_threshold: 10
USD_to_INR_threshold: 10
USD_to_INR_alert: False
SIVR_alert: False
GLDM_alert: False
```

6. Run the script

```bash
python main.py
```

# Usage

1. Open the terminal.

2. Navigate to the cloned repository.

3. Run the script using the command `python main.py`

