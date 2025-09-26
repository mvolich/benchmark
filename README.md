# Rubrics Positioning & Risk Dashboard

A Streamlit dashboard for analyzing Rubrics’ UCITS funds versus their benchmarks under multiple market scenarios.  
It combines quantitative positioning analysis with AI-driven insights to support portfolio management and board-level reporting.

---

## 📊 Features

### **1. Current Positioning**
- Floating KPI tiles: curve duration, spread sensitivity (DTS), carry, OGC, and net carry.
- Fund vs Benchmark comparisons by:
  - Currency (duration totals & differences).
  - Curve maturity buckets (bubble charts).
- Heatmaps of duration sensitivity.
- Risk–carry scatter plots.
- Bullet list of overweights/underweights.

### **2. Scenario Analysis (12-month horizon)**
- Fund and Benchmark expected 12-month returns, plus relative difference.
- Attribution by driver: Carry, Credit, Rates, and OGC.
- Scenario ranking (which scenarios help or hurt the Fund vs Benchmark).
- Driver mix donuts and relative rates impact by currency.
- Drill-down tables by currency and maturity.
- Full scenario table with grouped columns and Δ (Fund − Benchmark) for each driver.

### **3. GenAI Insights**
- One-click AI-generated summary and 3 ranked recommendations.
- Grounded only in computed Fund/Benchmark/scenario data — no hallucinations.
- Recommendations include tactical trades, credit shifts, and OGC impacts, each with estimated bps effect.
- Powered by OpenAI (`gpt-4o-mini`), with API key stored securely in Streamlit secrets.

---

## ⚙️ Installation

1. **Clone the repo**
   ```bash
   git clone https://github.com/<your-org>/rubrics-dashboard.git
   cd rubrics-dashboard
Create a virtual environment

bash
Copy code
python -m venv venv
source venv/bin/activate        # On macOS/Linux
venv\Scripts\activate           # On Windows
Install dependencies

bash
Copy code
pip install -r requirements.txt
🔑 Configuration
Data Input

Upload Dashboard_Input.xlsx in the sidebar when running the app.

Required sheets:

Combined 2 (positions, KRDs, DTS, carry).

Scenarios (rates shocks, credit shocks).

OGC (ongoing charges).

Optional raw tabs (GCF Raw, GFI Raw, EYF Raw) supply as-of dates.

Secrets

Store your OpenAI key in .streamlit/secrets.toml:

toml
Copy code
[openai]
OPENAI_API_KEY = "sk-..."
▶️ Running
Start the app with:

bash
Copy code
streamlit run dashboard.py
It will open in your browser at http://localhost:8501.

🖥️ Usage
Sidebar: Upload Excel, select Fund (GCF, GFI, EYF) and Scenario.

Tabs:

Current Positioning: risk/carry overview and Fund vs Benchmark exposures.

Scenario Analysis: attribution, scenario ranking, driver mix, drill-down, and scenario table.

GenAI Insights: AI-generated summary and recommendations.

📦 Requirements
Python 3.10+

Streamlit ≥ 1.32

Plotly ≥ 5.20

Pandas ≥ 2.1

NumPy ≥ 1.26

OpenAI ≥ 1.12

(See requirements.txt)

🔒 Security & Compliance
Do not commit .streamlit/secrets.toml (contains API keys).

Dashboard_Input.xlsx contains confidential positions/risk — treat as internal only.

AI outputs are deterministic summaries of in-app numbers; no external market data is queried.

All downloads (CSVs, tables) are for analyst/internal use only.

📝 License
Proprietary. © Rubrics Asset Management, 2025.

pgsql
Copy code

---

Would you like me to also prepare a **shorter “board pack” version** of this README (1-page, non-technical, with just purpose