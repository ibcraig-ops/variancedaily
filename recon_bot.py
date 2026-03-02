import imaplib, email, gzip, pandas as pd, smtplib, io, os, json, requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

# --- CONFIGURATION ---
# Replace with your actual dashboard URL
TRACKER_URL = "http://yourdailyvariances.free.nf/tracker.php" 

def get_attachments():
    print("Connecting to Gmail...")
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com", timeout=30)
        mail.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASS'))
        mail.select("inbox")
        
        # Search last 15 emails for reliability
        status, messages = mail.search(None, 'ALL')
        ipai_bytes, pes_bytes = None, None
        message_ids = messages[0].split()
        
        for num in reversed(message_ids[-15:]): 
            res, msg_data = mail.fetch(num, "(RFC822)")
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    for part in msg.walk():
                        if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None: 
                            continue
                        filename = part.get_filename()
                        if not filename: continue
                        fn = filename.lower()
                        
                        if fn.endswith('.gz'):
                            print(f"Found IPAI File: {filename}")
                            ipai_bytes = gzip.decompress(part.get_payload(decode=True))
                        elif fn.endswith('.xls') or fn.endswith('.xlsx'):
                            print(f"Found PES File: {filename}")
                            pes_bytes = part.get_payload(decode=True)
            if ipai_bytes and pes_bytes: break
                
        return ipai_bytes, pes_bytes
    except Exception as e:
        print(f"Gmail Connection Error: {e}")
        return None, None

def run_recon():
    ipai_raw, pes_raw = get_attachments()
    if not ipai_raw or not pes_raw:
        print("Required files not found in recent emails. Stopping.")
        return

    print("Files located. Starting data parsing...")

    # 1. PROCESS IPAI (CSV)
    # IPAI typically has a standard 35-column format
    df_ipai = pd.read_csv(io.BytesIO(ipai_raw), header=None, names=range(35), on_bad_lines='skip', engine='python')
    df_ipai = df_ipai[df_ipai[0] == 'IPAI']
    
    # Extract Tran Date (Column 9 / Index 8)
    raw_date = str(df_ipai.iloc[0, 8]).split('.')[0]
    tran_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
    
    # Group by Meter (Index 14) and sum Amount (Index 13)
    # Meter numbers are forced to strings and decimals stripped
    df_ipai[14] = df_ipai[14].astype(str).str.split('.').str[0]
    ipai_summary = df_ipai.groupby(14)[13].sum() / 100

    # 2. PROCESS PES (Excel) - STRICT INDEXING FIX
    # We use header=0 but then iloc to bypass naming errors like 'Vending transaction'
    df_pes = pd.read_excel(io.BytesIO(pes_raw), header=0)
    
    # FORCE DATA TYPES: Column 1 (Index 0) is Meter, Column 3 (Index 2) is Amount
    # We 'coerce' errors to handle any rogue text in numeric columns
    df_pes.iloc[:, 2] = pd.to_numeric(df_pes.iloc[:, 2], errors='coerce')
    df_pes.iloc[:, 0] = df_pes.iloc[:, 0].astype(str).str.split('.').str[0]

    # CLEANUP: Remove any rows that are empty in the amount column
    df_pes = df_pes.dropna(subset=[df_pes.columns[2]])
    
    # Group by the first column and sum the third
    pes_summary = df_pes.groupby(df_pes.columns[0])[df_pes.columns[2]].sum()

    # 3. COMPARISON & LEDGER SYNC
    all_meters = set(ipai_summary.index) | set(pes_summary.index)
    variances = []
    t1, t2 = float(ipai_summary.sum()), float(pes_summary.sum())

    print(f"Analyzing {len(all_meters)} unique meters...")

    for m in all_meters:
        v1, v2 = float(ipai_summary.get(m, 0)), float(pes_summary.get(m, 0))
        if abs(v1 - v2) > 0.01:
            variances.append({'m': str(m), 'v1': v1, 'v2': v2, 'diff': v1 - v2})
            
            # SYNC TO DASHBOARD: Push individual meter variances to the SQL Ledger
            try:
                requests.post(TRACKER_URL, json={
                    "meter_number": str(m),
                    "amount": v1 - v2,
                    "tran_date": tran_date,
                    "isRobotSync": True
                }, timeout=3)
            except:
                pass

    # 4. SAVE TO JSON HISTORY (GITHUB VIEW)
    new_run = {
        "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tran_date": tran_date,
        "ipai_total": t1,
        "pes_total": t2,
        "variance": t1 - t2,
        "items": variances,
        "source": "ROBOT"
    }
    
    history_file = 'history_data.json'
    all_history = []

    if os.path.exists(history_file):
        try:
            with open(history_file, 'r') as f:
                content = json.load(f)
                all_history = content if isinstance(content, list) else [content]
        except:
            all_history = []

    all_history.insert(0, new_run)
    all_history = all_history[:31] # Retain 31 days of history

    with open(history_file, 'w') as f:
        json.dump(all_history, f, indent=4)

    print(f"Recon Successful for {tran_date}. Total Variance: R {t1-t2:.2f}")

if __name__ == "__main__":
    run_recon()
