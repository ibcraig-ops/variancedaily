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
        # Scan last 15 emails to ensure we don't miss the attachments
        mail = imaplib.IMAP4_SSL("imap.gmail.com", timeout=30)
        mail.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASS'))
        mail.select("inbox")
        
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
        print("Required files not found. Stopping.")
        return

    print("Files located. Starting Smart Data Mapping...")

    # 1. PROCESS IPAI (Standard CSV)
    df_ipai = pd.read_csv(io.BytesIO(ipai_raw), header=None, names=range(35), on_bad_lines='skip', engine='python')
    df_ipai = df_ipai[df_ipai[0] == 'IPAI']
    
    # Extract Tran Date (Column 9 / Index 8)
    raw_date = str(df_ipai.iloc[0, 8]).split('.')[0]
    tran_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
    
    # String Lock for IPAI Meters
    df_ipai[14] = df_ipai[14].astype(str).str.split('.').str[0]
    ipai_summary = df_ipai.groupby(14)[13].sum() / 100

    # 2. PROCESS PES (Smart Keyword Mapping)
    # This prevents the 'Vending transaction' error by finding the correct columns
    df_pes = pd.read_excel(io.BytesIO(pes_raw))
    
    # DYNAMIC SEARCH: Find 'Meter' and 'Amount' columns by name
    mtr_col = next((c for c in df_pes.columns if 'meter' in str(c).lower()), df_pes.columns[0])
    amt_col = next((c for c in df_pes.columns if 'amount' in str(c).lower() or 'total' in str(c).lower()), df_pes.columns[2])

    print(f"Mapping detected: Meter -> [{mtr_col}], Amount -> [{amt_col}]")

    # CLEANUP: Turn descriptions into NaN and strip decimals from meters
    df_pes[amt_col] = pd.to_numeric(df_pes[amt_col], errors='coerce')
    df_pes[mtr_col] = df_pes[mtr_col].astype(str).str.split('.').str[0]

    # REMOVE NULLS: Drop rows that aren't financial transactions
    df_pes = df_pes.dropna(subset=[amt_col])
    pes_summary = df_pes.groupby(mtr_col)[amt_col].sum()

    # 3. COMPARISON & LEDGER SYNC
    all_meters = set(ipai_summary.index) | set(pes_summary.index)
    variances = []
    t1, t2 = float(ipai_summary.sum()), float(pes_summary.sum())

    print(f"Comparing {len(all_meters)} unique meters...")

    for m in all_meters:
        v1, v2 = float(ipai_summary.get(m, 0)), float(pes_summary.get(m, 0))
        if abs(v1 - v2) > 0.01:
            variances.append({'m': str(m), 'v1': v1, 'v2': v2, 'diff': v1 - v2})
            
            # PUSH TO SQL: Update your Dashboard Outstanding Ledger
            try:
                requests.post(TRACKER_URL, json={
                    "meter_number": str(m),
                    "amount": v1 - v2,
                    "tranDate": tran_date,
                    "isRobotSync": True
                }, timeout=3)
            except:
                pass

    # 4. SAVE TO GITHUB JSON
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
                all_history = content
