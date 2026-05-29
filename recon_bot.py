import imaplib, email, gzip, pandas as pd, smtplib, io, os, json, requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# --- CONFIGURATION ---
TRACKER_URL = "http://yourdailyvariances.free.nf/tracker.php" 

def send_email_report(tran_date, ipai_total, pes_total, variance, variances):
    print("Sending Email Notification...")
    msg = MIMEMultipart()
    msg['From'] = os.getenv('EMAIL_USER')
    msg['To'] = os.getenv('EMAIL_USER')
    msg['Subject'] = f"Recon Alert: {tran_date} (Var: R{variance:.2f})"

    var_list_html = "".join([f"<li>Meter {v['m']} ({v.get('u', 'UNKNOWN')}): R{v['diff']:.2f}</li>" for v in variances[:10]])
    if len(variances) > 10:
        var_list_html += "<li>... and more in the dashboard</li>"

    body = f"""
    <div style="font-family: Arial, sans-serif; color: #333;">
        <h2 style="color: #3182ce;">Recon Summary for {tran_date}</h2>
        <p><b>IPAI (Bank) Total:</b> R{ipai_total:.2f}</p>
        <p><b>PES (Sales) Total:</b> R{pes_total:.2f}</p>
        <p><b>Final Variance:</b> <span style="color:{'red' if abs(variance) > 0.1 else 'green'}">R{variance:.2f}</span></p>
        <hr>
        <h3>Top Variances:</h3>
        <ul>{var_list_html if variances else "<li>No variances found.</li>"}</ul>
        <p><i>Check your <a href="http://yourdailyvariances.free.nf">Dashboard</a> for the full audit trail.</i></p>
    </div>
    """
    msg.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASS'))
        server.send_message(msg)
        server.quit()
        print("✓ Email sent successfully.")
    except Exception as e:
        print(f"Email failed to send: {e}")

def get_attachments():
    print("Connecting to Gmail...")
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com", timeout=30)
        mail.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASS'))
        mail.select("inbox")
        
        status, messages = mail.search(None, 'ALL')
        ipai_bytes, pes_bytes = None, None
        message_ids = messages[0].split()
        
        print(f"Scanning the last 50 emails for the new IPAI and PES files...")
        for num in reversed(message_ids[-50:]): 
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
                        
                        if fn.endswith('.gz') and 'ipai' in fn and 'markup_per_utility' in fn:
                            print(f"✓ Found New IPAI Utility File: {filename}")
                            ipai_bytes = gzip.decompress(part.get_payload(decode=True))
                        elif (fn.endswith('.xls') or fn.endswith('.xlsx')) and 'pes' in fn:
                            print(f"✓ Found PES: {filename}")
                            pes_bytes = part.get_payload(decode=True)
            
            if ipai_bytes and pes_bytes: break
                
        return ipai_bytes, pes_bytes
    except Exception as e:
        print(f"Gmail Access Error: {e}")
        return None, None

def run_recon():
    ipai_raw, pes_raw = get_attachments()
    if not ipai_raw or not pes_raw:
        print("Required files still missing. Check if 'markup_per_utility' and 'pes' are in the email files.")
        return

    # 1. PROCESS IPAI (Collect raw tracking arrays)
    df_ipai = pd.read_csv(io.BytesIO(ipai_raw), header=None, names=range(50), on_bad_lines='skip', engine='python')
    df_ipai = df_ipai[df_ipai[0] == 'IPAI']
    raw_date = str(df_ipai.iloc[0, 8]).split('.')[0]
    tran_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
    
    df_ipai[14] = df_ipai[14].astype(str).str.split('.').str[0].str.slice(0, 11)
    df_ipai[19] = df_ipai[19].astype(str).str.strip()
    
    utility_totals = (df_ipai.groupby(19)[13].sum() / 100).to_dict()
    meter_to_utility = df_ipai.set_index(14)[19].to_dict()
    
    ipai_tx = {}
    for idx, row in df_ipai.iterrows():
        m = str(row[14])
        val = float(row[13]) / 100
        if m not in ipai_tx: ipai_tx[m] = []
        ipai_tx[m].append(val)

    # 2. PROCESS PES
    df_pes = pd.read_excel(io.BytesIO(pes_raw))
    mtr_col = next((c for c in df_pes.columns if 'meter' in str(c).lower()), df_pes.columns[0])
    amt_col = next((c for c in df_pes.columns if 'amount' in str(c).lower() or 'total' in str(c).lower()), df_pes.columns[2])
    
    df_pes[amt_col] = pd.to_numeric(df_pes[amt_col], errors='coerce')
    df_pes[mtr_col] = df_pes[mtr_col].astype(str).str.split('.').str[0].str.slice(0, 11)
    df_pes = df_pes.dropna(subset=[amt_col])
    
    pes_tx = {}
    for idx, row in df_pes.iterrows():
        m = str(row[mtr_col])
        val = float(row[amt_col])
        if m not in pes_tx: pes_tx[m] = []
        pes_tx[m].append(val)

    # 3. INSTANCE-BASED PAIR COMPILATION
    all_meters = set(ipai_tx.keys()) | set(pes_tx.keys())
    variances = []
    t1 = sum([sum(l) for l in ipai_tx.values()])
    t2 = sum([sum(l) for l in pes_tx.values()])
    
    for m in all_meters:
        arr1 = ipai_tx.get(m, [])
        arr2 = pes_tx.get(m, [])
        sum1 = sum(arr1)
        sum2 = sum(arr2)
        
        if abs(sum1 - sum2) > 0.01:
            u_name = meter_to_utility.get(m, "UNKNOWN")
            max_len = max(len(arr1), len(arr2))
            for i in range(max_len):
                v1 = arr1[i] if i < len(arr1) else 0.0
                v2 = arr2[i] if i < len(arr2) else 0.0
                variances.append({'m': str(m), 'v1': v1, 'v2': v2, 'diff': v1 - v2, 'u': u_name})
            try:
                requests.post(TRACKER_URL, json={"meter_number": str(m), "amount": sum1 - sum2, "tranDate": tran_date, "isRobotSync": True}, timeout=3)
            except: pass

    # 4. SAVE HISTORIC SUMMARY
    new_run = {
        "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
        "tran_date": tran_date, 
        "ipai_total": t1, 
        "pes_total": t2, 
        "variance": t1 - t2, 
        "utility_totals": utility_totals,
        "items": variances, 
        "source": "ROBOT"
    }
    
    h_file = 'history_data.json'
    all_h = []
    if os.path.exists(h_file):
        try: with open(h_file, 'r') as f: all_h = json.load(f)
        except: all_h = []
    
    all_h.insert(0, new_run)
    with open(h_file, 'w') as f:
        json.dump(all_h[:91], f, indent=4) 
        
    send_email_report(tran_date, t1, t2, t1 - t2, variances)
    print(f"Recon Successful. Record saved for {tran_date}")

if __name__ == "__main__":
    run_recon()
