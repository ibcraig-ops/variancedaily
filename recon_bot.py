import imaplib, email, gzip, pandas as pd, mysql.connector, smtplib, io, os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# --- LOAD SECRETS FROM ENVIRONMENT ---
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'database': os.getenv('DB_NAME')
}

def get_attachments():
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASS'))
    mail.select("inbox")
    
    # Search for unread emails with attachments
    status, messages = mail.search(None, '(UNSEEN)')
    ipai_bytes, pes_bytes, tran_date = None, None, "Unknown"

    for num in messages[0].split()[::-1]: # Look at newest first
        res, msg_data = mail.fetch(num, "(RFC822)")
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None: continue
                    filename = part.get_filename()
                    if not filename: continue
                    
                    if filename.endswith('.gz'):
                        ipai_bytes = gzip.decompress(part.get_payload(decode=True))
                    if filename.endswith('.xls') or filename.endswith('.xlsx'):
                        pes_bytes = part.get_payload(decode=True)
        if ipai_bytes and pes_bytes: break
    return ipai_bytes, pes_bytes

def run_recon():
    ipai_raw, pes_raw = get_attachments()
    if not ipai_raw or not pes_raw: return print("Files not found.")

    # Process IPAI (CSV)
    df_ipai = pd.read_csv(io.BytesIO(ipai_raw), header=None)
    df_ipai = df_ipai[df_ipai[0] == 'IPAI']
    tran_date = str(df_ipai.iloc[0, 8]).split('.')[0] # Column 9
    ipai_summary = df_ipai.groupby(14)[13].sum() / 100 # Meter (15), Cents (14)

    # Process PES (Excel)
    df_pes = pd.read_excel(io.BytesIO(pes_raw))
    pes_summary = df_pes.groupby(df_pes.columns[0])[df_pes.columns[2]].sum()

    # Compare
    all_meters = set(ipai_summary.index) | set(pes_summary.index)
    variances = []
    t1, t2 = ipai_summary.sum(), pes_summary.sum()

    for m in all_meters:
        v1, v2 = ipai_summary.get(m, 0), pes_summary.get(m, 0)
        if abs(v1 - v2) > 0.01:
            variances.append({'m': str(m), 'v1': v1, 'v2': v2, 'diff': v1 - v2})

    # Save to InfinityFree DB
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO recon_runs (run_time, tran_date, ipai_total, pes_total, variance) VALUES (%s, %s, %s, %s, %s)",
                   (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), tran_date, t1, t2, t1-t2))
    run_id = cursor.lastrowid
    for item in variances:
        cursor.execute("INSERT INTO recon_details (run_id, meter_number, ipai_amount, pes_amount, line_variance) VALUES (%s, %s, %s, %s, %s)",
                       (run_id, item['m'], item['v1'], item['v2'], item['diff']))
    conn.commit()
    conn.close()
    print("Database updated.")

if __name__ == "__main__":
    run_recon()
