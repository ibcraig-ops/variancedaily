import imaplib, email, gzip, pandas as pd, mysql.connector, smtplib, io, os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

# --- CONFIGURATION FROM GITHUB SECRETS ---
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
    
    # Search for unread emails
    status, messages = mail.search(None, '(UNSEEN)')
    ipai_bytes, pes_bytes = None, None

    # Get newest messages first
    message_ids = messages[0].split()
    for num in reversed(message_ids):
        res, msg_data = mail.fetch(num, "(RFC822)")
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
                        continue
                    filename = part.get_filename()
                    if not filename:
                        continue
                    
                    if filename.endswith('.gz'):
                        ipai_bytes = gzip.decompress(part.get_payload(decode=True))
                    elif filename.endswith('.xls') or filename.endswith('.xlsx'):
                        pes_bytes = part.get_payload(decode=True)
        
        if ipai_bytes and pes_bytes:
            break
            
    return ipai_bytes, pes_bytes

def send_email_report(summary_stats, variances, tran_date):
    sender = os.getenv('EMAIL_USER')
    password = os.getenv('EMAIL_PASS')
    
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = sender # By default, sends to you. Change if needed.
    msg['Subject'] = f"📊 Recon Alert: R {summary_stats['var']:.2f} Variance for {tran_date}"

    body = f"""
    Daily Reconciliation Summary
    ----------------------------
    Transaction Date: {tran_date}
    IPAI Total: R {summary_stats['ipai']:.2f}
    PES Total:  R {summary_stats['pes']:.2f}
    Net Diff:   R {summary_stats['var']:.2f}
    
    Attached is the breakdown of individual meter discrepancies.
    View the full history at your website dashboard.
    """
    msg.attach(MIMEText(body, 'plain'))

    if variances:
        df_var = pd.DataFrame(variances)
        df_var.columns = ['Meter Number', 'IPAI Amount', 'PES Amount', 'Variance']
        csv_buffer = io.StringIO()
        df_var.to_csv(csv_buffer, index=False)
        
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(csv_buffer.getvalue())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="Variance_Report_{tran_date}.csv"')
        msg.attach(part)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.send_message(msg)
    except Exception as e:
        print(f"Failed to send email: {e}")

def run_recon():
    ipai_raw, pes_raw = get_attachments()
    if not ipai_raw or not pes_raw:
        print("Required files (.gz and .xls) not found in recent unread emails.")
        return

    # Process IPAI - Robust parsing to avoid the "saw 19 fields" error
    df_ipai = pd.read_csv(
        io.BytesIO(ipai_raw), 
        header=None, 
        names=range(30), # Pre-allocate columns
        on_bad_lines='skip',
        engine='python'
    )
    
    df_ipai = df_ipai[df_ipai[0] == 'IPAI']
    if df_ipai.empty:
        print("No 'IPAI' identifier rows found in the CSV.")
        return

    # Extract transaction date from the 9th column (index 8)
    raw_date = str(df_ipai.iloc[0, 8]).split('.')[0]
    tran_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}" if len(raw_date) >= 8 else "Unknown"
    
    # Meter is index 14, Amount (Cents) is index 13
    ipai_summary = df_ipai.groupby(14)[13].sum() / 100

    # Process PES
    df_pes = pd.read_excel(io.BytesIO(pes_raw))
    # Assuming Meter is Column 1 (index 0) and Amount is Column 3 (index 2)
    pes_summary = df_pes.groupby(df_pes.columns[0])[df_pes.columns[2]].sum()

    # Compare
    all_meters = set(ipai_summary.index) | set(pes_summary.index)
    variances = []
    t1, t2 = ipai_summary.sum(), pes_summary.sum()

    for m in all_meters:
        v1, v2 = ipai_summary.get(m, 0), pes_summary.get(m, 0)
        if abs(v1 - v2) > 0.01:
            variances.append({'m': str(m), 'v1': float(v1), 'v2': float(v2), 'diff': float(v1 - v2)})

    # Save to Database via the PHP Bridge
    payload = {
        "runTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tranDate": tran_date,
        "ipai": float(t1),
        "pes": float(t2),
        "var": float(t1-t2),
        "items": variances
    }
    
    # Update this URL to your actual InfinityFree website URL
    bridge_url = "https://ourdailyvariances.free.nf/history.php"
    
    import requests
    response = requests.post(bridge_url, json=payload)
    
    if response.status_code == 200:
        print("Successfully updated database via PHP Bridge.")
    else:
        print(f"Failed to update database. Status: {response.status_code}")
    
    
    # Update Run Summary
    cursor.execute("INSERT INTO recon_runs (run_time, tran_date, ipai_total, pes_total, variance) VALUES (%s, %s, %s, %s, %s)",
                   (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), tran_date, float(t1), float(t2), float(t1-t2)))
    run_id = cursor.lastrowid
    
    # Update Line Items
    for item in variances:
        cursor.execute("INSERT INTO recon_details (run_id, meter_number, ipai_amount, pes_amount, line_variance) VALUES (%s, %s, %s, %s, %s)",
                       (run_id, item['m'], item['v1'], item['v2'], item['diff']))
    
    # Update Automation Status for the Web App
    cursor.execute("""
        UPDATE automation_status 
        SET last_run_time = %s, status = 'Success', recipient = %s 
        WHERE id = 1
    """, (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), os.getenv('EMAIL_USER')))
    
    conn.commit()
    conn.close()

    # Email Notification
    stats = {'ipai': float(t1), 'pes': float(t2), 'var': float(t1-t2)}
    send_email_report(stats, variances, tran_date)
    print("Process Complete. Database updated and email sent.")

if __name__ == "__main__":
    run_recon()
