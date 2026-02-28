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
    ipai_bytes, pes_bytes, tran_date = None, None, "Unknown"

    for num in messages[0].split()[::-1]:
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

def send_email_report(summary_stats, variances, tran_date):
    sender = os.getenv('EMAIL_USER')
    password = os.getenv('EMAIL_PASS')
    
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = sender # Sending it to yourself
    msg['Subject'] = f"📊 Recon Alert: R {summary_stats['var']:.2f} Variance for {tran_date}"

    # Email Body
    body = f"""
    Daily Reconciliation Summary
    ----------------------------
    Transaction Date: {tran_date}
    IPAI Total: R {summary_stats['ipai']:.2f}
    PES Total:  R {summary_stats['pes']:.2f}
    Net Diff:   R {summary_stats['var']:.2f}
    
    Attached is the breakdown of individual meter discrepancies.
    View the full history at your InfinityFree URL.
    """
    msg.attach(MIMEText(body, 'plain'))

    # Create CSV Attachment
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

    # Send
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, password)
        server.send_message(msg)

def run_recon():
    ipai_raw, pes_raw = get_attachments()
    if not ipai_raw or not pes_raw: return print("Files not found.")

    # Process IPAI
    df_ipai = pd.read_csv(
    io.BytesIO(ipai_raw), 
    header=None, 
    names=range(25),     # Tells Python to expect up to 25 columns
    fill_value=None, 
    index_col=False, 
    on_bad_lines='skip'  # Skips lines that don't match rather than crashing
    df_ipai = df_ipai[df_ipai[0] == 'IPAI']
    tran_date = str(df_ipai.iloc[0, 8]).split('.')[0]
    ipai_summary = df_ipai.groupby(14)[13].sum() / 100

    # Process PES
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

    # Save to Database
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

    # Email Notification
    stats = {'ipai': t1, 'pes': t2, 'var': t1-t2}
    send_email_report(stats, variances, tran_date)
    print("Process Complete.")

if __name__ == "__main__":
    run_recon()

# Add this inside the run_recon() function after the email is sent:
cursor = conn.cursor()
cursor.execute("""
    UPDATE automation_status 
    SET last_run_time = %s, status = 'Success', recipient = %s 
    WHERE id = 1
""", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), os.getenv('EMAIL_USER')))
conn.commit()
