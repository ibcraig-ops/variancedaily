import imaplib, email, gzip, pandas as pd, smtplib, io, os, requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

def get_attachments():
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASS'))
    mail.select("inbox")
    status, messages = mail.search(None, 'ALL')
    ipai_bytes, pes_bytes = None, None
    message_ids = messages[0].split()
    for num in reversed(message_ids[-10:]): 
        res, msg_data = mail.fetch(num, "(RFC822)")
        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                for part in msg.walk():
                    if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None: continue
                    filename = part.get_filename()
                    if not filename: continue
                    fn = filename.lower()
                    if fn.endswith('.gz'):
                        ipai_bytes = gzip.decompress(part.get_payload(decode=True))
                    elif fn.endswith('.xls') or fn.endswith('.xlsx'):
                        pes_bytes = part.get_payload(decode=True)
        if ipai_bytes and pes_bytes: break
    return ipai_bytes, pes_bytes

def send_email_report(summary_stats, variances, tran_date):
    sender = os.getenv('EMAIL_USER')
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = sender
    msg['Subject'] = f"📊 Recon Alert: R {summary_stats['var']:.2f} Variance for {tran_date}"
    body = f"Daily Recon Summary\nDate: {tran_date}\nIPAI: R {summary_stats['ipai']:.2f}\nPES: R {summary_stats['pes']:.2f}\nDiff: R {summary_stats['var']:.2f}"
    msg.attach(MIMEText(body, 'plain'))
    if variances:
        df_var = pd.DataFrame(variances)
        df_var.columns = ['Meter', 'IPAI', 'PES', 'Variance']
        csv_buffer = io.StringIO()
        df_var.to_csv(csv_buffer, index=False)
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(csv_buffer.getvalue())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="Variance_{tran_date}.csv"')
        msg.attach(part)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, os.getenv('EMAIL_PASS'))
        server.send_message(msg)

def run_recon():
    ipai_raw, pes_raw = get_attachments()
    if not ipai_raw or not pes_raw:
        print("Required files not found.")
        return

    # Process IPAI
    df_ipai = pd.read_csv(io.BytesIO(ipai_raw), header=None, names=range(30), on_bad_lines='skip', engine='python')
    df_ipai = df_ipai[df_ipai[0] == 'IPAI']
    raw_date = str(df_ipai.iloc[0, 8]).split('.')[0]
    tran_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
    ipai_summary = df_ipai.groupby(14)[13].sum() / 100

    # Process PES
    df_pes = pd.read_excel(io.BytesIO(pes_raw))
    pes_summary = df_pes.groupby(df_pes.columns[0])[df_pes.columns[2]].sum()

    # Compare
    all_meters = set(ipai_summary.index) | set(pes_summary.index)
    variances = []
    t1, t2 = float(ipai_summary.sum()), float(pes_summary.sum())

    for m in all_meters:
        v1, v2 = float(ipai_summary.get(m, 0)), float(pes_summary.get(m, 0))
        if abs(v1 - v2) > 0.01:
            variances.append({'m': str(m), 'v1': v1, 'v2': v2, 'diff': v1 - v2})

    # --- SAVE VIA BRIDGE ---
    payload = {
        "runTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tranDate": tran_date,
        "ipai": t1,
        "pes": t2,
        "var": t1-t2,
        "items": variances
    }
    
    # UPDATE THIS URL
    bridge_url = "https://ourdailyvariances.free.nf/history.php"
    
    try:
        response = requests.post(bridge_url, json=payload)
        print(f"Bridge Response: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Bridge failed: {e}")

    send_email_report({'ipai': t1, 'pes': t2, 'var': t1-t2}, variances, tran_date)

if __name__ == "__main__":
    run_recon()

