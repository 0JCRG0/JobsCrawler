import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import pretty_errors

def send_email(log_file_path):
	fromaddr = "maddy@rolehounds.com"
	toaddr = "juancarlosrg1999@gmail.com"
	msg = MIMEMultipart()

	time_sent = datetime.now()

	msg['From'] = fromaddr
	msg['To'] = toaddr
	msg['Subject'] = f"Logs of crawlers and their embeddings at {time_sent}"

	with open(log_file_path, 'rb') as f:
		part = MIMEBase('application', 'octet-stream')
		part.set_payload(f.read())
		encoders.encode_base64(part)
		part.add_header('Content-Disposition', 'attachment; filename="log.txt"')
		msg.attach(part)

	server = smtplib.SMTP('smtp-mail.outlook.com', 587)
	server.starttls()
	server.login(fromaddr, "buttercuP339!")
	server.send_message(msg)
	server.quit()

# At the end of your script
send_email('C:\\Users\\juanc\\log_crawl.txt')