from psycopg2 import sql
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from psycopg2.extensions import cursor

""" LOAD THE ENVIRONMENT VARIABLES """

async def link_exists_in_db(link: str, cur: cursor, test: bool = False) -> str | None:
	table = "main_jobs"

	if test:
		table = "test"

	query = sql.SQL(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE link=%s)")
	cur.execute(query, (link,))

	result = cur.fetchone()

	return result[0] if result else None


""" OTHER UTILS """


def send_email(log_file_path):
	fromaddr = "maddy@rolehounds.com"
	toaddr = "juancarlosrg1999@gmail.com"
	msg = MIMEMultipart()

	msg["From"] = fromaddr
	msg["To"] = toaddr
	msg["Subject"] = "Finished crawling & embedding! logs attached"

	with open(log_file_path, "rb") as f:
		part = MIMEBase("application", "octet-stream")
		part.set_payload(f.read())
		encoders.encode_base64(part)
		part.add_header("Content-Disposition", 'attachment; filename="log.txt"')
		msg.attach(part)

	server = smtplib.SMTP("smtp-mail.outlook.com", 587)
	server.starttls()
	server.login(fromaddr, "buttercuP339!")
	server.send_message(msg)
	server.quit()

