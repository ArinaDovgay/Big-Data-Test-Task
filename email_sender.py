import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from os.path import basename
from os import getenv

from dotenv import load_dotenv


def send_email(sender, receiver, subject, message, files):
    load_dotenv()
    password = getenv("pass")

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()

    try:
        server.login(sender, password)
        msg = MIMEMultipart()
        msg["Subject"] = subject

        for f in files or []:
            with open(f, "rb") as fil:
                part = MIMEApplication(fil.read(), Name=basename(f))
            # After the file is closed
            part["Content-Disposition"] = 'attachment; filename="%s"' % basename(f)
            msg.attach(part)

        msg.attach(MIMEText(message))
        server.sendmail(sender, receiver, msg.as_string())

        return "The message was sent successfully!"
    except Exception as _ex:
        return f"{_ex}\nCheck your login or password please!"


if __name__ == "__main__":
    sender = "arinadovgay@gmail.com"
    receiver = "lardi28102000@gmail.com"
    subject = "Bid Data Intreship - Test task - Dovhai Oryna"
    message = (
        "Hello, sending you results, csv file is attached. Here is the link to GitHub "
        "https://github.com/ArinaDovgay/Big-Data-Test-task-"
    )
    print(
        send_email(
            sender=sender,
            receiver=receiver,
            subject=subject,
            message=message,
            files=["/home/arina/spark_test/data/output/task_result.csv"],
        )
    )
