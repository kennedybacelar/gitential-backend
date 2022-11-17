from email.mime.application import MIMEApplication
from os.path import basename

from typing import Optional, Union
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Template
from structlog import get_logger
from pathlib import Path
from gitential2.datatypes.email_templates import EmailTemplate, RenderedEmail
from . import get_email_template
from .context import GitentialContext

logger = get_logger(__name__)

EMAIL_TEMPLATES_DIR = Path(__file__).parents[2] / "email_templates"


class ExtendedEmailService:

    def __init__(self,
                 g: GitentialContext,
                 recipients: list,
                 attachment_path: str,
                 template_name: str):
        self.recipients = recipients
        self.attachment_path = attachment_path
        self.template_name = template_name
        self.g = g

    def email_to_many(self):
        template = get_email_template(self.template_name)
        email_recipients = ", ".join(self.recipients)
        if not template:
            logger.error(f"Email template not found")
            return
        rendered_email = self._render_email_template(template, recipient=email_recipients)
        self.smtp_send(rendered_email, attachment = self.attachment_path)

    @staticmethod
    def _rendered_email_to_message(email: RenderedEmail, attachment: Union[str, None] = None) -> MIMEMultipart:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = email.subject
        msg["From"] = email.sender
        msg["To"] = email.recipient
        part1 = MIMEText(email.body_plaintext, "plain")
        part2 = MIMEText(email.body_html, "html")
        msg.attach(part1)
        msg.attach(part2)

        if attachment:
            with open(attachment, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=basename(attachment)
                )
            # After the file is closed
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(attachment)
            msg.attach(part)
        return msg

    def _render_email_template(self, template: EmailTemplate, recipient: Union[str, list, None] = None, **kwargs
                               ) -> RenderedEmail:
        def _render_template(s: str) -> str:
            t = Template(s)
            return t.render(settings=self.g.settings, **kwargs)

        return RenderedEmail(
            sender=self.g.settings.email.sender,
            recipient=recipient,
            subject=_render_template(template.subject),
            body_html=_render_template(template.body_html),
            body_plaintext=_render_template(template.body_plaintext),
        )

    def smtp_send(self, email: RenderedEmail, attachment: Optional[str] = None):
        email_settings = self.g.settings.email
        if email_settings.smtp_host and email_settings.smtp_port:
            try:
                server = smtplib.SMTP(email_settings.smtp_host, email_settings.smtp_port)
                server.ehlo()
                server.starttls()
                # stmplib docs recommend calling ehlo() before & after starttls()
                server.ehlo()
                if email_settings.smtp_username and email_settings.smtp_password:
                    server.login(email_settings.smtp_username, email_settings.smtp_password)

                server.sendmail(email.sender, email.recipient.split(","),
                                self._rendered_email_to_message(email, attachment=attachment).as_string())
                server.close()
            except Exception:  # pylint: disable=broad-except
                logger.exception("Failed to send email.")
            else:
                logger.info(f'Email sent to {email.recipient} with subject "{email.subject}"')
        else:
            logger.warning("SMTP not configured, cannot send emails.")
