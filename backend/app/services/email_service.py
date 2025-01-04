from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import os
from datetime import datetime
from typing import List, Optional

class EmailService:
    def __init__(self):
        # SMTP Configuration
        self.smtp_server = os.getenv('SMTP_SERVER')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_username = os.getenv('SMTP_USERNAME')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.from_email = os.getenv('SMTP_FROM_EMAIL')
        
        # Admin email mapping
        self.admin_emails = {
            'NETWORK': os.getenv('NETWORK_ADMIN_EMAIL'),
            'SECURITY': os.getenv('SECURITY_ADMIN_EMAIL'),
            'AVAILABILITY': os.getenv('AVAILABILITY_ADMIN_EMAIL'),
            'DATA': os.getenv('DATA_ADMIN_EMAIL'),
            'RESOURCE': os.getenv('RESOURCE_ADMIN_EMAIL'),
            'PERFORMANCE': os.getenv('PERFORMANCE_ADMIN_EMAIL')
        }

    async def send_email(self, subject: str, body: str, to_emails: List[str], anomaly_type: Optional[str] = None) -> bool:
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = self.from_email
            msg['To'] = ', '.join(to_emails)
            msg['Subject'] = subject

            # Attach both plain text and HTML versions
            msg.attach(MIMEText(body, 'plain'))
            msg.attach(MIMEText(self._get_html_content(body), 'html'))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)
            return True
        except Exception as e:
            print(f"Email sending failed: {str(e)}")
            return False

    def get_admin_emails(self, anomaly_type: str) -> List[str]:
        admin_email = self.admin_emails.get(anomaly_type)
        return [admin_email] if admin_email else []

    def _get_severity_color(self, score: float) -> str:
        if score >= 0.8:
            return "#DC2626"  # Red
        elif score >= 0.7:
            return "#F59E0B"  # Amber
        else:
            return "#2563EB"  # Blue

    def _get_html_content(self, plain_text: str) -> str:
        # Extract information from plain text
        lines = plain_text.strip().split('\n')
        type_line = next((line for line in lines if line.startswith('Type:')), '')
        score_line = next((line for line in lines if line.startswith('Severity Score:')), '')
        time_line = next((line for line in lines if line.startswith('Time Detected:')), '')
        
        # Extract the log details (everything after "Details:")
        details_start = plain_text.find('Details:')
        details = plain_text[details_start:].split('\n', 1)[1].strip() if details_start != -1 else ''
        
        # Extract score value for color
        try:
            score = float(score_line.split(':')[1].strip())
        except:
            score = 0.0
        
        color = self._get_severity_color(score)
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background-color: #F3F4F6; padding: 20px; border-radius: 8px;">
                <div style="background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                    <h1 style="color: {color}; margin-top: 0; font-size: 24px; border-bottom: 2px solid {color}; padding-bottom: 10px;">
                        ⚠️ Anomaly Detection Alert
                    </h1>
                    
                    <div style="margin: 20px 0;">
                        <p style="margin: 8px 0;"><strong>Type:</strong> <span style="color: {color}">{type_line.split(':')[1].strip()}</span></p>
                        <p style="margin: 8px 0;"><strong>Severity Score:</strong> <span style="color: {color}">{score_line.split(':')[1].strip()}</span></p>
                        <p style="margin: 8px 0;"><strong>Time Detected:</strong> {time_line.split(':')[1].strip()}</p>
                    </div>

                    <div style="background-color: #F3F4F6; padding: 15px; border-radius: 4px; margin-top: 20px;">
                        <h2 style="margin-top: 0; font-size: 18px; color: #374151;">Details</h2>
                        <pre style="white-space: pre-wrap; font-family: monospace; margin: 0;">{details}</pre>
                    </div>

                    <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #E5E7EB; font-size: 12px; color: #6B7280;">
                        <p style="margin: 0; text-align: center;">This is an automated message from the Anomaly Detection System.</p>
                    </div>
                </div>
            </div>
        </body>
        </html>
        """

    def format_anomaly_notification(
        self,
        anomaly_text: str,
        anomaly_type: str,
        score: float,
        timestamp: datetime
    ) -> tuple[str, str]:
        subject = f"New Alert: {anomaly_type} Anomaly Detected"
        
        body = f"""
Anomaly Detection Alert
----------------------
Type: {anomaly_type}
Severity Score: {score:.2f}
Time Detected: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}

Details:
{anomaly_text}

"""
        return subject, body 