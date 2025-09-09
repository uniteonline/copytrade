import smtplib
from email.mime.text import MIMEText
from email.header import Header

def send_email(subject: str, content: str) -> None:
    """
    使用 QQ 企业邮箱 (alert@uniteonline.cn) 向固定地址 (admin@uniteonline.cn) 发送邮件。

    :param subject: 邮件标题
    :param content: 邮件正文
    """
    # QQ 企业邮箱 SMTP 配置信息（465 端口 + SSL）
    smtp_server = "smtp.exmail.qq.com"
    smtp_port = 465

    # 邮箱及登录凭证
    username = "xxx"
    password = "xxx"         # 若已启用授权码，此处需改成授权码
    sender_email = "xxx"
    recipient_email = "xxx@aaa.cn"  # 收件人可按需写死或改成动态传入

    # 构造邮件
    message = MIMEText(content, "plain", "utf-8")
    message["From"] = Header(sender_email, "utf-8")
    message["To"] = Header(recipient_email, "utf-8")
    message["Subject"] = Header(subject, "utf-8")

    try:
        # 连接 SMTP 服务器 (SSL)
        smtp_obj = smtplib.SMTP_SSL(smtp_server, smtp_port)
        smtp_obj.login(username, password)
        smtp_obj.sendmail(sender_email, [recipient_email], message.as_string())
        smtp_obj.quit()
        print(f"[INFO] 邮件已成功发送到: {recipient_email}")
    except Exception as e:
        print(f"[ERROR] 发送邮件失败: {e}")
