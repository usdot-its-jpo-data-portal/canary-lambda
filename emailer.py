import boto3

class Emailer:
    def __init__(self):
        self.ses_client = boto3.client('ses')

    ### TODO
    def send_report(message):
        # response = ses_client.send_email(
        #     Source=SOURCE_EMAIL,
        #     Destination={
        #         'ToAddresses': [
        #             DESTINATION_EMAIL,
        #         ],
        #         'CcAddresses': [
        #         ],
        #         'BccAddresses': [
        #         ],
        #     },
        #     Message={
        #         'Subject': {
        #             'Charset': 'UTF-8',
        #             'Data': '[DATAHUB AUTOMATED ALERT] DataHub Canary Validation Lambda Results'
        #         },
        #         'Body': {
        #             'Text': {
        #                 'Charset': 'UTF-8',
        #                 'Data': message
        #             }
        #         }
        #     },
        #     ReplyToAddresses=[
        #     ],
        #     ReturnPath='',
        #     SourceArn='',
        #     ReturnPathArn='',
        # )
        return

    ### TODO
    def send_report_with_attachment(message):
        # response = client.send_raw_email(
        #     Destination={
        #         'ToAddresses': [
        #             DESTINATION_EMAIL,
        #         ],
        #         'CcAddresses': [
        #         ],
        #         'BccAddresses': [
        #         ],
        #     },
        #     FromArn='',
        #     RawMessage={
        #         'Data': 'From: sender@example.com\nTo: recipient@example.com\nSubject: Test email (contains an attachment)\nMIME-Version: 1.0\nContent-type: Multipart/Mixed; boundary="NextPart"\n\n--NextPart\nContent-Type: text/plain\n\nThis is the message body.\n\n--NextPart\nContent-Type: text/plain;\nContent-Disposition: attachment; filename="attachment.txt"\n\nThis is the text in the attachment.\n\n--NextPart--',
        #     },
        #     ReturnPathArn='',
        #     Source='',
        #     SourceArn='',
        # )
        return
