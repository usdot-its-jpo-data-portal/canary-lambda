from botocore.vendored import requests
import json

class SlackMessage():
    def __init__(self, success, files, recordcount, validationcount, errorcount, timestamp, function_name, aws_request_id, log_group_name, log_stream_name):
        self.validation = "PASSED" if success else "FAILED"
        self.files = files
        self.recordcount = recordcount
        self.validationcount = validationcount
        self.errorcount = errorcount
        self.timestamp = timestamp
        self.function_name = function_name
        self.aws_request_id = aws_request_id
        self.log_group_name = log_group_name
        self.log_stream_name = log_stream_name

    def send(self, logger, dest_url):
        slack_message = {
            "blocks":[
            	{
            		"type": "divider"
            	},
            	{
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Validation:* %s" % self.validation
            		}
            	},
            	{
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Files Analyzed (%d):* ```%s```" % (len(self.files), "\n".join(self.files))
            		}
            	},
                {
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Records Analyzed:* %d" % self.recordcount
            		}
            	},
                {
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Validations Performed:* %d" % self.validationcount
            		}
            	},
                {
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Validations Failed:* %d" % self.errorcount
            		}
            	},
                {
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Timestamp:* %s" % self.timestamp
            		}
            	},
                {
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Function Name:* %s" % self.function_name
            		}
            	},
                {
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Request ID:* %s" % self.aws_request_id
            		}
            	},
                {
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*CloudWatch Logs:* %s" % "https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logEventViewer:group="+self.log_group_name+";stream="+self.log_stream_name
            		}
            	},
                {
            		"type": "divider"
            	}
            ]
        }
        logger.info("Sending slack message to %s" % dest_url)
        logger.info(json.dumps(slack_message))
        with requests.Session() as session:
            r = session.post(dest_url, data=json.dumps(slack_message))
            logger.info("Slack API response: %s [%s]" % (r.status_code, r.reason))
