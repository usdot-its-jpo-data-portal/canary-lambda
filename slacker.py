from botocore.vendored import requests
import json

class SlackMessage():
    def __init__(self, success, prefixes, filecount, recordcount, validationcount, errorcount, errorstring, starttime, endtime, function_name, aws_request_id, log_group_name, log_stream_name):
        if success and validationcount > 0:
            self.validation = "PASSED"
        elif success and validationcount == 0:
            self.validation = "N/A"
        else:
            self.validation = "FAILED"
        self.filecount = filecount
        self.errorstring = "*Failed Validations:* ```%s```" % errorstring
        if len(self.errorstring) > 2950:
            self.errorstring = self.errorstring[:2950] + " ... [TRUNCATED LIST]"
        self.prefixes = prefixes
        self.recordcount = recordcount
        self.validationcount = validationcount
        self.errorcount = errorcount
        self.starttime = starttime
        self.endtime = endtime
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
            			"text": "*Validation Result:* %s" % self.validation
            		}
            	},
                {
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Prefixes Queried (%d):* ```%s```" % (len(self.prefixes), "\n".join(self.prefixes))
            		}
            	},
            	{
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Files Analyzed:* %d" % self.filecount
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
            			"text": self.errorstring
            		}
            	},
                {
            		"type": "section",
            		"text": {
            			"type": "mrkdwn",
            			"text": "*Started At:* %s\n*Completed At:* %s\n*Time Taken:* %.3f seconds" % (self.starttime.strftime("%H:%M:%S"), self.endtime.strftime("%H:%M:%S"), (self.endtime-self.starttime).total_seconds())
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
            logger.info("Slack API response: %s [%s] (%s)" % (r.status_code, r.reason, r.text))
