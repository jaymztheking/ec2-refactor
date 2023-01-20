def send_slack_message(config: dict, message: str, project_root: str, slack_webhook_url_dict: dict) -> bool:
    pass

def send_template_slack_message(config: dict, type: str, project_root: str, slack_webhook_url_dict: dict) -> bool:
    pass


class SlackSender:
    def __init__(self, config: dict, project_root: str, slack_webhook_url_dict: dict):
        self.config = config
        self.project_root = project_root
        self.slack_webhook_url_dict = slack_webhook_url_dict

    def send_message(message: str) -> bool:
        pass