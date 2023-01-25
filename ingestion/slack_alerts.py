class SlackSender:
    def __init__(self, config: dict, project_root: str, slack_webhook_url_dict: dict):
        self.config = config
        self.project_root = project_root
        self.slack_webhook_url_dict = slack_webhook_url_dict

    def send_message(message: str) -> bool:
        pass