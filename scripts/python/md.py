from string import Template

SLACK_CHANNELS = {
    "engineering": {"name": "#kairos-dev", "link": "CJGQD1XEZ"},
}

_TEMPLATE = Template("""
# $name
$description

Owned by **$owner**. Contact **[$slack_name](https://apexclearing.slack.com/archives/$slack_link)** for support.
""")

def build(config: dict[str, str]):
    return _TEMPLATE.safe_substitute(config)

doc_md = build({
    "name": "Command Retry",
    "description": "Trigger the command retry job 4 times a day. This will run `cmd/ledger/command_retry` which will retry all the failed commands by executing the retry-able error SQL query.",
    "owner": "engineering",
    "slack_name": SLACK_CHANNELS["engineering"]["name"],
    "slack_link": SLACK_CHANNELS["engineering"]["link"],
})

print(doc_md)
