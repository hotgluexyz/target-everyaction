"""EveryAction target class."""

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel
from target_everyaction.sinks import ContactsSink


class TargetEveryAction(TargetHotglue):
    """Target for EveryAction."""

    name = "target-everyaction"
    SINK_TYPES = [ContactsSink]
    alerting_level = AlertingLevel.ERROR

    config_jsonschema = th.PropertiesList(
        th.Property("app_name", th.StringType, required=True),
        th.Property("api_key", th.StringType, required=True),
    ).to_dict()


if __name__ == "__main__":
    TargetEveryAction.cli()
