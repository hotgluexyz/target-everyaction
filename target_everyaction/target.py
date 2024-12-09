"""EveryAction target class."""

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_everyaction.sinks import ContactsSink


class TargetEveryAction(TargetHotglue):
    """Target for EveryAction."""

    name = "target-everyaction"
    SINK_TYPES = [ContactsSink]

    config_jsonschema = th.PropertiesList(
        th.Property("app_name", th.StringType, required=True),
        th.Property("api_key", th.StringType, required=True),
    ).to_dict()


if __name__ == "__main__":
    TargetEveryAction.cli()
