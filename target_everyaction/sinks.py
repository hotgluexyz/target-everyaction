"""EveryAction target sink class, which handles writing streams."""

from target_everyaction.client import EveryActionSink
import singer

LOGGER = singer.get_logger()


class ContactsSink(EveryActionSink):
    """EveryAction target sink class."""

    name = "Contacts"
    endpoint = "people/findOrCreate"

    def map_fields(self, record: dict) -> dict:
        payload = {
            "firstName": record.get("first_name"),
            "lastName": record.get("last_name"),
            "middleName": record.get("middle_name"),
            "suffix": record.get("suffix"),
            "title": record.get("title"),
            "salutation": record.get("salutation"),
            "nickname": record.get("nickname"),
            "dateOfBirth": record.get("date_of_birth"),
            "sex": record.get("sex"),
            "employer": record.get("employer"),
            "occupation": record.get("occupation"),
            "jobTitle": record.get("job_title"),
            "party": record.get("party"),
        }

        if "email" in record:
            payload["emails"] = [
                {
                    "email": record["email"],
                    "type": "P",
                    "isPreferred": True,
                }
            ]

        if "addresses" in record and isinstance(record["addresses"], list):
            payload["addresses"] = []
            for address in record["addresses"]:
                address_dict = {
                    "addressLine1": address.get("line1"),
                    "addressLine2": address.get("line2"),
                    "addressLine3": address.get("line3"),
                    "city": address.get("city"),
                    "stateOrProvince": address.get("state"),
                    "zipOrPostalCode": address.get("postal_code"),
                    "countryCode": address.get("country"),
                    "type": "Voting",
                    "isPreferred": True,
                }
                payload["addresses"].append(address_dict)

        if "phone_numbers" in record and isinstance(record["phone_numbers"], list):
            payload["phones"] = []
            for phone in record["phone_numbers"]:
                phone_dict = {
                    "phoneNumber": phone.get("number"),
                    "phoneType": phone.get("type", "H"),
                    "isPreferred": True,
                }
                payload["phones"].append(phone_dict)

        return payload

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return self.map_fields(record)

    def upsert_record(self, record: dict, context: dict):
        method = "POST"
        state_dict = dict()

        response = self.request_api(method, request_data=record, endpoint=self.endpoint)
        if response.status_code in [200, 201]:
            state_dict["success"] = True
            id = response.json().get("vanId")
        else:
            state_dict["success"] = False
            id = None

        LOGGER.info(f"Upsert result: id={id}, success={state_dict['success']}")
        return id, response.ok, state_dict
